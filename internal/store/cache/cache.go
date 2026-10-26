// Package cache wraps a store.Store and briefly reuses the results of slow
// aggregations, so live refreshes cannot pile up multi-second queries.
package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
)

const (
	minSlow    = 100 * time.Millisecond // faster queries are never cached
	maxTTL     = 30 * time.Second       // a result is kept 5x its duration, at most this
	maxEntries = 200
)

type entry struct {
	done    chan struct{} // closed once val and err are set
	val     any
	err     error
	expires time.Time
}

// Store caches Histogram, TaskTypes and TypeStats. Every other method, the
// task list included, goes straight to the wrapped store.
type Store struct {
	store.Store
	mu      sync.Mutex
	entries map[string]*entry
}

func New(s store.Store) *Store {
	return &Store{Store: s, entries: map[string]*entry{}}
}

func (c *Store) Histogram(ctx context.Context, q store.HistogramQuery) ([]store.HistogramRow, error) {
	f := q.Filter
	k := key("hist", f.State, f.TaskName, f.Name, f.ArgsSearch, f.From, f.To, f.Stale, q.Bucket, q.Offset)
	return cached(ctx, c, k, func(ctx context.Context) ([]store.HistogramRow, error) { return c.Store.Histogram(ctx, q) })
}

func (c *Store) TaskTypes(ctx context.Context, from, to time.Time) ([]store.TypeSummary, error) {
	return cached(ctx, c, key("types", from, to), func(ctx context.Context) ([]store.TypeSummary, error) {
		return c.Store.TaskTypes(ctx, from, to)
	})
}

func (c *Store) TypeStats(ctx context.Context, name string, from, to time.Time) (*store.TypeStats, error) {
	return cached(ctx, c, key("stats", name, from, to), func(ctx context.Context) (*store.TypeStats, error) {
		return c.Store.TypeStats(ctx, name, from, to)
	})
}

// key builds a cache key. Times are rounded to maxTTL: windows ending "now"
// move on every request and would otherwise never match.
func key(parts ...any) string {
	round := func(t time.Time) int64 { return t.Truncate(maxTTL).Unix() }
	for i, p := range parts {
		switch v := p.(type) {
		case time.Time:
			parts[i] = round(v)
		case *store.StaleFilter:
			if v != nil {
				parts[i] = [2]int64{round(v.RunningStartedBefore), round(v.QueuedBefore)}
			}
		}
	}
	return fmt.Sprint(parts...)
}

// cached runs fn once per key: concurrent callers wait for that same run, and
// a slow result is reused until it expires.
func cached[T any](ctx context.Context, c *Store, k string, fn func(context.Context) (T, error)) (T, error) {
	c.mu.Lock()
	e, ok := c.entries[k]
	if ok {
		select {
		case <-e.done:
			ok = time.Now().Before(e.expires)
		default: // still running: wait for it below
		}
	}
	if ok {
		c.mu.Unlock()
		<-e.done
	} else {
		if len(c.entries) >= maxEntries {
			clear(c.entries)
		}
		e = &entry{done: make(chan struct{})}
		c.entries[k] = e
		c.mu.Unlock()

		start := time.Now()
		e.val, e.err = fn(context.WithoutCancel(ctx)) // the result may serve other requests
		if d := time.Since(start); d >= minSlow && e.err == nil {
			e.expires = time.Now().Add(min(5*d, maxTTL))
		}
		close(e.done)
	}
	v, _ := e.val.(T)
	return v, e.err
}
