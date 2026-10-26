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
	maxEntries = 200                    // the cache is emptied when full
	// queryTimeout bounds a shared query, which no single request cancels.
	queryTimeout = 2 * time.Minute
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
	return cached(ctx, c, histogramKey(q), func(ctx context.Context) ([]store.HistogramRow, error) {
		return c.Store.Histogram(ctx, q)
	})
}

func (c *Store) TaskTypes(ctx context.Context, from, to time.Time) ([]store.TypeSummary, error) {
	return cached(ctx, c, key("types", round(from), round(to)), func(ctx context.Context) ([]store.TypeSummary, error) {
		return c.Store.TaskTypes(ctx, from, to)
	})
}

func (c *Store) TypeStats(ctx context.Context, name string, from, to time.Time) (*store.TypeStats, error) {
	return cached(ctx, c, key("stats", name, round(from), round(to)), func(ctx context.Context) (*store.TypeStats, error) {
		return c.Store.TypeStats(ctx, name, from, to)
	})
}

// histogramKey is built from the whole query, so a field added to the filter
// later cannot be silently left out of the key.
func histogramKey(q store.HistogramQuery) string {
	hasStale, stale := q.Stale != nil, store.StaleFilter{}
	if hasStale {
		stale = store.StaleFilter{
			RunningStartedBefore: round(q.Stale.RunningStartedBefore),
			QueuedBefore:         round(q.Stale.QueuedBefore),
		}
	}
	q.Stale = nil // compared by value below, not by pointer address
	q.From, q.To = round(q.From), round(q.To)
	return key("hist", q, hasStale, stale)
}

// round truncates times used in keys: windows ending "now" move on every
// request and would otherwise never match an entry.
func round(t time.Time) time.Time { return t.Truncate(maxTTL) }

// key renders the parts in Go syntax, so strings stay quoted and separated.
func key(kind string, parts ...any) string { return fmt.Sprintf("%s%#v", kind, parts) }

// cached runs fn once per key: concurrent callers wait for that same run, and
// a slow result is reused until it expires. The query runs in its own
// goroutine, so a caller that goes away does not cancel it for the others.
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
	if !ok {
		if len(c.entries) >= maxEntries {
			clear(c.entries)
		}
		e = &entry{done: make(chan struct{})}
		c.entries[k] = e
		go run(context.WithoutCancel(ctx), e, fn)
	}
	c.mu.Unlock()

	select {
	case <-e.done:
		v, _ := e.val.(T)
		return v, e.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// run fills e with fn's result. It always closes e.done, even if fn panics, so
// waiters are never stuck; a failed or fast result is not kept.
func run[T any](ctx context.Context, e *entry, fn func(context.Context) (T, error)) {
	defer close(e.done)
	defer func() {
		if r := recover(); r != nil {
			e.err = fmt.Errorf("cached query panicked: %v", r)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	start := time.Now()
	e.val, e.err = fn(ctx)
	if d := time.Since(start); d >= minSlow && e.err == nil {
		e.expires = time.Now().Add(min(5*d, maxTTL))
	}
}
