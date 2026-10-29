package redisadmin

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newSharedReader returns a Reader whose overview fetch is counted and, when
// gate is set, waits for it.
func newSharedReader(ttl time.Duration, gate chan struct{}, fail *atomic.Bool) (*Reader, *atomic.Int64) {
	var fetches atomic.Int64
	r := &Reader{overviewTTL: ttl}
	r.fetch = func(ctx context.Context) (Overview, error) {
		fetches.Add(1)
		if gate != nil {
			<-gate
		}
		if fail != nil && fail.Load() {
			return Overview{}, errors.New("down")
		}
		return Overview{FetchedAt: time.Now()}, nil
	}
	return r, &fetches
}

func TestOverviewSharedByConcurrentCallers(t *testing.T) {
	gate := make(chan struct{})
	r, fetches := newSharedReader(time.Minute, gate, nil)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.Overview(context.Background()); err != nil {
				t.Errorf("Overview: %v", err)
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // let every caller join the fetch
	close(gate)
	wg.Wait()
	if n := fetches.Load(); n != 1 {
		t.Errorf("fetches = %d; want 1 for 10 concurrent callers", n)
	}
}

func TestOverviewReusedWithinTTL(t *testing.T) {
	r, fetches := newSharedReader(50*time.Millisecond, nil, nil)
	ctx := context.Background()
	r.Overview(ctx)
	r.Overview(ctx)
	if n := fetches.Load(); n != 1 {
		t.Errorf("fetches = %d within the TTL; want 1", n)
	}
	time.Sleep(70 * time.Millisecond)
	r.Overview(ctx)
	if n := fetches.Load(); n != 2 {
		t.Errorf("fetches = %d after the TTL; want 2", n)
	}
}

func TestOverviewErrorNotReused(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	r, fetches := newSharedReader(time.Minute, nil, &fail)
	ctx := context.Background()
	if _, err := r.Overview(ctx); err == nil {
		t.Fatal("want the fetch error")
	}
	fail.Store(false)
	if _, err := r.Overview(ctx); err != nil {
		t.Fatalf("second Overview: %v; want a new fetch", err)
	}
	if n := fetches.Load(); n != 2 {
		t.Errorf("fetches = %d; want 2", n)
	}
}

func TestOverviewCallerLeavingKeepsFetch(t *testing.T) {
	gate := make(chan struct{})
	r, fetches := newSharedReader(time.Minute, gate, nil)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { _, err := r.Overview(ctx); errc <- err }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("leaving caller: err = %v; want context.Canceled", err)
	}
	close(gate)
	if _, err := r.Overview(context.Background()); err != nil {
		t.Fatalf("other caller: %v", err)
	}
	if n := fetches.Load(); n != 1 {
		t.Errorf("fetches = %d; want the first fetch reused", n)
	}
}
