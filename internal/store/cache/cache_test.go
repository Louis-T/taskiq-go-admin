package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
)

// slowStore counts TaskTypes calls, each taking delay.
type slowStore struct {
	store.Store
	delay time.Duration
	calls atomic.Int32
}

func (s *slowStore) TaskTypes(context.Context, time.Time, time.Time) ([]store.TypeSummary, error) {
	s.calls.Add(1)
	time.Sleep(s.delay)
	return []store.TypeSummary{{Name: "t"}}, nil
}

func TestSlowResultIsSharedAndReused(t *testing.T) {
	s := &slowStore{delay: 150 * time.Millisecond}
	c := New(s)
	now := time.Now().Truncate(maxTTL).Add(maxTTL / 2) // mid-window: +1ms stays in the same key
	var wg sync.WaitGroup
	for range 5 { // concurrent identical requests share one run
		wg.Go(func() {
			got, err := c.TaskTypes(context.Background(), now.Add(-time.Hour), now)
			if err != nil || len(got) != 1 {
				t.Errorf("got %v %v", got, err)
			}
		})
	}
	wg.Wait()
	// a later request with a slightly moved "now" reuses the slow result
	c.TaskTypes(context.Background(), now.Add(-time.Hour+time.Millisecond), now.Add(time.Millisecond))
	if n := s.calls.Load(); n != 1 {
		t.Fatalf("store called %d times, want 1", n)
	}
}

func TestFastResultIsNotCached(t *testing.T) {
	s := &slowStore{}
	c := New(s)
	now := time.Now()
	c.TaskTypes(context.Background(), now, now)
	c.TaskTypes(context.Background(), now, now)
	if n := s.calls.Load(); n != 2 {
		t.Fatalf("store called %d times, want 2", n)
	}
}

func TestHistogramKeys(t *testing.T) {
	at := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	q := func(f store.Filter) store.HistogramQuery {
		f.From, f.To = at, at.Add(time.Hour)
		return store.HistogramQuery{Filter: f, Bucket: time.Minute}
	}
	byState := histogramKey(q(store.Filter{State: "failure"}))
	bySearch := histogramKey(q(store.Filter{Name: "failure"}))
	staleOff := histogramKey(q(store.Filter{Stale: &store.StaleFilter{}}))
	noStale := histogramKey(q(store.Filter{}))
	if byState == bySearch || staleOff == noStale {
		t.Fatal("different filters share a key")
	}
	moved := q(store.Filter{State: "failure"})
	moved.From, moved.To = moved.From.Add(time.Second), moved.To.Add(time.Second) // same 30s window
	if histogramKey(moved) != byState {
		t.Fatal("a window moved by 1s must reuse the key")
	}
	s1, s2 := &store.StaleFilter{QueuedBefore: at}, &store.StaleFilter{QueuedBefore: at}
	if histogramKey(q(store.Filter{Stale: s1})) != histogramKey(q(store.Filter{Stale: s2})) {
		t.Fatal("equal stale filters must share a key whatever their address")
	}
}

// panicStore panics on TaskTypes.
type panicStore struct{ store.Store }

func (panicStore) TaskTypes(context.Context, time.Time, time.Time) ([]store.TypeSummary, error) {
	panic("boom")
}

func TestPanicDoesNotBlockLaterCallers(t *testing.T) {
	c := New(panicStore{})
	now := time.Now()
	for range 2 {
		done := make(chan error, 1)
		go func() { _, err := c.TaskTypes(context.Background(), now, now); done <- err }()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("expected an error from the panicking query")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("caller blocked after a panic")
		}
	}
}

func TestWaiterHonoursItsContext(t *testing.T) {
	c := New(&slowStore{delay: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.TaskTypes(ctx, time.Now(), time.Now()); err == nil || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("err=%v after %v", err, time.Since(start))
	}
}
