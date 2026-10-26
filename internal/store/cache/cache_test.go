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
