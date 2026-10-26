package ingest

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// fakeStore fails any batch containing the task id "bad".
type fakeStore struct {
	store.Store
	mu      sync.Mutex
	batches [][]string
	applied map[string]int
}

func (f *fakeStore) ApplyEvents(_ context.Context, events []task.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []string
	for _, e := range events {
		ids = append(ids, e.TaskID)
		if e.TaskID == "bad" {
			f.batches = append(f.batches, ids)
			return errors.New("boom")
		}
	}
	f.batches = append(f.batches, ids)
	for _, id := range ids {
		f.applied[id]++
	}
	return nil
}

func TestBadEventDoesNotFailNeighbours(t *testing.T) {
	fs := &fakeStore{applied: map[string]int{}}
	ing := New(fs, 50, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go ing.Run()

	var wg sync.WaitGroup
	errs := sync.Map{}
	for _, id := range []string{"a", "b", "bad", "c", "d"} {
		wg.Go(func() {
			errs.Store(id, ing.Submit(context.Background(), task.Event{TaskID: id}))
		})
	}
	wg.Wait()
	ing.Close(context.Background())

	errs.Range(func(k, v any) bool {
		if (k == "bad") != (v != nil) {
			t.Errorf("%s: err = %v", k, v)
		}
		return true
	})
	for _, id := range []string{"a", "b", "c", "d"} {
		if fs.applied[id] != 1 {
			t.Errorf("%s applied %d times", id, fs.applied[id])
		}
	}
}

func TestSubmitAfterClose(t *testing.T) {
	ing := New(&fakeStore{applied: map[string]int{}}, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go ing.Run()
	ing.Close(context.Background())
	if err := ing.Submit(context.Background(), task.Event{TaskID: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v", err)
	}
}

func TestCloseFlushesQueue(t *testing.T) {
	fs := &fakeStore{applied: map[string]int{}}
	ing := New(fs, 10, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b", "c"} {
		wg.Go(func() { _ = ing.Submit(context.Background(), task.Event{TaskID: id}) })
	}
	// Run starts late: events are queued, then Close must still flush them.
	for len(ing.queue) < 3 {
	}
	go ing.Run()
	wg.Wait()
	ing.Close(context.Background())
	if len(fs.applied) != 3 {
		t.Fatalf("applied %v", fs.applied)
	}
}
