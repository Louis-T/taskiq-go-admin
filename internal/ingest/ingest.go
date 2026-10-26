// Package ingest serializes task events into the store with group commit.
//
// Every middleware report is a small write. Instead of one transaction per
// HTTP request (one fsync and one lock acquisition each), a single goroutine
// drains whatever is queued and applies it in one transaction. Under load this
// multiplies throughput; when idle a lone event is written immediately, so no
// latency is added. Handlers still wait for the commit before answering, so a
// 200 means the event is committed.
package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

var ErrClosed = errors.New("ingester is closed")

type request struct {
	event task.Event
	done  chan error
}

// Ingester batches events into store.ApplyEvents.
type Ingester struct {
	store    store.Store
	maxBatch int
	log      *slog.Logger
	// OnCommit, when set, is called after events were written.
	OnCommit func()

	queue    chan request
	mu       sync.RWMutex
	closed   bool
	finished chan struct{}
}

// New creates an ingester; call Run to start it.
func New(s store.Store, maxBatch int, log *slog.Logger) *Ingester {
	if maxBatch <= 0 {
		maxBatch = 500
	}
	return &Ingester{
		store:    s,
		maxBatch: maxBatch,
		log:      log,
		queue:    make(chan request, maxBatch),
		finished: make(chan struct{}),
	}
}

// Submit queues an event and waits until it is committed.
func (i *Ingester) Submit(ctx context.Context, e task.Event) error {
	req := request{event: e, done: make(chan error, 1)}
	i.mu.RLock()
	if i.closed {
		i.mu.RUnlock()
		return ErrClosed
	}
	select {
	case i.queue <- req:
		i.mu.RUnlock()
	case <-ctx.Done():
		i.mu.RUnlock()
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		// The event is queued and will still be applied.
		return ctx.Err()
	}
}

// Run processes the queue until Close is called and the queue is drained.
func (i *Ingester) Run() {
	defer close(i.finished)
	batch := make([]request, 0, i.maxBatch)
	for first := range i.queue {
		batch = append(batch[:0], first)
	drain:
		for len(batch) < i.maxBatch {
			select {
			case r, ok := <-i.queue:
				if !ok {
					break drain
				}
				batch = append(batch, r)
			default:
				break drain
			}
		}
		i.flush(batch)
	}
}

// Close stops accepting events, then waits for queued ones to be written,
// or for ctx to end (a stuck database must not block the process exit).
func (i *Ingester) Close(ctx context.Context) error {
	i.mu.Lock()
	if !i.closed {
		i.closed = true
		close(i.queue)
	}
	i.mu.Unlock()
	select {
	case <-i.finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (i *Ingester) flush(batch []request) {
	// Writes must complete even during shutdown: use a background context.
	ctx := context.Background()
	events := make([]task.Event, len(batch))
	for k, r := range batch {
		events[k] = r.event
	}
	err := i.store.ApplyEvents(ctx, events)
	if err == nil || len(batch) == 1 {
		if err == nil {
			i.committed()
		} else {
			i.log.Error("apply event", "task_id", batch[0].event.TaskID, "kind", batch[0].event.Kind.String(), "err", err)
		}
		for _, r := range batch {
			r.done <- err
		}
		return
	}
	// One bad event must not fail its neighbours: retry one by one.
	i.log.Warn("batch apply failed, retrying events individually", "size", len(batch), "err", err)
	for _, r := range batch {
		err := i.store.ApplyEvents(ctx, []task.Event{r.event})
		if err != nil {
			i.log.Error("apply event", "task_id", r.event.TaskID, "kind", r.event.Kind.String(), "err", err)
		}
		r.done <- err
	}
	i.committed()
}

func (i *Ingester) committed() {
	if i.OnCommit != nil {
		i.OnCommit()
	}
}
