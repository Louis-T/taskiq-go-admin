package redisadmin

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
)

type fakeSettings struct {
	mu    sync.Mutex
	s     store.Settings
	reads atomic.Int64
}

func (f *fakeSettings) GetSettings(context.Context) (store.Settings, error) {
	f.reads.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.s, nil
}

// fakeTrimmer counts runs; when block is set each run waits for it.
type fakeTrimmer struct {
	runs  atomic.Int64
	block chan struct{}
}

func (f *fakeTrimmer) trimAll(ctx context.Context, now time.Time, keep time.Duration) (trimResult, error) {
	f.runs.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
		}
	}
	return trimResult{mode: modeAcked}, nil
}

func intp(n int) *int { return &n }

func newFakeCleaner(s store.Settings) (*Cleaner, *fakeSettings, *fakeTrimmer) {
	fs := &fakeSettings{s: s}
	ft := &fakeTrimmer{}
	c := NewCleaner(nil, fs, maintenance.BatchConfig{Size: 10}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c.Tick = 10 * time.Millisecond
	c.trim = ft
	return c, fs, ft
}

// runLoop starts c.Run and returns a stop func that waits for it to return.
func runLoop(c *Cleaner) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCleanerScheduleOff(t *testing.T) {
	c, fs, ft := newFakeCleaner(store.Settings{RedisKeepAckedMinutes: intp(0)})
	stop := runLoop(c)
	waitFor(t, "5 ticks", func() bool { return fs.reads.Load() >= 5 })
	stop()
	if n := ft.runs.Load(); n != 0 {
		t.Errorf("runs = %d; want 0 without an interval", n)
	}
}

func TestCleanerScheduleInterval(t *testing.T) {
	c, _, ft := newFakeCleaner(store.Settings{RedisTrimIntervalMinutes: intp(1), RedisKeepAckedMinutes: intp(0)})
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var ticks atomic.Int64
	var runsAt sync.Map // tick number -> true
	c.Now = func() time.Time {
		n := ticks.Add(1)
		return base.Add(time.Duration(n) * 30 * time.Second)
	}
	c.OnDone = func() { runsAt.Store(ticks.Load(), true) }
	stop := runLoop(c)
	waitFor(t, "4 ticks", func() bool { return ticks.Load() >= 4 })
	stop()
	// Over ticks 1-4 (30 s apart) it runs at 1 and 3; tick 5, if it came
	// before stop, runs again 60 s after tick 3.
	for tick, want := range map[int64]bool{1: true, 2: false, 3: true, 4: false} {
		if _, ok := runsAt.Load(tick); ok != want {
			t.Errorf("run at tick %d = %v; want %v", tick, ok, want)
		}
	}
	if n := ft.runs.Load(); n < 2 || n > 3 {
		t.Errorf("runs = %d; want 2 (or 3 with tick 5)", n)
	}
}

func TestCleanerTriggerNeedsKeepAge(t *testing.T) {
	c, _, ft := newFakeCleaner(store.Settings{RedisTrimIntervalMinutes: intp(1)})
	stop := runLoop(c)
	if c.Trigger(context.Background()) {
		t.Error("Trigger() = true without a keep-age")
	}
	time.Sleep(50 * time.Millisecond)
	stop()
	if n := ft.runs.Load(); n != 0 {
		t.Errorf("runs = %d; want 0", n)
	}
}

func TestCleanerTriggerWhileRunning(t *testing.T) {
	c, _, ft := newFakeCleaner(store.Settings{RedisKeepAckedMinutes: intp(5)})
	ft.block = make(chan struct{})
	stop := runLoop(c)
	defer stop()
	ctx := context.Background()

	if !c.Trigger(ctx) {
		t.Fatal("first Trigger() = false")
	}
	waitFor(t, "the run to start", func() bool { return c.Status(ctx).Running })
	if c.Trigger(ctx) {
		t.Error("second Trigger() = true while a run is running")
	}
	st := c.Status(ctx)
	if !st.Running || st.KeepAcked == nil || *st.KeepAcked != 5 || st.Interval != nil {
		t.Errorf("Status = %+v; want running with keep 5, no interval", st)
	}
	close(ft.block)
	waitFor(t, "the run to end", func() bool { return c.Status(ctx).Seq == 1 })
	time.Sleep(50 * time.Millisecond)
	st = c.Status(ctx)
	if n := ft.runs.Load(); n != 1 || st.Seq != 1 || st.Running || st.Last == nil || st.Last.Mode != modeAcked {
		t.Errorf("runs = %d, Status = %+v; want exactly one finished run", n, st)
	}
}

// A trigger the loop has not picked up yet already counts as running, so a
// page refresh in between keeps polling the box.
func TestCleanerStatusRunningWhilePending(t *testing.T) {
	c, _, ft := newFakeCleaner(store.Settings{RedisKeepAckedMinutes: intp(5)})
	ctx := context.Background()
	if !c.Trigger(ctx) {
		t.Fatal("Trigger() = false")
	}
	if st := c.Status(ctx); !st.Running {
		t.Errorf("Status = %+v before the loop picks the trigger up; want Running", st)
	}
	stop := runLoop(c)
	defer stop()
	waitFor(t, "the run to end", func() bool { return c.Status(ctx).Seq == 1 })
	if st := c.Status(ctx); st.Running || ft.runs.Load() != 1 {
		t.Errorf("Status = %+v, runs = %d; want one finished run", st, ft.runs.Load())
	}
}
