// Package storetest is a conformance suite for store.Store implementations.
// A new backend passes it by calling Run from its own tests.
package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// Factory returns an empty store; it is closed by the suite.
type Factory func(t *testing.T) store.Store

var base = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// Run executes the whole suite.
func Run(t *testing.T, newStore Factory) {
	tests := map[string]func(*testing.T, store.Store){
		"ApplyAndGet":         testApplyAndGet,
		"OutOfOrderInBatch":   testOutOfOrderInBatch,
		"Pagination":          testPagination,
		"Filters":             testFilters,
		"Histogram":           testHistogram,
		"DeleteInBatches":     testDeleteInBatches,
		"AbandonRunning":      testAbandonRunning,
		"Settings":            testSettings,
		"NotFound":            testNotFound,
		"KnownTaskIDs":        testKnownTaskIDs,
		"BackupOrUnsupported": testBackup,
		"StaleAndCount":       testStaleAndCount,
		"TaskTypesAndStats":   testTaskTypesAndStats,
	}
	for name, fn := range tests {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			defer s.Close()
			fn(t, s)
		})
	}
}

func ptr[T any](v T) *T { return &v }

func queuedEvent(id, name string, at time.Time) task.Event {
	return task.Event{Kind: task.KindQueued, TaskID: id, At: at, Name: name,
		Args: json.RawMessage(`[1,"x"]`), Kwargs: json.RawMessage(`{"user":"Zoé"}`),
		Labels: json.RawMessage(`{"_retries":"0"}`), Worker: ptr("broker")}
}

func mustApply(t *testing.T, s store.Store, events ...task.Event) {
	t.Helper()
	if err := s.ApplyEvents(context.Background(), events); err != nil {
		t.Fatalf("ApplyEvents: %v", err)
	}
}

func testApplyAndGet(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s, queuedEvent("a", "app.tasks:add", base))
	mustApply(t, s, task.Event{Kind: task.KindStarted, TaskID: "a", At: base.Add(time.Second), Name: "app.tasks:add"})
	mustApply(t, s, task.Event{Kind: task.KindExecuted, TaskID: "a", At: base.Add(2 * time.Second),
		ExecutionTime: ptr(0.75), ReturnValue: json.RawMessage(`{"return_value":{"n":2}}`)})

	got, err := s.GetTask(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != task.Success || got.Name != "app.tasks:add" || *got.Worker != "broker" {
		t.Fatalf("got %+v", got)
	}
	if !got.QueuedAt.Equal(base) || !got.StartedAt.Equal(base.Add(time.Second)) || !got.FinishedAt.Equal(base.Add(2*time.Second)) {
		t.Fatalf("timestamps %v %v %v", got.QueuedAt, got.StartedAt, got.FinishedAt)
	}
	if *got.ExecutionTime != 0.75 || string(got.ReturnValue) != `{"return_value":{"n":2}}` {
		t.Fatalf("result %v %s", *got.ExecutionTime, got.ReturnValue)
	}
	if string(got.Args) != `[1,"x"]` || string(got.Kwargs) != `{"user":"Zoé"}` || string(got.Labels) != `{"_retries":"0"}` {
		t.Fatalf("json %s %s %s", got.Args, got.Kwargs, got.Labels)
	}
}

func testOutOfOrderInBatch(t *testing.T, s store.Store) {
	mustApply(t, s,
		task.Event{Kind: task.KindExecuted, TaskID: "b", At: base.Add(2 * time.Second), Error: ptr("boom")},
		task.Event{Kind: task.KindStarted, TaskID: "b", At: base.Add(time.Second), Name: "job"},
		queuedEvent("b", "job", base),
	)
	got, err := s.GetTask(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != task.Failure || got.Name != "job" || !got.QueuedAt.Equal(base) {
		t.Fatalf("got %+v", got)
	}
}

func testPagination(t *testing.T, s store.Store) {
	ctx := context.Background()
	var events []task.Event
	for i := range 7 {
		events = append(events, queuedEvent(fmt.Sprintf("t%d", i), "job", base.Add(time.Duration(i)*time.Second)))
	}
	// two tasks sharing a timestamp: the id breaks the tie
	events = append(events, queuedEvent("t3b", "job", base.Add(3*time.Second)))
	mustApply(t, s, events...)

	ids := func(r *store.ListResult) (out []string) {
		for _, tk := range r.Tasks {
			out = append(out, tk.ID)
		}
		return
	}
	page1, err := s.ListTasks(ctx, store.ListQuery{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ids(page1)) != "[t6 t5 t4]" || page1.PrevCursor != nil || page1.NextCursor == nil {
		t.Fatalf("page1 %v %+v", ids(page1), page1)
	}
	page2, _ := s.ListTasks(ctx, store.ListQuery{Limit: 3, Cursor: page1.NextCursor, Direction: store.Next})
	if fmt.Sprint(ids(page2)) != "[t3b t3 t2]" || page2.PrevCursor == nil || page2.NextCursor == nil {
		t.Fatalf("page2 %v", ids(page2))
	}
	page3, _ := s.ListTasks(ctx, store.ListQuery{Limit: 3, Cursor: page2.NextCursor, Direction: store.Next})
	if fmt.Sprint(ids(page3)) != "[t1 t0]" || page3.NextCursor != nil {
		t.Fatalf("page3 %v next=%v", ids(page3), page3.NextCursor)
	}
	back, _ := s.ListTasks(ctx, store.ListQuery{Limit: 3, Cursor: page3.PrevCursor, Direction: store.Prev})
	if fmt.Sprint(ids(back)) != "[t3b t3 t2]" || back.PrevCursor == nil || back.NextCursor == nil {
		t.Fatalf("back %v", ids(back))
	}
	first, _ := s.ListTasks(ctx, store.ListQuery{Limit: 3, Cursor: back.PrevCursor, Direction: store.Prev})
	if fmt.Sprint(ids(first)) != "[t6 t5 t4]" || first.PrevCursor != nil {
		t.Fatalf("first %v prev=%v", ids(first), first.PrevCursor)
	}
	if _, err := s.ListTasks(ctx, store.ListQuery{Limit: 0}); err == nil {
		t.Fatal("limit 0 accepted")
	}
}

func testFilters(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s,
		queuedEvent("1", "app.tasks:send_email", base),
		queuedEvent("2", "app.tasks:SEND_sms", base.Add(time.Minute)),
		queuedEvent("3", "app.reports:build_100%_report", base.Add(2*time.Minute)),
		task.Event{Kind: task.KindQueued, TaskID: "4", At: base.Add(3 * time.Minute), Name: "other",
			Args: json.RawMessage(`["needle-ARG"]`), Kwargs: json.RawMessage(`{"k":"v"}`)},
		task.Event{Kind: task.KindStarted, TaskID: "2", At: base.Add(time.Minute)},
	)
	cases := []struct {
		name string
		f    store.Filter
		want int
	}{
		{"no filter", store.Filter{}, 4},
		{"fts", store.Filter{Name: "send"}, 2},
		{"fts case", store.Filter{Name: "SEND_EMAIL"}, 1},
		{"short", store.Filter{Name: "ms"}, 1},
		{"like escape percent", store.Filter{Name: "%"}, 1},
		{"like escape underscore", store.Filter{Name: "_"}, 3},
		{"fts quotes", store.Filter{Name: `a"b`}, 0},
		{"state", store.Filter{State: task.Running}, 1},
		{"state+name", store.Filter{State: task.Queued, Name: "send"}, 1},
		{"args", store.Filter{ArgsSearch: "needle-arg"}, 1},
		{"kwargs", store.Filter{ArgsSearch: "zoé"}, 3},
		{"args percent literal", store.Filter{ArgsSearch: "%"}, 0},
		{"from", store.Filter{From: base.Add(time.Minute)}, 3},
		{"to exclusive", store.Filter{To: base.Add(time.Minute)}, 1},
		{"range", store.Filter{From: base.Add(time.Minute), To: base.Add(3 * time.Minute)}, 2},
	}
	for _, c := range cases {
		res, err := s.ListTasks(ctx, store.ListQuery{Filter: c.f, Limit: 100})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(res.Tasks) != c.want {
			t.Errorf("%s: got %d tasks, want %d", c.name, len(res.Tasks), c.want)
		}
	}
}

func testHistogram(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s,
		queuedEvent("1", "a", base.Add(10*time.Second)),
		queuedEvent("2", "a", base.Add(20*time.Second)),
		queuedEvent("3", "a", base.Add(70*time.Second)),
		task.Event{Kind: task.KindExecuted, TaskID: "2", At: base.Add(30 * time.Second)},
	)
	rows, err := s.Histogram(ctx, store.HistogramQuery{Filter: store.Filter{From: base, To: base.Add(2 * time.Minute)}, Bucket: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, r := range rows {
		got[fmt.Sprintf("%s/%s", r.Start.Sub(base), r.State)] = r.Count
	}
	want := map[string]int64{"0s/queued": 1, "0s/success": 1, "1m0s/queued": 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}

	// Offset shifts boundaries: with 1h buckets offset by +30m (UTC+00:30),
	// buckets start at hh:30 UTC, so all three tasks (12:00:10..12:01:10)
	// fall into the bucket starting at 11:30.
	rows, err = s.Histogram(ctx, store.HistogramQuery{Filter: store.Filter{From: base.Add(-time.Hour), To: base.Add(time.Hour)}, Bucket: time.Hour, Offset: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, r := range rows {
		if !r.Start.Equal(base.Add(-30 * time.Minute)) {
			t.Fatalf("offset bucket start %v", r.Start)
		}
		total += r.Count
	}
	if total != 3 {
		t.Fatalf("offset total %d", total)
	}
}

func testDeleteInBatches(t *testing.T, s store.Store) {
	ctx := context.Background()
	var events []task.Event
	for i := range 25 {
		events = append(events, queuedEvent(fmt.Sprintf("d%02d", i), "purge_me", base.Add(time.Duration(i)*time.Minute)))
	}
	mustApply(t, s, events...)
	cutoff := base.Add(20 * time.Minute)
	n, err := s.DeleteQueuedBefore(ctx, cutoff, 8)
	if err != nil || n != 8 {
		t.Fatalf("first batch n=%d err=%v", n, err)
	}
	n, _ = s.DeleteQueuedBefore(ctx, cutoff, 8)
	if n != 8 {
		t.Fatalf("second batch %d", n)
	}
	n, _ = s.DeleteQueuedBefore(ctx, cutoff, 8)
	if n != 4 {
		t.Fatalf("last batch %d", n)
	}
	res, _ := s.ListTasks(ctx, store.ListQuery{Limit: 100})
	if len(res.Tasks) != 5 {
		t.Fatalf("remaining %d", len(res.Tasks))
	}
	// the name index must follow deletions
	res, _ = s.ListTasks(ctx, store.ListQuery{Limit: 100, Filter: store.Filter{Name: "purge"}})
	if len(res.Tasks) != 5 {
		t.Fatalf("fts remaining %d", len(res.Tasks))
	}
}

func testAbandonRunning(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s,
		task.Event{Kind: task.KindStarted, TaskID: "old", At: base, Name: "x"},
		task.Event{Kind: task.KindStarted, TaskID: "new", At: base.Add(time.Hour), Name: "x"},
		task.Event{Kind: task.KindExecuted, TaskID: "done", At: base},
	)
	n, err := s.AbandonRunning(ctx, base.Add(time.Minute), 100)
	if err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got, _ := s.GetTask(ctx, "old")
	if got.State != task.Abandoned {
		t.Fatalf("old state %s", got.State)
	}
	got, _ = s.GetTask(ctx, "new")
	if got.State != task.Running {
		t.Fatalf("new state %s", got.State)
	}
	// a late result still lands
	mustApply(t, s, task.Event{Kind: task.KindExecuted, TaskID: "old", At: base.Add(2 * time.Minute)})
	got, _ = s.GetTask(ctx, "old")
	if got.State != task.Success {
		t.Fatalf("after executed %s", got.State)
	}
}

func testStaleAndCount(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s,
		queuedEvent("q-old", "job", base),
		queuedEvent("q-new", "job", base.Add(time.Hour)),
		task.Event{Kind: task.KindStarted, TaskID: "r-old", At: base, Name: "job"},
		task.Event{Kind: task.KindStarted, TaskID: "r-new", At: base.Add(time.Hour), Name: "job"},
		task.Event{Kind: task.KindExecuted, TaskID: "done", At: base},
	)
	cut := base.Add(30 * time.Minute)
	cases := []struct {
		name string
		f    store.StaleFilter
		want int64
	}{
		{"both", store.StaleFilter{RunningStartedBefore: cut, QueuedBefore: cut}, 2},
		{"running only", store.StaleFilter{RunningStartedBefore: cut}, 1},
		{"queued only", store.StaleFilter{QueuedBefore: cut}, 1},
		{"disabled", store.StaleFilter{}, 0},
	}
	for _, c := range cases {
		n, err := s.Count(ctx, store.Filter{Stale: &c.f})
		if err != nil || n != c.want {
			t.Errorf("%s: n=%d err=%v want %d", c.name, n, err, c.want)
		}
	}
	if n, _ := s.Count(ctx, store.Filter{}); n != 5 {
		t.Errorf("count all = %d", n)
	}
}

func testTaskTypesAndStats(t *testing.T, s store.Store) {
	ctx := context.Background()
	var events []task.Event
	for i := range 20 {
		id := fmt.Sprintf("a%02d", i)
		q := base.Add(time.Duration(i) * time.Second)
		events = append(events,
			queuedEvent(id, "app:alpha", q),
			task.Event{Kind: task.KindStarted, TaskID: id, At: q.Add(time.Duration(i) * 100 * time.Millisecond)},
		)
		var errMsg *string
		if i%4 == 0 {
			errMsg = ptr("boom")
		}
		events = append(events, task.Event{Kind: task.KindExecuted, TaskID: id, At: q.Add(time.Minute), ExecutionTime: ptr(float64(i + 1)), Error: errMsg})
	}
	events = append(events, queuedEvent("b1", "app:beta", base), task.Event{Kind: task.KindExecuted, TaskID: "orphan", At: base})
	mustApply(t, s, events...)

	types, err := s.TaskTypes(ctx, base.Add(-time.Hour), base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(types) != 2 || types[0].Name != "app:alpha" || types[0].Total != 20 || types[1].Name != "app:beta" {
		t.Fatalf("types %+v", types)
	}
	a := types[0]
	if a.Counts[task.Failure] != 5 || a.Counts[task.Success] != 15 || *a.FailureRate() != 0.25 || *a.MaxExec != 20 || *a.AvgExec != 10.5 {
		t.Fatalf("alpha summary %+v rate=%v", a, *a.FailureRate())
	}
	if types[1].FailureRate() != nil {
		t.Fatal("beta has no finished run")
	}

	st, err := s.TypeStats(ctx, "app:alpha", base.Add(-time.Hour), base.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if *st.P50Exec != 10 || *st.P95Exec != 19 {
		t.Fatalf("exec percentiles %v %v", *st.P50Exec, *st.P95Exec)
	}
	if *st.P50Wait != 900*time.Millisecond || *st.P95Wait != 1800*time.Millisecond {
		t.Fatalf("wait percentiles %v %v", *st.P50Wait, *st.P95Wait)
	}
	empty, err := s.TypeStats(ctx, "app:nothing", base.Add(-time.Hour), base.Add(time.Hour))
	if err != nil || empty.Total != 0 || empty.P50Exec != nil {
		t.Fatalf("empty %+v %v", empty, err)
	}
	res, _ := s.ListTasks(ctx, store.ListQuery{Limit: 100, Filter: store.Filter{TaskName: "app:beta"}})
	if len(res.Tasks) != 1 {
		t.Fatalf("exact name filter: %d", len(res.Tasks))
	}
}

func testSettings(t *testing.T, s store.Store) {
	ctx := context.Background()
	got, err := s.GetSettings(ctx)
	// Every feature is off by default: no cleanup, no stale tagging (some
	// deployments only run long tasks).
	if err != nil || got.DeleteOldTTLMinutes != nil || got.StaleRunningMinutes != nil || got.StaleQueuedMinutes != nil {
		t.Fatalf("defaults %+v %v", got, err)
	}
	if err := s.SaveSettings(ctx, store.Settings{DeleteOldTTLMinutes: ptr(90)}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSettings(ctx)
	if got.DeleteOldTTLMinutes == nil || *got.DeleteOldTTLMinutes != 90 {
		t.Fatalf("saved %+v", got)
	}
	if err := s.SaveSettings(ctx, store.Settings{DeleteOldTTLMinutes: ptr(0)}); err == nil {
		t.Fatal("ttl 0 accepted")
	}
	if err := s.SaveSettings(ctx, store.Settings{DeleteOldTTLMinutes: ptr(90), StaleRunningMinutes: ptr(15), StaleQueuedMinutes: ptr(5)}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSettings(ctx)
	if *got.DeleteOldTTLMinutes != 90 || *got.StaleRunningMinutes != 15 || *got.StaleQueuedMinutes != 5 {
		t.Fatalf("all settings %+v", got)
	}
	if err := s.SaveSettings(ctx, store.Settings{}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSettings(ctx)
	if got.DeleteOldTTLMinutes != nil || got.StaleRunningMinutes != nil || got.StaleQueuedMinutes != nil {
		t.Fatalf("cleared %+v", got)
	}
	if got.RedisTrimIntervalMinutes != nil || got.RedisKeepAckedMinutes != nil {
		t.Fatalf("redis settings not nil by default %+v", got)
	}
	if err := s.SaveSettings(ctx, store.Settings{RedisTrimIntervalMinutes: ptr(5), RedisKeepAckedMinutes: ptr(0)}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSettings(ctx)
	if got.RedisTrimIntervalMinutes == nil || *got.RedisTrimIntervalMinutes != 5 || got.RedisKeepAckedMinutes == nil || *got.RedisKeepAckedMinutes != 0 {
		t.Fatalf("redis settings %+v", got)
	}
	if err := s.SaveSettings(ctx, store.Settings{RedisKeepAckedMinutes: ptr(-1)}); err == nil || !strings.Contains(err.Error(), "redis_keep_acked_minutes") {
		t.Fatalf("keep acked -1: %v", err)
	}
	if err := s.SaveSettings(ctx, store.Settings{RedisTrimIntervalMinutes: ptr(0)}); err == nil || !strings.Contains(err.Error(), "redis_trim_interval_minutes") {
		t.Fatalf("trim interval 0: %v", err)
	}
	if err := s.SaveSettings(ctx, store.Settings{}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetSettings(ctx)
	if got.RedisTrimIntervalMinutes != nil || got.RedisKeepAckedMinutes != nil {
		t.Fatalf("redis settings not cleared %+v", got)
	}
}

func testNotFound(t *testing.T, s store.Store) {
	if _, err := s.GetTask(context.Background(), "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func testKnownTaskIDs(t *testing.T, s store.Store) {
	ctx := context.Background()
	mustApply(t, s, queuedEvent("a", "x", base), queuedEvent("b", "x", base))
	got, err := s.KnownTaskIDs(ctx, []string{"a", "missing", "b", "a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !got["a"] || !got["b"] {
		t.Fatalf("KnownTaskIDs = %v; want a and b", got)
	}
	if got, err := s.KnownTaskIDs(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("KnownTaskIDs(nil) = %v, %v; want empty", got, err)
	}
}

func testBackup(t *testing.T, s store.Store) {
	mustApply(t, s, queuedEvent("1", "a", base))
	var buf bytes.Buffer
	err := s.Backup(context.Background(), &buf)
	if errors.Is(err, store.ErrUnsupported) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("empty backup")
	}
}
