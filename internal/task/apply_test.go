package task

import (
	"encoding/json"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func queued(at time.Time, labels string) Event {
	e := Event{Kind: KindQueued, TaskID: "t1", At: at, Name: "app:job", Args: json.RawMessage(`[1]`), Kwargs: json.RawMessage(`{}`), Worker: ptr("broker")}
	if labels != "" {
		e.Labels = json.RawMessage(labels)
	}
	return e
}

func started(at time.Time, labels string) Event {
	e := queued(at, labels)
	e.Kind = KindStarted
	return e
}

func executed(at time.Time, errMsg *string) Event {
	return Event{Kind: KindExecuted, TaskID: "t1", At: at, Error: errMsg, ExecutionTime: ptr(1.5), ReturnValue: json.RawMessage(`{"return_value":2}`)}
}

func applyAll(events ...Event) Task {
	var cur *Task
	for _, e := range events {
		next := Apply(cur, e)
		cur = &next
	}
	return *cur
}

func permutations(events []Event) [][]Event {
	if len(events) <= 1 {
		return [][]Event{events}
	}
	var out [][]Event
	for i := range events {
		rest := append(append([]Event{}, events[:i]...), events[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]Event{events[i]}, p...))
		}
	}
	return out
}

func TestApplyAnyOrderConverges(t *testing.T) {
	q := queued(t0, `{}`)
	s := started(t0.Add(time.Second), `{}`)
	x := executed(t0.Add(3*time.Second), nil)

	for _, order := range permutations([]Event{q, s, x}) {
		got := applyAll(order...)
		kinds := ""
		for _, e := range order {
			kinds += e.Kind.String() + ">"
		}
		if got.State != Success {
			t.Errorf("%s state = %s, want success", kinds, got.State)
		}
		if got.Name != "app:job" {
			t.Errorf("%s name = %q", kinds, got.Name)
		}
		if !got.QueuedAt.Equal(q.At) {
			t.Errorf("%s queuedAt = %v, want %v", kinds, got.QueuedAt, q.At)
		}
		if got.StartedAt == nil || !got.StartedAt.Equal(s.At) {
			t.Errorf("%s startedAt = %v, want %v", kinds, got.StartedAt, s.At)
		}
		if got.FinishedAt == nil || !got.FinishedAt.Equal(x.At) {
			t.Errorf("%s finishedAt = %v", kinds, got.FinishedAt)
		}
		if string(got.ReturnValue) != `{"return_value":2}` {
			t.Errorf("%s returnValue = %s", kinds, got.ReturnValue)
		}
	}
}

func TestApplyFailure(t *testing.T) {
	got := applyAll(queued(t0, ""), started(t0, ""), executed(t0.Add(time.Second), ptr("ValueError('boom')")))
	if got.State != Failure || got.Error == nil || *got.Error != "ValueError('boom')" {
		t.Fatalf("got %+v", got)
	}
}

func TestExecutedFirstEstimatesTimes(t *testing.T) {
	got := applyAll(executed(t0, nil))
	if got.State != Success || got.Name != "" {
		t.Fatalf("got %+v", got)
	}
	want := t0.Add(-1500 * time.Millisecond)
	if !got.QueuedAt.Equal(want) || !got.StartedAt.Equal(want) {
		t.Fatalf("estimated times %v / %v, want %v", got.QueuedAt, got.StartedAt, want)
	}
}

func TestDuplicatesAreIdempotent(t *testing.T) {
	q, s, x := queued(t0, ""), started(t0.Add(time.Second), ""), executed(t0.Add(2*time.Second), nil)
	once := applyAll(q, s, x)
	twice := applyAll(q, s, x, x, s, q)
	if once.State != twice.State || !once.QueuedAt.Equal(twice.QueuedAt) || !once.StartedAt.Equal(*twice.StartedAt) || !once.FinishedAt.Equal(*twice.FinishedAt) {
		t.Fatalf("once %+v\ntwice %+v", once, twice)
	}
}

func TestExecutedOverridesAbandoned(t *testing.T) {
	cur := applyAll(queued(t0, ""), started(t0, ""))
	cur.State = Abandoned
	got := Apply(&cur, executed(t0.Add(time.Minute), nil))
	if got.State != Success {
		t.Fatalf("state = %s", got.State)
	}
}

func TestLateStartedKeepsAbandoned(t *testing.T) {
	cur := applyAll(queued(t0, ""), started(t0, ""))
	cur.State = Abandoned
	if got := Apply(&cur, started(t0, "")); got.State != Abandoned {
		t.Fatalf("state = %s", got.State)
	}
}

func TestStaleExecutedIgnored(t *testing.T) {
	got := applyAll(queued(t0, ""), started(t0, ""), executed(t0.Add(5*time.Second), nil), executed(t0.Add(2*time.Second), ptr("old")))
	if got.State != Success || got.Error != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestRetryRestartsLifecycle(t *testing.T) {
	fail := executed(t0.Add(2*time.Second), ptr("boom"))
	got := applyAll(queued(t0, ""), started(t0.Add(time.Second), ""), fail,
		queued(t0.Add(3*time.Second), `{"_retries":"1"}`))
	if got.State != Queued || got.Attempt != 1 || got.FinishedAt != nil || got.Error != nil || got.StartedAt != nil {
		t.Fatalf("after retry queued: %+v", got)
	}
	got = Apply(&got, started(t0.Add(4*time.Second), `{"_retries":"1"}`))
	if got.State != Running {
		t.Fatalf("after retry started: %s", got.State)
	}
	got = Apply(&got, executed(t0.Add(5*time.Second), nil))
	if got.State != Success || got.Attempt != 1 {
		t.Fatalf("after retry executed: %+v", got)
	}
}

func TestRetryResultArrivingBeforeItsStart(t *testing.T) {
	// attempt 0 fails, then attempt 1's result overtakes attempt 1's queued/started.
	got := applyAll(
		queued(t0, ""), started(t0.Add(time.Second), ""), executed(t0.Add(2*time.Second), ptr("boom")),
		executed(t0.Add(6*time.Second), nil),
		started(t0.Add(4*time.Second), `{"_retries":1}`),
		queued(t0.Add(3*time.Second), `{"_retries":1}`),
	)
	if got.State != Success || got.Attempt != 1 || got.Error != nil {
		t.Fatalf("got %+v", got)
	}
	if !got.StartedAt.Equal(t0.Add(4 * time.Second)) {
		t.Fatalf("startedAt = %v", got.StartedAt)
	}
}

func TestOldAttemptEventsOnlyFill(t *testing.T) {
	cur := applyAll(started(t0.Add(4*time.Second), `{"_retries":2}`))
	got := Apply(&cur, queued(t0, `{"_retries":1}`))
	if got.State != Running || got.Attempt != 2 || !got.QueuedAt.Equal(t0.Add(4*time.Second)) {
		t.Fatalf("got %+v", got)
	}
}

func TestEventAttempt(t *testing.T) {
	cases := map[string]int{
		``:                     0,
		`{}`:                   0,
		`{"_retries":3}`:       3,
		`{"_retries":"2"}`:     2,
		`{"_retries":"x"}`:     0,
		`{"_retries":-1}`:      0,
		`["not","an","obj"]`:   0,
		`{"_retries":null}`:    0,
		`{"_retries":" 4 "}`:   4,
		`{"_retries":1.0}`:     1,
		`{"other":"label"}`:    0,
		`{"_retries":"1.5"}`:   0,
		`{"_retries":true}`:    0,
		`{"_retries":{"a":1}}`: 0,
	}
	for labels, want := range cases {
		e := Event{Labels: json.RawMessage(labels)}
		if labels == "" {
			e.Labels = nil
		}
		if got := e.Attempt(); got != want {
			t.Errorf("Attempt(%s) = %d, want %d", labels, got, want)
		}
	}
}

func TestApplyDoesNotMutateInput(t *testing.T) {
	cur := applyAll(queued(t0, ""))
	before := cur
	_ = Apply(&cur, started(t0.Add(time.Second), ""))
	if cur.State != before.State || cur.StartedAt != nil {
		t.Fatalf("input mutated: %+v", cur)
	}
}
