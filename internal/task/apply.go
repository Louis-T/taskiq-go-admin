package task

import "time"

// Apply merges an event into the current task (nil when the task is unknown)
// and returns the resulting task. It never mutates cur.
//
// The middleware sends its three reports fire-and-forget, concurrently, and
// over the network, so they can arrive in any order, more than once, or not at
// all (taskiq-admin#16: "executed" arriving before "started" left tasks stuck
// in "running"). The rules below make the result independent of arrival order:
//
//   - Within one attempt, state only moves forward: queued < running <
//     success/failure. A late "queued" or "started" only fills in data that is
//     still missing (name, args, timestamps...), it never downgrades the state.
//   - "executed" always wins over "running", "queued" and "abandoned" (the
//     admin marks running tasks abandoned at startup, but the worker may well
//     have kept running). Between two results, the most recent finishedAt wins,
//     so a stale duplicate cannot overwrite a newer attempt's outcome.
//   - "executed" carries no attempt number: a result older than the start of
//     the current retry belongs to the previous attempt and is ignored.
//   - A new attempt (retry, detected through the "_retries" label) restarts the
//     lifecycle, unless the worker clock shows its result already arrived
//     (finishedAt >= the new event's timestamp).
func Apply(cur *Task, e Event) Task {
	if cur == nil {
		return newTask(e)
	}
	t := *cur
	switch e.Kind {
	case KindQueued:
		applyQueued(&t, e)
	case KindStarted:
		applyStarted(&t, e)
	case KindExecuted:
		applyExecuted(&t, e)
	}
	return t
}

func newTask(e Event) Task {
	t := Task{ID: e.TaskID, QueuedAt: e.At}
	switch e.Kind {
	case KindQueued:
		t.State = Queued
		t.Attempt = e.Attempt()
		fillMeta(&t, e, true)
	case KindStarted:
		// "queued" not received (yet): use startedAt as the best known queue
		// time, the real one replaces it if the report shows up later.
		t.State = Running
		t.Attempt = e.Attempt()
		t.StartedAt = timePtr(e.At)
		fillMeta(&t, e, true)
	case KindExecuted:
		// Nothing known but the result. The name stays empty until "queued"
		// or "started" arrives; queuedAt is estimated from the execution time.
		applyResult(&t, e)
		t.QueuedAt = *t.StartedAt
	}
	return t
}

func applyQueued(t *Task, e Event) {
	attempt := e.Attempt()
	switch {
	case attempt < t.Attempt:
		fillMeta(t, e, false)
	case attempt == t.Attempt:
		fillMeta(t, e, false)
		// The queued report is the authority on queue time (it may replace a
		// value estimated from "started" or "executed").
		t.QueuedAt = e.At
	default:
		fillMeta(t, e, true)
		t.Attempt = attempt
		t.QueuedAt = e.At
		if resultIsNewerOrSame(t, e.At) {
			return
		}
		t.State = Queued
		t.StartedAt = nil
		clearResult(t)
	}
}

func applyStarted(t *Task, e Event) {
	attempt := e.Attempt()
	switch {
	case attempt < t.Attempt:
		fillMeta(t, e, false)
	case attempt == t.Attempt:
		fillMeta(t, e, false)
		t.StartedAt = timePtr(e.At)
		if t.State == Queued {
			t.State = Running
		}
	default:
		fillMeta(t, e, true)
		t.Attempt = attempt
		t.StartedAt = timePtr(e.At)
		if resultIsNewerOrSame(t, e.At) {
			return
		}
		t.State = Running
		clearResult(t)
	}
}

func applyExecuted(t *Task, e Event) {
	if t.FinishedAt != nil && t.FinishedAt.After(e.At) {
		return // stale result of an earlier attempt
	}
	if t.Attempt > 0 && t.FinishedAt == nil && e.At.Before(attemptStart(t)) {
		return // result of the previous attempt, arriving after the retry started
	}
	applyResult(t, e)
}

// attemptStart is when the current attempt began, as far as we know.
func attemptStart(t *Task) time.Time {
	if t.StartedAt != nil {
		return *t.StartedAt
	}
	return t.QueuedAt
}

func applyResult(t *Task, e Event) {
	t.FinishedAt = timePtr(e.At)
	t.Error = e.Error
	t.ExecutionTime = e.ExecutionTime
	t.ReturnValue = e.ReturnValue
	if e.Error != nil {
		t.State = Failure
	} else {
		t.State = Success
	}
	if t.StartedAt == nil {
		// Estimate; the real startedAt replaces it if "started" arrives later.
		start := e.At
		if e.ExecutionTime != nil && *e.ExecutionTime > 0 {
			start = e.At.Add(-time.Duration(*e.ExecutionTime * float64(time.Second))).Truncate(time.Millisecond)
		}
		t.StartedAt = &start
	}
}

// resultIsNewerOrSame reports whether the stored result was produced at or
// after ts, meaning it belongs to the attempt that ts announces. Like
// attemptStart, it compares timestamps from the worker that ran the task and
// the one that re-kicked it (usually the same machine), so large clock skew
// between workers can still misattribute a result.
func resultIsNewerOrSame(t *Task, ts time.Time) bool {
	return t.State.Terminal() && t.FinishedAt != nil && !t.FinishedAt.Before(ts)
}

func clearResult(t *Task) {
	t.FinishedAt = nil
	t.Error = nil
	t.ExecutionTime = nil
	t.ReturnValue = nil
}

// fillMeta copies the descriptive fields of a queued/started event. With
// overwrite=false only missing fields are filled.
func fillMeta(t *Task, e Event, overwrite bool) {
	if e.Name != "" && (overwrite || t.Name == "") {
		t.Name = e.Name
	}
	if e.Args != nil && (overwrite || t.Args == nil) {
		t.Args = e.Args
	}
	if e.Kwargs != nil && (overwrite || t.Kwargs == nil) {
		t.Kwargs = e.Kwargs
	}
	if e.Labels != nil && (overwrite || t.Labels == nil) {
		t.Labels = e.Labels
	}
	if e.Worker != nil && (overwrite || t.Worker == nil) {
		t.Worker = e.Worker
	}
}

func timePtr(t time.Time) *time.Time { return &t }
