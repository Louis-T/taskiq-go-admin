// Package task holds the domain model of a taskiq task as seen by the admin
// panel, and the rules that merge lifecycle events into it.
package task

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// State is the lifecycle state of a task.
type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Success   State = "success"
	Failure   State = "failure"
	Abandoned State = "abandoned"
)

// States lists every state, in display order.
var States = []State{Queued, Running, Success, Failure, Abandoned}

// ParseState returns the state named s, or false when s is not a known state.
func ParseState(s string) (State, bool) {
	for _, st := range States {
		if string(st) == s {
			return st, true
		}
	}
	return "", false
}

// Terminal reports whether the task received its "executed" event.
func (s State) Terminal() bool { return s == Success || s == Failure }

// Settled reports whether the task will not change anymore unless a new
// event arrives (terminal, or abandoned by the admin panel).
func (s State) Settled() bool { return s.Terminal() || s == Abandoned }

// Task is the stored view of one task.
//
// JSON columns (Args, Kwargs, Labels, ReturnValue) are kept as raw JSON so
// that any payload the middleware sends round-trips untouched; nil means NULL.
type Task struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	State         State           `json:"state"`
	Error         *string         `json:"error"`
	Worker        *string         `json:"worker"`
	ExecutionTime *float64        `json:"executionTime"`
	QueuedAt      time.Time       `json:"queuedAt"`
	StartedAt     *time.Time      `json:"startedAt"`
	FinishedAt    *time.Time      `json:"finishedAt"`
	Args          json.RawMessage `json:"args"`
	Kwargs        json.RawMessage `json:"kwargs"`
	Labels        json.RawMessage `json:"labels"`
	ReturnValue   json.RawMessage `json:"returnValue"`
	// Attempt is the retry number of the attempt the task currently shows
	// (taken from the "_retries" label set by taskiq retry middlewares).
	Attempt int `json:"attempt"`
}

// Kind identifies which middleware hook produced an event.
type Kind int

const (
	KindQueued   Kind = iota // post_send, client side
	KindStarted              // pre_execute, worker side
	KindExecuted             // post_execute, worker side
)

func (k Kind) String() string {
	switch k {
	case KindQueued:
		return "queued"
	case KindStarted:
		return "started"
	case KindExecuted:
		return "executed"
	}
	return "unknown"
}

// Event is one lifecycle report received from the taskiq middleware.
type Event struct {
	Kind   Kind
	TaskID string
	// At is queuedAt, startedAt or finishedAt depending on Kind.
	At time.Time

	// queued / started
	Name   string
	Args   json.RawMessage
	Kwargs json.RawMessage
	Labels json.RawMessage
	Worker *string

	// executed
	Error         *string
	ExecutionTime *float64
	ReturnValue   json.RawMessage
}

// Attempt returns the retry number carried by the event's "_retries" label,
// 0 when absent. Label values may be numbers or numeric strings depending on
// the broker serializer.
func (e Event) Attempt() int {
	if len(e.Labels) == 0 {
		return 0
	}
	var labels map[string]json.RawMessage
	if err := json.Unmarshal(e.Labels, &labels); err != nil {
		return 0
	}
	raw, ok := labels["_retries"]
	if !ok {
		return 0
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err == nil {
		return max(int(n), 0)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
			return max(v, 0)
		}
	}
	return 0
}

// jsTime marshals like JavaScript's Date.toISOString (fixed milliseconds, UTC),
// the format of the original taskiq-admin API.
type jsTime time.Time

func (t jsTime) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(t).UTC().Format("2006-01-02T15:04:05.000Z") + `"`), nil
}

func jsTimePtr(t *time.Time) *jsTime {
	if t == nil {
		return nil
	}
	v := jsTime(*t)
	return &v
}

// MarshalJSON renders timestamps in the original API format.
func (t Task) MarshalJSON() ([]byte, error) {
	type plain Task
	return json.Marshal(struct {
		plain
		QueuedAt   jsTime  `json:"queuedAt"`
		StartedAt  *jsTime `json:"startedAt"`
		FinishedAt *jsTime `json:"finishedAt"`
	}{plain(t), jsTime(t.QueuedAt), jsTimePtr(t.StartedAt), jsTimePtr(t.FinishedAt)})
}
