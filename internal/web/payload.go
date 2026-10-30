package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// Payloads sent by taskiq.middlewares.taskiq_admin_middleware.TaskiqAdminMiddleware.
//
// queued/started: {"args", "kwargs", "labels", "queuedAt"|"startedAt", "taskName", "worker"}
// executed:       {"finishedAt", "executionTime", "error", "returnValue": {"return_value": ...}}
//
// Parsing is deliberately lenient: unknown fields are ignored, a missing
// timestamp defaults to the reception time, and values of an unexpected JSON
// type are kept rather than rejected. Losing a report is worse than storing
// an odd value: for the same reason, args, kwargs or a return value larger
// than maxValueBytes is replaced by a small marker instead of failing the
// event (taskiq tasks may pass megabytes of ML vectors around).
type lifecyclePayload struct {
	TaskName  json.RawMessage `json:"taskName"`
	Args      json.RawMessage `json:"args"`
	Kwargs    json.RawMessage `json:"kwargs"`
	Labels    json.RawMessage `json:"labels"`
	Worker    json.RawMessage `json:"worker"`
	QueuedAt  json.RawMessage `json:"queuedAt"`
	StartedAt json.RawMessage `json:"startedAt"`
}

type executedPayload struct {
	FinishedAt    json.RawMessage `json:"finishedAt"`
	ExecutionTime json.RawMessage `json:"executionTime"`
	Error         json.RawMessage `json:"error"`
	ReturnValue   json.RawMessage `json:"returnValue"`
}

const maxTaskIDLength = 512

func decodeEvent(kind task.Kind, taskID string, body io.Reader, now time.Time, maxValueBytes int) (task.Event, error) {
	if taskID == "" || len(taskID) > maxTaskIDLength {
		return task.Event{}, fmt.Errorf("task id must be 1 to %d bytes long", maxTaskIDLength)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return task.Event{}, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte("{}")
	}
	e := task.Event{Kind: kind, TaskID: taskID}

	switch kind {
	case task.KindQueued, task.KindStarted:
		var p lifecyclePayload
		if err := json.Unmarshal(data, &p); err != nil {
			return e, fmt.Errorf("invalid JSON body: %w", err)
		}
		ts := p.QueuedAt
		if kind == task.KindStarted {
			ts = p.StartedAt
		}
		if e.At, err = jsonTime(ts, now); err != nil {
			return e, err
		}
		if s := jsonString(p.TaskName); s != nil {
			e.Name = *s
		}
		e.Args = capJSON(normalizeJSON(p.Args), maxValueBytes)
		e.Kwargs = capJSON(normalizeJSON(p.Kwargs), maxValueBytes)
		e.Labels = normalizeJSON(p.Labels)
		e.Worker = jsonString(p.Worker)

	case task.KindExecuted:
		var p executedPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return e, fmt.Errorf("invalid JSON body: %w", err)
		}
		if e.At, err = jsonTime(p.FinishedAt, now); err != nil {
			return e, err
		}
		if e.ExecutionTime, err = jsonFloat(p.ExecutionTime); err != nil {
			return e, fmt.Errorf("executionTime: %w", err)
		}
		// The original admin treated an empty error as success; keep that.
		if s := jsonString(p.Error); s != nil && *s != "" {
			e.Error = s
		}
		e.ReturnValue = capReturnValue(normalizeJSON(p.ReturnValue), maxValueBytes)
	}
	return e, nil
}

func isNull(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

// normalizeJSON re-encodes a JSON value compactly, with non-ASCII characters
// as UTF-8 instead of \u escapes (Python's json.dumps escapes them by
// default), so stored values are readable and searchable. nil for null.
func normalizeJSON(raw json.RawMessage) json.RawMessage {
	if isNull(raw) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep big integers exact
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return raw
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// How much of the beginning and the end of an oversized value its marker keeps.
const (
	previewHeadBytes = 200
	previewTailBytes = 200
)

// capJSON replaces a value longer than limit bytes (0: no limit) with a
// marker recording its size, its beginning and its end.
func capJSON(raw json.RawMessage, limit int) json.RawMessage {
	if limit <= 0 || len(raw) <= limit {
		return raw
	}
	cut := min(previewHeadBytes, len(raw))
	for cut > 0 && cut < len(raw) && !utf8.RuneStart(raw[cut]) {
		cut--
	}
	tail := max(len(raw)-previewTailBytes, cut)
	for tail < len(raw) && !utf8.RuneStart(raw[tail]) {
		tail++
	}
	marker, _ := json.Marshal(struct {
		Truncated bool   `json:"truncated"`
		Bytes     int    `json:"bytes"`
		Preview   string `json:"preview"`
	}{true, len(raw), string(raw[:cut]) + "…" + string(raw[tail:])})
	return marker
}

// capReturnValue caps the result inside the middleware's {"return_value": x}
// wrapper, so the stored shape stays the same.
func capReturnValue(raw json.RawMessage, limit int) json.RawMessage {
	if limit <= 0 || len(raw) <= limit {
		return raw
	}
	var wrapper map[string]json.RawMessage
	if json.Unmarshal(raw, &wrapper) != nil || len(wrapper) != 1 || wrapper["return_value"] == nil {
		return capJSON(raw, limit)
	}
	out, _ := json.Marshal(map[string]json.RawMessage{"return_value": capJSON(wrapper["return_value"], limit)})
	return out
}

// jsonString returns a JSON string's value, the raw JSON text of any other
// non-null value, or nil for null/absent.
func jsonString(raw json.RawMessage) *string {
	if isNull(raw) {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return &s
	}
	s = string(normalizeJSON(raw))
	return &s
}

func jsonFloat(raw json.RawMessage) (*float64, error) {
	if isNull(raw) {
		return nil, nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err != nil {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return nil, errors.New("must be a number")
		}
		if f, err = strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
			return nil, errors.New("must be a number")
		}
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errors.New("must be finite")
	}
	return &f, nil
}

// jsonTime parses an ISO-8601 string (naive values are UTC, which is what the
// middleware sends) or a unix timestamp in seconds or milliseconds. Absent or
// null means now. The result is truncated to the storage precision (ms).
func jsonTime(raw json.RawMessage, now time.Time) (time.Time, error) {
	if isNull(raw) {
		return now.UTC().Truncate(time.Millisecond), nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		t, ok := parseTime(s, time.UTC)
		if !ok {
			return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
		}
		return t.time.UTC().Truncate(time.Millisecond), nil
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		if math.Abs(f) < 1e11 { // seconds
			f *= 1000
		}
		return time.UnixMilli(int64(f)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %s", raw)
}

type parsedTime struct {
	time time.Time
	// precision is the span covered by the written value (a day for
	// "2026-01-19", a minute for "2026-01-19T10:30"), used for inclusive ends.
	precision time.Duration
}

var timeLayouts = []struct {
	layout    string
	precision time.Duration
}{
	{time.RFC3339Nano, 0},
	{"2006-01-02T15:04:05.999999999", 0},
	{"2006-01-02 15:04:05.999999999Z07:00", 0},
	{"2006-01-02 15:04:05.999999999", 0},
	{"2006-01-02T15:04Z07:00", time.Minute},
	{"2006-01-02T15:04", time.Minute},
	{"2006-01-02 15:04", time.Minute},
	{"2006-01-02", 24 * time.Hour},
}

// parseTime parses the supported timestamp formats; values without a zone
// are interpreted in loc.
func parseTime(s string, loc *time.Location) (parsedTime, bool) {
	s = strings.TrimSpace(s)
	for _, l := range timeLayouts {
		if t, err := time.ParseInLocation(l.layout, s, loc); err == nil {
			p := l.precision
			if p == 0 {
				p = time.Millisecond
			}
			return parsedTime{time: t, precision: p}, true
		}
	}
	return parsedTime{}, false
}
