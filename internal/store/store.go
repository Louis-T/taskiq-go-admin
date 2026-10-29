// Package store defines the persistence contract of the admin panel.
//
// Every database backend (sqlite today; postgres, mysql/mariadb later) lives
// in its own sub-package and implements Store. Domain rules (how events merge
// into a task) stay in package task, so a backend is only about SQL. The
// storetest package holds a conformance suite each backend must pass.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/task"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrUnsupported = errors.New("not supported by this database driver")
)

// MaxListLimit caps the page size of ListTasks.
const MaxListLimit = 100

// Store is the persistence contract. Implementations must be safe for
// concurrent use.
type Store interface {
	// ApplyEvents merges events into their tasks (see task.Apply) atomically:
	// either every event is applied or none is.
	ApplyEvents(ctx context.Context, events []task.Event) error

	GetTask(ctx context.Context, id string) (*task.Task, error)
	// KnownTaskIDs returns which of ids are stored tasks (the map holds only
	// those, set to true).
	KnownTaskIDs(ctx context.Context, ids []string) (map[string]bool, error)
	ListTasks(ctx context.Context, q ListQuery) (*ListResult, error)
	// Histogram counts tasks per (queuedAt bucket, state) in [From, To).
	Histogram(ctx context.Context, q HistogramQuery) ([]HistogramRow, error)

	// DeleteQueuedBefore deletes at most limit tasks queued before cutoff and
	// returns how many were deleted. Callers loop to purge in small batches.
	DeleteQueuedBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	// AbandonRunning marks at most limit running tasks started before cutoff
	// as abandoned and returns how many were updated.
	AbandonRunning(ctx context.Context, cutoff time.Time, limit int) (int64, error)

	// Count returns how many tasks match the filter.
	Count(ctx context.Context, f Filter) (int64, error)
	// TaskTypes summarizes every named task type queued in [from, to),
	// most run first.
	TaskTypes(ctx context.Context, from, to time.Time) ([]TypeSummary, error)
	// TypeStats returns detailed statistics for one task name in [from, to).
	TypeStats(ctx context.Context, name string, from, to time.Time) (*TypeStats, error)

	GetSettings(ctx context.Context) (Settings, error)
	SaveSettings(ctx context.Context, s Settings) error

	// Backup writes a consistent snapshot of the database to w, or returns
	// ErrUnsupported.
	Backup(ctx context.Context, w io.Writer) error

	Close() error
}

// Filter is shared by the list and the histogram.
type Filter struct {
	State task.State // empty: any
	// Name is a case-insensitive substring of the task name.
	Name string
	// TaskName is an exact task name.
	TaskName string
	// ArgsSearch is a case-insensitive substring of args or kwargs (JSON text).
	ArgsSearch string
	// Stale, when set, keeps only tasks that look stuck.
	Stale *StaleFilter
	// From/To bound queuedAt (inclusive From, exclusive To). Zero: unbounded.
	From, To time.Time
}

// StaleFilter matches tasks running since before RunningStartedBefore, or
// queued since before QueuedBefore. A zero time disables that criterion.
type StaleFilter struct {
	RunningStartedBefore time.Time
	QueuedBefore         time.Time
}

// Enabled reports whether at least one criterion is set.
func (f StaleFilter) Enabled() bool {
	return !f.RunningStartedBefore.IsZero() || !f.QueuedBefore.IsZero()
}

type Direction string

const (
	Next Direction = "next"
	Prev Direction = "prev"
)

// Cursor points at a task in the (queuedAt DESC, id DESC) ordering.
type Cursor struct {
	QueuedAt time.Time `json:"queuedAt"`
	ID       string    `json:"id"`
}

func (c Cursor) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"queuedAt": c.QueuedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		"id":       c.ID,
	})
}

type ListQuery struct {
	Filter
	Limit     int
	Cursor    *Cursor
	Direction Direction
}

type ListResult struct {
	Tasks      []task.Task `json:"tasks"`
	NextCursor *Cursor     `json:"nextCursor"`
	PrevCursor *Cursor     `json:"prevCursor"`
}

type HistogramQuery struct {
	Filter
	Bucket time.Duration
	// Offset shifts bucket boundaries, so that buckets align on the viewer's
	// local time (e.g. daily buckets starting at local midnight).
	Offset time.Duration
}

type HistogramRow struct {
	Start time.Time
	State task.State
	Count int64
}

// TypeSummary aggregates the runs of one task name over a period.
type TypeSummary struct {
	Name         string
	Total        int64
	Counts       map[task.State]int64
	AvgExec      *float64 // seconds
	MaxExec      *float64 // seconds
	LastQueuedAt time.Time
}

// FailureRate is failures / finished runs, nil when nothing finished.
func (t TypeSummary) FailureRate() *float64 {
	finished := t.Counts[task.Success] + t.Counts[task.Failure]
	if finished == 0 {
		return nil
	}
	r := float64(t.Counts[task.Failure]) / float64(finished)
	return &r
}

// TypeStats adds percentiles to a TypeSummary. Nil values mean no data.
type TypeStats struct {
	TypeSummary
	P50Exec, P95Exec *float64       // seconds
	P50Wait, P95Wait *time.Duration // queued -> started
}

// Settings are the runtime-editable settings (stored in the database).
// A nil value disables the feature.
type Settings struct {
	// DeleteOldTTLMinutes enables the cleanup of tasks queued more than this
	// many minutes ago.
	DeleteOldTTLMinutes *int `json:"delete_old_ttl_minutes"`
	// StaleRunningMinutes flags tasks running for longer than this as stale.
	StaleRunningMinutes *int `json:"stale_running_minutes"`
	// StaleQueuedMinutes flags tasks queued for longer than this as stale.
	StaleQueuedMinutes *int `json:"stale_queued_minutes"`
	// RedisTrimIntervalMinutes enables the periodic trim of acknowledged
	// Redis stream entries, run at this interval.
	RedisTrimIntervalMinutes *int `json:"redis_trim_interval_minutes"`
	// RedisKeepAckedMinutes is how long acknowledged stream entries are kept
	// before being trimmed (0 trims them as soon as possible).
	RedisKeepAckedMinutes *int `json:"redis_keep_acked_minutes"`
}

const (
	TTLMinMinutes = 1
	TTLMaxMinutes = 60 * 24 * 365
)

// Validate checks the settings bounds.
func (s Settings) Validate() error {
	for _, f := range []struct {
		name string
		v    *int
		min  int
	}{
		{"delete_old_ttl_minutes", s.DeleteOldTTLMinutes, TTLMinMinutes},
		{"stale_running_minutes", s.StaleRunningMinutes, TTLMinMinutes},
		{"stale_queued_minutes", s.StaleQueuedMinutes, TTLMinMinutes},
		{"redis_trim_interval_minutes", s.RedisTrimIntervalMinutes, TTLMinMinutes},
		{"redis_keep_acked_minutes", s.RedisKeepAckedMinutes, 0},
	} {
		if f.v != nil && (*f.v < f.min || *f.v > TTLMaxMinutes) {
			return fmt.Errorf("%s must be between %d and %d", f.name, f.min, TTLMaxMinutes)
		}
	}
	return nil
}

// StaleFilter returns the stale criteria at time now, or nil when stale
// detection is disabled.
func (s Settings) StaleFilter(now time.Time) *StaleFilter {
	var f StaleFilter
	if s.StaleRunningMinutes != nil {
		f.RunningStartedBefore = now.Add(-time.Duration(*s.StaleRunningMinutes) * time.Minute)
	}
	if s.StaleQueuedMinutes != nil {
		f.QueuedBefore = now.Add(-time.Duration(*s.StaleQueuedMinutes) * time.Minute)
	}
	if !f.Enabled() {
		return nil
	}
	return &f
}

// IsStale reports whether t matches the criteria.
func (f *StaleFilter) IsStale(t *task.Task) bool {
	if f == nil {
		return false
	}
	switch t.State {
	case task.Running:
		return !f.RunningStartedBefore.IsZero() && t.StartedAt != nil && t.StartedAt.Before(f.RunningStartedBefore)
	case task.Queued:
		return !f.QueuedBefore.IsZero() && t.QueuedAt.Before(f.QueuedBefore)
	}
	return false
}

// Paginate trims a page fetched with limit+1 rows (in query order) and
// computes the cursors. Backends share it so they all paginate identically.
func Paginate(rows []task.Task, q ListQuery) *ListResult {
	hasMore := len(rows) > q.Limit
	if hasMore {
		rows = rows[:q.Limit]
	}
	isNext := q.Direction != Prev
	if !isNext {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}
	res := &ListResult{Tasks: rows}
	if res.Tasks == nil {
		res.Tasks = []task.Task{}
	}
	if len(rows) == 0 {
		return res
	}
	first, last := rows[0], rows[len(rows)-1]
	hasNext := (isNext && hasMore) || (!isNext && q.Cursor != nil)
	hasPrev := (isNext && q.Cursor != nil) || (!isNext && hasMore)
	if hasNext {
		res.NextCursor = &Cursor{QueuedAt: last.QueuedAt, ID: last.ID}
	}
	if hasPrev {
		res.PrevCursor = &Cursor{QueuedAt: first.QueuedAt, ID: first.ID}
	}
	return res
}
