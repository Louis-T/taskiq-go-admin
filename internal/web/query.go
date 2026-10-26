package web

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// taskQuery is the parsed form of the list/histogram query string, shared by
// the JSON API and the UI so both accept exactly the same parameters.
type taskQuery struct {
	State task.State
	// StaleOnly is the "stale" pseudo-state: tasks that look stuck according
	// to the stale settings.
	StaleOnly  bool
	TaskName   string // exact name
	Search     string
	ArgsSearch string
	// StartDate/EndDate are kept as written so the UI can echo them back.
	StartDate, EndDate string
	From, To           time.Time

	Limit     int
	Cursor    *store.Cursor
	Direction store.Direction
}

// filter builds the store filter. stale holds the current stale criteria
// (nil when disabled); "state=stale" then matches nothing.
func (q taskQuery) filter(stale *store.StaleFilter) store.Filter {
	f := store.Filter{State: q.State, TaskName: q.TaskName, Name: q.Search, ArgsSearch: q.ArgsSearch, From: q.From, To: q.To}
	if q.StaleOnly {
		f.Stale = &store.StaleFilter{}
		if stale != nil {
			f.Stale = stale
		}
	}
	return f
}

// parseTaskQuery parses the query string. Dates without a zone are read in
// loc; an end date is inclusive of what it names (endDate=2026-01-21 includes
// the whole day, taskiq-admin#14). Invalid values are reported in errs and
// left at their zero value, so the UI can ignore them while the API rejects.
func parseTaskQuery(v url.Values, loc *time.Location, limitKey string, defaultLimit int) (q taskQuery, errs []error) {
	if s := strings.TrimSpace(v.Get("state")); s == "stale" {
		q.StaleOnly = true
	} else if s != "" {
		if st, ok := task.ParseState(s); ok {
			q.State = st
		} else {
			errs = append(errs, fmt.Errorf("state: unknown state %q", s))
		}
	}
	q.TaskName = v.Get("taskName")
	q.Search = strings.TrimSpace(v.Get("search"))
	q.ArgsSearch = strings.TrimSpace(v.Get("argsSearch"))

	if s := strings.TrimSpace(v.Get("startDate")); s != "" {
		if p, ok := parseTime(s, loc); ok {
			q.StartDate, q.From = s, p.time
		} else {
			errs = append(errs, fmt.Errorf("startDate: invalid date %q", s))
		}
	}
	if s := strings.TrimSpace(v.Get("endDate")); s != "" {
		if p, ok := parseTime(s, loc); ok {
			q.EndDate, q.To = s, inclusiveEnd(p)
		} else {
			errs = append(errs, fmt.Errorf("endDate: invalid date %q", s))
		}
	}

	q.Limit = defaultLimit
	if s := strings.TrimSpace(v.Get(limitKey)); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > store.MaxListLimit {
			errs = append(errs, fmt.Errorf("%s: must be between 1 and %d", limitKey, store.MaxListLimit))
		} else {
			q.Limit = n
		}
	}

	q.Direction = store.Next
	switch d := v.Get("direction"); d {
	case "", "next":
	case "prev":
		q.Direction = store.Prev
	default:
		errs = append(errs, fmt.Errorf("direction: must be next or prev"))
	}

	cursorAt, cursorID := strings.TrimSpace(v.Get("cursorQueuedAt")), v.Get("cursorId")
	switch {
	case cursorAt == "" && cursorID == "":
	case cursorAt == "" || cursorID == "":
		errs = append(errs, fmt.Errorf("cursorQueuedAt and cursorId must be provided together"))
	default:
		if p, ok := parseTime(cursorAt, time.UTC); ok {
			q.Cursor = &store.Cursor{QueuedAt: p.time, ID: cursorID}
		} else {
			errs = append(errs, fmt.Errorf("cursorQueuedAt: invalid date %q", cursorAt))
		}
	}
	if q.Cursor == nil {
		q.Direction = store.Next
	}
	return q, errs
}

func inclusiveEnd(p parsedTime) time.Time {
	if p.precision == 24*time.Hour {
		return p.time.AddDate(0, 0, 1) // DST-safe "end of that day"
	}
	return p.time.Add(p.precision)
}

// location returns the viewer's time zone (cookie set by app.js), UTC when
// unknown.
func location(r *http.Request) *time.Location {
	if c, err := r.Cookie("tz"); err == nil && c.Value != "" {
		if name, err := url.QueryUnescape(c.Value); err == nil {
			if loc, err := time.LoadLocation(name); err == nil {
				return loc
			}
		}
	}
	return time.UTC
}
