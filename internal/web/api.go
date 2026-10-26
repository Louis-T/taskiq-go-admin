package web

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
)

func (s *Server) apiListTasks(w http.ResponseWriter, r *http.Request) {
	q, errs := parseTaskQuery(r.URL.Query(), time.UTC, "limit", 15)
	if len(errs) > 0 {
		writeError(w, http.StatusBadRequest, errors.Join(errs...).Error())
		return
	}
	stale, err := s.staleFilter(r)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	res, err := s.store.ListTasks(r.Context(), store.ListQuery{Filter: q.filter(stale), Limit: q.Limit, Cursor: q.Cursor, Direction: q.Direction})
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) apiGetTask(w http.ResponseWriter, r *http.Request) {
	t, err := s.store.GetTask(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

type histogramJSON struct {
	Bucket string           `json:"bucket"`
	From   time.Time        `json:"from"`
	To     time.Time        `json:"to"`
	Rows   []histogramEntry `json:"rows"`
}

type histogramEntry struct {
	Start time.Time `json:"start"`
	State string    `json:"state"`
	Count int64     `json:"count"`
}

// apiHistogram: same filters as /api/tasks, plus "range" (1h, 6h, 24h, 7d,
// 30d) when no dates are given, and an optional "bucket" duration.
func (s *Server) apiHistogram(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	q, errs := parseTaskQuery(v, time.UTC, "limit", 15)
	if len(errs) > 0 {
		writeError(w, http.StatusBadRequest, errors.Join(errs...).Error())
		return
	}
	win := chartWindow(q, v.Get("range"), s.now(), time.UTC)
	if b := v.Get("bucket"); b != "" {
		d, err := time.ParseDuration(b)
		if err != nil || d < time.Second || win.To.Sub(win.From)/d > maxBuckets {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("bucket: must be a duration >= 1s giving at most %d buckets", maxBuckets))
			return
		}
		win.Bucket = d
		win.From = alignDown(win.From, d, win.Offset)
	}
	stale, err := s.staleFilter(r)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	rows, err := s.histogram(r, q.filter(stale), win)
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	out := histogramJSON{Bucket: win.Bucket.String(), From: win.From, To: win.To, Rows: []histogramEntry{}}
	for _, row := range rows {
		out.Rows = append(out.Rows, histogramEntry{Start: row.Start, State: string(row.State), Count: row.Count})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiBackup(w http.ResponseWriter, r *http.Request) {
	// A big database takes a while to snapshot: lift the write deadline.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	name := s.now().UTC().Format("2006-01-02 15-04-05") + "-backup.db"
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	bw := &lazyWriter{w: w}
	if err := s.store.Backup(r.Context(), bw); err != nil {
		if errors.Is(err, store.ErrUnsupported) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if !bw.started {
			s.serverError(w, r, err)
			return
		}
		s.log.Error("backup interrupted", "err", err)
	}
}

// lazyWriter lets an error response be sent if nothing was written yet.
type lazyWriter struct {
	w       http.ResponseWriter
	started bool
}

func (l *lazyWriter) Write(p []byte) (int, error) {
	l.started = true
	return l.w.Write(p)
}

func (s *Server) apiGetSettings(w http.ResponseWriter, r *http.Request) {
	set, err := s.store.GetSettings(r.Context())
	if err != nil {
		s.serverError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, set)
}

func (s *Server) apiPutSettings(w http.ResponseWriter, r *http.Request) {
	var set store.Settings
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&set); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := set.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.SaveSettings(r.Context(), set); err != nil {
		s.serverError(w, r, err)
		return
	}
	s.purger.Trigger()
	writeJSON(w, http.StatusOK, set)
}

func (s *Server) serverError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return
	}
	s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}
