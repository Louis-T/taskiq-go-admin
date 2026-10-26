// Package web serves the ingestion API used by the taskiq middleware, a JSON
// read API, and the htmx user interface.
package web

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/events"
	"github.com/louis-t/taskiq-go-admin/internal/ingest"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

//go:embed static
var staticFS embed.FS

// Options configures the server.
type Options struct {
	APIToken     string
	MaxBodyBytes int64
	// PurgeInfo is shown on the settings page.
	PurgeInterval  time.Duration
	PurgeBatchSize int
	// Version is shown in the footer and /healthz.
	Version string
}

type Server struct {
	opts     Options
	store    store.Store
	ingester *ingest.Ingester
	purger   *maintenance.Purger
	hub      *events.Hub
	log      *slog.Logger
	pages    *pages
	now      func() time.Time
}

func NewServer(opts Options, s store.Store, ing *ingest.Ingester, purger *maintenance.Purger, hub *events.Hub, log *slog.Logger) (*Server, error) {
	p, err := parsePages()
	if err != nil {
		return nil, err
	}
	return &Server{opts: opts, store: s, ingester: ing, purger: purger, hub: hub, log: log, pages: p, now: time.Now}, nil
}

// Handler returns the HTTP handler with every route.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Ingestion (taskiq middleware), token protected.
	mux.HandleFunc("POST /api/tasks/{id}/queued", s.handleEvent(task.KindQueued))
	mux.HandleFunc("POST /api/tasks/{id}/started", s.handleEvent(task.KindStarted))
	mux.HandleFunc("POST /api/tasks/{id}/executed", s.handleEvent(task.KindExecuted))

	// JSON read API.
	mux.HandleFunc("GET /api/tasks", s.apiListTasks)
	mux.HandleFunc("GET /api/tasks/histogram", s.apiHistogram)
	mux.HandleFunc("GET /api/tasks/backup", s.apiBackup)
	mux.HandleFunc("GET /api/tasks/{id}", s.apiGetTask)
	mux.HandleFunc("GET /api/settings", s.apiGetSettings)
	mux.HandleFunc("PUT /api/settings", s.apiPutSettings)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.opts.Version})
	})
	mux.HandleFunc("GET /events", s.events)

	// UI.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/tasks", http.StatusFound)
	})
	mux.HandleFunc("GET /tasks", s.uiTasks)
	mux.HandleFunc("GET /tasks/{id}", s.uiTask)
	mux.HandleFunc("GET /types", s.uiTypes)
	mux.HandleFunc("GET /types/{name}", s.uiType)
	mux.HandleFunc("GET /settings", s.uiSettings)
	mux.HandleFunc("POST /settings", s.uiSaveSettings)
	mux.HandleFunc("POST /settings/purge", s.uiPurgeNow)

	static, _ := fs.Sub(staticFS, "static")
	fileServer := http.StripPrefix("/static/", http.FileServerFS(static))
	mux.Handle("GET /static/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("v") != "" {
			// Fingerprinted URL: the content never changes for this URL.
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		fileServer.ServeHTTP(w, r)
	}))

	// The UI has no authentication, so reject cross-site state-changing
	// requests (CSRF): another site could otherwise post settings such as a
	// 1-minute TTL. Non-browser clients (the middleware, curl) send neither
	// Sec-Fetch-Site nor Origin and are not affected.
	csrf := http.NewCrossOriginProtection()
	return s.recoverer(s.logRequests(csrf.Handler(mux)))
}

func (s *Server) authorized(r *http.Request) bool {
	token := r.Header.Get("access-token")
	return token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.opts.APIToken)) == 1
}

func (s *Server) handleEvent(kind task.Kind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "Invalid access token")
			return
		}
		body := http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes)
		ev, err := decodeEvent(kind, r.PathValue("id"), body, s.now())
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			s.log.Warn("rejected task event", "task_id", r.PathValue("id"), "kind", kind.String(), "err", err)
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.ingester.Submit(r.Context(), ev); err != nil {
			if errors.Is(err, ingest.ErrClosed) {
				writeError(w, http.StatusServiceUnavailable, "shutting down")
				return
			}
			if r.Context().Err() != nil {
				return // client went away; the event is still applied
			}
			writeError(w, http.StatusInternalServerError, "could not store event")
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"success": true})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": true, "statusCode": status, "message": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/static/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		level := slog.LevelDebug
		if rec.status >= 400 {
			level = slog.LevelWarn
		}
		s.log.Log(r.Context(), level, "request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "took", time.Since(start).Round(time.Microsecond).String())
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.log.Error("panic", "path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
