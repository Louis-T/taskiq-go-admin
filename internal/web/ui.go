package web

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

var perPageOptions = []int{10, 15, 20, 50, 100}

const defaultPerPage = 15

type layoutData struct {
	Title   string
	Nav     string
	TZ      string
	Loc     *time.Location
	States  []task.State
	Version string
}

func (s *Server) layout(r *http.Request, title, nav string) layoutData {
	loc := location(r)
	return layoutData{Title: title, Nav: nav, TZ: loc.String(), Loc: loc, States: task.States, Version: s.opts.Version}
}

// staleFilter returns the current stale criteria, nil when disabled.
func (s *Server) staleFilter(r *http.Request) (*store.StaleFilter, error) {
	set, err := s.store.GetSettings(r.Context())
	if err != nil {
		return nil, err
	}
	return set.StaleFilter(s.now()), nil
}

// listParams is the UI state of a task list page, all of it in the URL.
type listParams struct {
	taskQuery
	PerPage int
	Range   string
	Live    bool
	// BasePath is "/tasks", or "/types/<name>" on a task type page.
	BasePath string
}

func parseListParams(v url.Values, loc *time.Location, basePath string) listParams {
	q, _ := parseTaskQuery(v, loc, "perPage", defaultPerPage) // the UI ignores invalid values
	p := listParams{taskQuery: q, PerPage: q.Limit, Range: presetByKey(v.Get("range")).Key, Live: true, BasePath: basePath}
	// The form sends live=0 then live=1 when the box is checked (hidden input
	// first), only live=0 when unchecked; absent means default (on).
	if vals := v["live"]; len(vals) > 0 {
		p.Live = vals[len(vals)-1] == "1"
	}
	return p
}

// StateParam is the value of the state select ("stale" included).
func (p listParams) StateParam() string {
	if p.StaleOnly {
		return "stale"
	}
	return string(p.State)
}

// values encodes the filters and display settings, without the cursor.
func (p listParams) values() url.Values {
	v := url.Values{}
	set := func(k, val string) {
		if val != "" {
			v.Set(k, val)
		}
	}
	set("state", p.StateParam())
	if p.BasePath == "/tasks" {
		set("taskName", p.TaskName)
	}
	set("search", p.Search)
	set("argsSearch", p.ArgsSearch)
	set("startDate", p.StartDate)
	set("endDate", p.EndDate)
	if p.PerPage != defaultPerPage {
		v.Set("perPage", strconv.Itoa(p.PerPage))
	}
	if p.Range != defaultRange {
		v.Set("range", p.Range)
	}
	if !p.Live {
		v.Set("live", "0")
	}
	return v
}

func (p listParams) HasFilters() bool {
	return p.State != "" || p.StaleOnly || p.Search != "" || p.ArgsSearch != "" || p.StartDate != "" || p.EndDate != ""
}

func (p listParams) url(v url.Values) string {
	if len(v) == 0 {
		return p.BasePath
	}
	return p.BasePath + "?" + v.Encode()
}

// URL returns the page URL with some parameters overridden ("" removes).
func (p listParams) URL(kv ...string) string {
	v := p.values()
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i+1] == "" {
			v.Del(kv[i])
		} else {
			v.Set(kv[i], kv[i+1])
		}
	}
	return p.url(v)
}

func typeURL(name string) string { return "/types/" + url.PathEscape(name) }

type tasksPage struct {
	layoutData
	P              listParams
	TypeName       string           // set on a task type page
	Type           *store.TypeStats // idem
	Result         *store.ListResult
	Chart          *chartView
	Stale          *store.StaleFilter // nil when stale detection is off
	StaleCount     int64
	SelfURL        string // current page incl. cursor, for live refresh
	NextURL        string
	PrevURL        string
	ClearURL       string
	PerPageOptions []int
}

func (s *Server) uiTasks(w http.ResponseWriter, r *http.Request) {
	s.renderList(w, r, "tasks", "Tasks", "/tasks", "")
}

func (s *Server) uiType(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	s.renderList(w, r, "type", shortName(name), typeURL(name), name)
}

// renderList serves the tasks page and the task type pages, which share the
// toolbar, chart and table.
func (s *Server) renderList(w http.ResponseWriter, r *http.Request, page, title, basePath, typeName string) {
	loc := location(r)
	p := parseListParams(r.URL.Query(), loc, basePath)
	if typeName != "" {
		p.TaskName = typeName
	}
	stale, err := s.staleFilter(r)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	filter := p.filter(stale)

	data := tasksPage{
		layoutData:     s.layout(r, title, "tasks"),
		P:              p,
		TypeName:       typeName,
		Stale:          stale,
		PerPageOptions: perPageOptions,
		ClearURL:       listParams{PerPage: p.PerPage, Range: p.Range, Live: p.Live, BasePath: basePath}.URL(),
	}
	if typeName != "" {
		data.Nav = "types"
	}

	self := p.values()
	if p.Cursor != nil {
		self.Set("cursorQueuedAt", p.Cursor.QueuedAt.Format(time.RFC3339Nano))
		self.Set("cursorId", p.Cursor.ID)
		self.Set("direction", string(p.Direction))
	}
	data.SelfURL = p.url(self)

	res, err := s.store.ListTasks(r.Context(), store.ListQuery{Filter: filter, Limit: p.PerPage, Cursor: p.Cursor, Direction: p.Direction})
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	if p.Direction == store.Prev && res.PrevCursor == nil {
		// Went back to the first page: drop the cursor so new tasks show up.
		res, err = s.store.ListTasks(r.Context(), store.ListQuery{Filter: filter, Limit: p.PerPage})
		if err != nil {
			s.uiError(w, r, err)
			return
		}
		data.SelfURL = p.url(p.values())
	}
	data.Result = res
	cursorURL := func(c *store.Cursor, dir string) string {
		if c == nil {
			return ""
		}
		v := p.values()
		v.Set("cursorQueuedAt", c.QueuedAt.Format(time.RFC3339Nano))
		v.Set("cursorId", c.ID)
		v.Set("direction", dir)
		return p.url(v)
	}
	data.NextURL = cursorURL(res.NextCursor, "next")
	data.PrevURL = cursorURL(res.PrevCursor, "prev")

	if stale != nil && !p.StaleOnly {
		f := filter
		f.State, f.Stale = "", stale
		if data.StaleCount, err = s.store.Count(r.Context(), f); err != nil {
			s.uiError(w, r, err)
			return
		}
	}

	win := chartWindow(p.taskQuery, p.Range, s.now(), loc)
	rows, err := s.histogram(r, filter, win)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	base := p.values()
	data.Chart = buildChart(rows, win, loc, func(from, to time.Time) string {
		return p.url(drillDownValues(base, from, to, loc))
	})
	if typeName != "" {
		// Stats cover the chart period, ignoring the other list filters.
		if data.Type, err = s.store.TypeStats(r.Context(), typeName, win.From, win.To); err != nil {
			s.uiError(w, r, err)
			return
		}
	}

	switch target := htmxTarget(r); target {
	case "results", "main":
		// Keep the address bar in sync (canonical URL, no history spam).
		w.Header().Set("HX-Replace-Url", data.SelfURL)
		block := "results"
		if target == "main" {
			block = "tasks-main"
		}
		s.render(w, r, page, block, data)
	default:
		s.render(w, r, page, "layout", data)
	}
}

type typesPage struct {
	layoutData
	Range   string
	Presets []rangePreset
	Period  string
	Types   []store.TypeSummary
	SelfURL string
}

func (s *Server) uiTypes(w http.ResponseWriter, r *http.Request) {
	loc := location(r)
	preset := presetByKey(r.URL.Query().Get("range"))
	now := s.now()
	from := now.Add(-preset.Span)
	types, err := s.store.TaskTypes(r.Context(), from, now)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	data := typesPage{
		layoutData: s.layout(r, "Task types", "types"),
		Range:      preset.Key,
		Presets:    rangePresets,
		Period:     from.In(loc).Format("Jan 2 15:04") + " – " + now.In(loc).Format("Jan 2 15:04"),
		Types:      types,
		SelfURL:    "/types",
	}
	if preset.Key != defaultRange {
		data.SelfURL += "?range=" + preset.Key
	}
	if htmxTarget(r) == "types-results" {
		w.Header().Set("HX-Replace-Url", data.SelfURL)
		s.render(w, r, "types", "types-results", data)
		return
	}
	s.render(w, r, "types", "layout", data)
}

type taskPage struct {
	layoutData
	ID    string
	Task  *task.Task
	Stale bool
}

func (s *Server) uiTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	data := taskPage{layoutData: s.layout(r, "Task "+id, "tasks"), ID: id}
	t, err := s.store.GetTask(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// Rendered live: the first report may not have arrived yet.
		w.Header().Set("Cache-Control", "no-store")
	case err != nil:
		s.uiError(w, r, err)
		return
	default:
		data.Task = t
		stale, err := s.staleFilter(r)
		if err != nil {
			s.uiError(w, r, err)
			return
		}
		data.Stale = stale.IsStale(t)
	}
	if htmxTarget(r) == "task-detail" {
		s.render(w, r, "task", "task-detail", data)
		return
	}
	s.render(w, r, "task", "layout", data)
}

type settingsPage struct {
	layoutData
	TTL, StaleRunning, StaleQueued string
	Min, Max                       int
	PurgeInterval                  string
	PurgeBatchSize                 int
	Message                        string
	Error                          string
}

func optionalInt(v *int) string {
	if v == nil {
		return ""
	}
	return strconv.Itoa(*v)
}

func (s *Server) settingsData(r *http.Request) (settingsPage, error) {
	set, err := s.store.GetSettings(r.Context())
	return settingsPage{
		layoutData:     s.layout(r, "Settings", "settings"),
		TTL:            optionalInt(set.DeleteOldTTLMinutes),
		StaleRunning:   optionalInt(set.StaleRunningMinutes),
		StaleQueued:    optionalInt(set.StaleQueuedMinutes),
		Min:            store.TTLMinMinutes,
		Max:            store.TTLMaxMinutes,
		PurgeInterval:  humanDuration(s.opts.PurgeInterval),
		PurgeBatchSize: s.opts.PurgeBatchSize,
	}, err
}

func (s *Server) uiSettings(w http.ResponseWriter, r *http.Request) {
	data, err := s.settingsData(r)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	s.render(w, r, "settings", "layout", data)
}

func (s *Server) uiSaveSettings(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	var (
		set     store.Settings
		formErr string
	)
	for _, f := range []struct {
		key, label string
		dst        **int
	}{
		{"delete_old_ttl_minutes", "Retention", &set.DeleteOldTTLMinutes},
		{"stale_running_minutes", "Running tasks threshold", &set.StaleRunningMinutes},
		{"stale_queued_minutes", "Queued tasks threshold", &set.StaleQueuedMinutes},
	} {
		raw := strings.TrimSpace(r.PostForm.Get(f.key))
		if raw == "" {
			continue
		}
		n, err := strconv.Atoi(raw)
		if err != nil || n < store.TTLMinMinutes || n > store.TTLMaxMinutes {
			formErr = f.label + " must be a whole number of minutes between " +
				strconv.Itoa(store.TTLMinMinutes) + " and " + strconv.Itoa(store.TTLMaxMinutes) + "."
			break
		}
		*f.dst = &n
	}
	if formErr == "" {
		if err := s.store.SaveSettings(r.Context(), set); err != nil {
			s.uiError(w, r, err)
			return
		}
		s.purger.Trigger()
		s.hub.Publish() // stale badges may change
	}
	data, err := s.settingsData(r)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	if formErr != "" {
		data.Error = formErr
		data.TTL = r.PostForm.Get("delete_old_ttl_minutes")
		data.StaleRunning = r.PostForm.Get("stale_running_minutes")
		data.StaleQueued = r.PostForm.Get("stale_queued_minutes")
	} else {
		data.Message = "Settings saved."
	}
	s.renderSettingsCard(w, r, data)
}

func (s *Server) uiPurgeNow(w http.ResponseWriter, r *http.Request) {
	data, err := s.settingsData(r)
	if err != nil {
		s.uiError(w, r, err)
		return
	}
	if data.TTL == "" {
		data.Error = "Set a retention first: cleanup is disabled."
	} else {
		s.purger.Trigger()
		data.Message = "Cleanup started in the background."
	}
	s.renderSettingsCard(w, r, data)
}

func (s *Server) renderSettingsCard(w http.ResponseWriter, r *http.Request, data settingsPage) {
	if htmxTarget(r) == "" {
		// No JavaScript: plain form post, redirect back.
		http.Redirect(w, r, "/settings", http.StatusSeeOther)
		return
	}
	s.render(w, r, "settings", "settings-card", data)
}

func htmxTarget(r *http.Request) string {
	if r.Header.Get("HX-Request") != "true" || r.Header.Get("HX-History-Restore-Request") == "true" {
		return ""
	}
	return r.Header.Get("HX-Target")
}

func (s *Server) render(w http.ResponseWriter, r *http.Request, page, block string, data any) {
	var buf bytes.Buffer
	if err := s.pages.execute(&buf, page, block, data); err != nil {
		s.uiError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Vary", "HX-Request, HX-Target")
	_, _ = buf.WriteTo(w)
}

func (s *Server) uiError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(r.Context().Err(), context.Canceled) {
		return
	}
	s.log.Error("page failed", "path", r.URL.Path, "err", err)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}
