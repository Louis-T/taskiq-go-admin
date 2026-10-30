package web

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/louis-t/taskiq-go-admin/internal/events"
	"github.com/louis-t/taskiq-go-admin/internal/ingest"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/store/sqlite"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

const token = "supersecret"

type env struct {
	srv   *httptest.Server
	store store.Store
	hub   *events.Hub
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWithRedis(t, nil, nil)
}

func newEnvWithRedis(t *testing.T, rd RedisReader, rc RedisCleaner) *env {
	t.Helper()
	st, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := events.NewHub()
	ing := ingest.New(st, 100, log)
	ing.OnCommit = hub.Publish
	go ing.Run()
	purger := maintenance.NewPurger(st, time.Minute, maintenance.BatchConfig{Size: 100}, log)
	s, err := NewServer(Options{APIToken: token, MaxBodyBytes: 4 << 20, MaxValueBytes: 64 << 10, PurgeInterval: time.Minute, PurgeBatchSize: 100, Version: "v-test", Redis: rd, RedisCleaner: rc}, st, ing, purger, hub, log)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(s.Handler())
	t.Cleanup(func() {
		hub.Close()
		hs.Close()
		ing.Close(context.Background())
		st.Close()
	})
	return &env{srv: hs, store: st, hub: hub}
}

func (e *env) post(t *testing.T, path, body string, tok string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("access-token", tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func (e *env) get(t *testing.T, path string, headers ...string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// Bodies exactly as TaskiqAdminMiddleware builds them (naive UTC isoformat).
const (
	queuedBody   = `{"args":[1,"é"],"kwargs":{"user":"Zoé"},"labels":{"schedule_id":"abc"},"queuedAt":"2026-01-02T10:00:00.123456","taskName":"app.tasks:add_one","worker":"mybroker"}`
	startedBody  = `{"args":[1,"é"],"kwargs":{"user":"Zoé"},"labels":{"schedule_id":"abc"},"startedAt":"2026-01-02T10:00:01.000001","taskName":"app.tasks:add_one","worker":"mybroker"}`
	executedBody = `{"finishedAt":"2026-01-02T10:00:02.5","executionTime":1.4999,"error":null,"returnValue":{"return_value":2}}`
	failedBody   = `{"finishedAt":"2026-01-02T10:00:02.5","executionTime":0.1,"error":"ValueError('boom')","returnValue":{"return_value":null}}`
)

func TestIngestAuth(t *testing.T) {
	e := newEnv(t)
	if r := e.post(t, "/api/tasks/x/queued", queuedBody, ""); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", r.StatusCode)
	}
	if r := e.post(t, "/api/tasks/x/queued", queuedBody, "wrong"); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token: %d", r.StatusCode)
	}
	if r := e.post(t, "/api/tasks/x/queued", queuedBody, token); r.StatusCode != http.StatusOK {
		t.Fatalf("good token: %d", r.StatusCode)
	}
}

func TestIngestOutOfOrder(t *testing.T) {
	e := newEnv(t)
	// taskiq-admin#16: executed, then started, then queued.
	for _, step := range []struct{ path, body string }{
		{"/api/tasks/abc/executed", executedBody},
		{"/api/tasks/abc/started", startedBody},
		{"/api/tasks/abc/queued", queuedBody},
	} {
		if r := e.post(t, step.path, step.body, token); r.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", step.path, r.StatusCode)
		}
	}
	code, body := e.get(t, "/api/tasks/abc")
	if code != 200 {
		t.Fatalf("get: %d %s", code, body)
	}
	var got task.Task
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.State != task.Success || got.Name != "app.tasks:add_one" || *got.Worker != "mybroker" {
		t.Fatalf("got %+v", got)
	}
	if got.QueuedAt.Format(time.RFC3339Nano) != "2026-01-02T10:00:00.123Z" || !strings.Contains(body, `"queuedAt":"2026-01-02T10:00:00.123Z","startedAt":"2026-01-02T10:00:01.000Z"`) {
		t.Fatalf("queuedAt %v", got.QueuedAt)
	}
	if string(got.Kwargs) != `{"user":"Zoé"}` || string(got.Args) != `[1,"é"]` {
		t.Fatalf("json not normalized: %s %s", got.Args, got.Kwargs)
	}
	if string(got.ReturnValue) != `{"return_value":2}` || *got.ExecutionTime != 1.4999 {
		t.Fatalf("result %s %v", got.ReturnValue, got.ExecutionTime)
	}
}

func TestIngestConcurrentOrdering(t *testing.T) {
	e := newEnv(t)
	var wg sync.WaitGroup
	for i := range 50 {
		id := "c" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		for _, step := range []struct{ kind, body string }{{"executed", failedBody}, {"started", startedBody}, {"queued", queuedBody}} {
			wg.Go(func() {
				if r := e.post(t, "/api/tasks/"+id+"/"+step.kind, step.body, token); r.StatusCode != 200 {
					t.Errorf("%s %s: %d", id, step.kind, r.StatusCode)
				}
			})
		}
	}
	wg.Wait()
	res, err := e.store.ListTasks(context.Background(), store.ListQuery{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Tasks) != 50 {
		t.Fatalf("got %d tasks", len(res.Tasks))
	}
	for _, tk := range res.Tasks {
		if tk.State != task.Failure || tk.Name == "" {
			t.Fatalf("task %s: %s %q", tk.ID, tk.State, tk.Name)
		}
	}
}

func TestIngestRejectsGarbage(t *testing.T) {
	e := newEnv(t)
	cases := map[string]string{
		"/api/tasks/x/queued":   `not json`,
		"/api/tasks/y/started":  `{"startedAt":"yesterday"}`,
		"/api/tasks/z/executed": `{"executionTime":"fast"}`,
	}
	for path, body := range cases {
		if r := e.post(t, path, body, token); r.StatusCode != http.StatusBadRequest {
			t.Errorf("%s %s: %d", path, body, r.StatusCode)
		}
	}
	big := `{"args":["` + strings.Repeat("a", 5<<20) + `"]}`
	if r := e.post(t, "/api/tasks/big/queued", big, token); r.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("big body: %d", r.StatusCode)
	}
}

// A report too big to store whole (taskiq passes ML vectors through args and
// return values) must still move the task through its lifecycle.
func TestIngestOversizedValues(t *testing.T) {
	e := newEnv(t)
	vectors := `[` + strings.Repeat(`0.123456789,`, 200_000) + `0]`
	for _, step := range []struct{ kind, body string }{
		{"queued", `{"args":[` + vectors + `],"kwargs":{},"labels":{"queue_name":"q"},"queuedAt":"2026-01-02T10:00:00","taskName":"app:upsert","worker":"w"}`},
		{"started", `{"args":[` + vectors + `],"kwargs":{},"labels":{"queue_name":"q"},"startedAt":"2026-01-02T10:00:01","taskName":"app:upsert","worker":"w"}`},
		{"executed", `{"finishedAt":"2026-01-02T10:00:02","executionTime":1,"error":null,"returnValue":{"return_value":` + vectors + `}}`},
	} {
		if r := e.post(t, "/api/tasks/huge/"+step.kind, step.body, token); r.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", step.kind, r.StatusCode)
		}
	}
	got, err := e.store.GetTask(context.Background(), "huge")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != task.Success || got.Name != "app:upsert" || string(got.Labels) != `{"queue_name":"q"}` || string(got.Kwargs) != `{}` {
		t.Fatalf("got %+v", got)
	}
	if !strings.HasPrefix(string(got.Args), `{"truncated":true,"bytes":2400005,"preview":"[[0.123456789,`) || !strings.HasSuffix(string(got.Args), `0.123456789,0]]"}`) || len(got.Args) > 500 {
		t.Fatalf("oversized args not replaced: %.300s", got.Args)
	}
	if !strings.HasPrefix(string(got.ReturnValue), `{"return_value":{"truncated":true,"bytes":2400003,"preview":"[0.123456789,`) {
		t.Fatalf("oversized return value not replaced: %.300s", got.ReturnValue)
	}
}

func TestCapJSONPreviewKeepsBothEnds(t *testing.T) {
	preview := func(raw string) string {
		t.Helper()
		var m struct {
			Bytes   int    `json:"bytes"`
			Preview string `json:"preview"`
		}
		if err := json.Unmarshal(capJSON(json.RawMessage(raw), 10), &m); err != nil {
			t.Fatal(err)
		}
		if m.Bytes != len(raw) {
			t.Errorf("bytes = %d; want %d", m.Bytes, len(raw))
		}
		return m.Preview
	}

	raw := strings.Repeat("a", 1000) + strings.Repeat("b", 1000)
	if got, want := preview(raw), strings.Repeat("a", previewHeadBytes)+"…"+strings.Repeat("b", previewTailBytes); got != want {
		t.Errorf("preview = %q; want %q", got, want)
	}
	// Both cuts stay on a character boundary.
	raw = strings.Repeat("é", 1000)
	if got := preview(raw); !utf8.ValidString(got) || !strings.Contains(got, "é…é") {
		t.Errorf("preview = %q", got)
	}
	// A value shorter than both ends together is not repeated.
	raw = strings.Repeat("a", previewHeadBytes) + "bcd"
	if got, want := preview(raw), strings.Repeat("a", previewHeadBytes)+"…bcd"; got != want {
		t.Errorf("preview = %q; want %q", got, want)
	}
}

func TestDecodeEventLenient(t *testing.T) {
	now := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	e, err := decodeEvent(task.KindQueued, "id", strings.NewReader(`{"taskName":"t","unknown":1,"worker":null}`), now, 0)
	if err != nil || !e.At.Equal(now) || e.Name != "t" || e.Worker != nil {
		t.Fatalf("%+v %v", e, err)
	}
	e, err = decodeEvent(task.KindExecuted, "id", strings.NewReader(`{"finishedAt":1767348000,"executionTime":"0.5","error":""}`), now, 0)
	if err != nil || e.At.Unix() != 1767348000 || *e.ExecutionTime != 0.5 || e.Error != nil {
		t.Fatalf("%+v %v", e, err)
	}
	e, _ = decodeEvent(task.KindExecuted, "id", strings.NewReader(`{"finishedAt":"2026-01-02T10:00:00+02:00","error":{"type":"X"}}`), now, 0)
	if e.At.Format(time.RFC3339) != "2026-01-02T08:00:00Z" || *e.Error != `{"type":"X"}` {
		t.Fatalf("%+v", e)
	}
	if _, err := decodeEvent(task.KindQueued, strings.Repeat("x", 600), strings.NewReader(`{}`), now, 0); err == nil {
		t.Fatal("long id accepted")
	}
	e, _ = decodeEvent(task.KindStarted, "id", strings.NewReader(``), now, 0)
	if !e.At.Equal(now) {
		t.Fatal("empty body")
	}
}

func TestAPIListAndDateParams(t *testing.T) {
	e := newEnv(t)
	e.post(t, "/api/tasks/a/queued", queuedBody, token)
	// taskiq-admin#14: plain dates in the query string; endDate is inclusive.
	code, body := e.get(t, "/api/tasks?limit=10&startDate=2026-01-02&endDate=2026-01-02")
	if code != 200 || !strings.Contains(body, `"id":"a"`) {
		t.Fatalf("%d %s", code, body)
	}
	code, body = e.get(t, "/api/tasks?limit=10&endDate=2026-01-01")
	if code != 200 || !strings.Contains(body, `"tasks":[]`) {
		t.Fatalf("%d %s", code, body)
	}
	for _, bad := range []string{"limit=0", "limit=1000", "state=nope", "startDate=soon", "cursorId=x", "direction=up"} {
		if code, _ := e.get(t, "/api/tasks?"+bad); code != 400 {
			t.Errorf("%s: %d", bad, code)
		}
	}
	if code, _ := e.get(t, "/api/tasks/missing"); code != 404 {
		t.Errorf("missing task: %d", code)
	}
}

func TestAPIHistogram(t *testing.T) {
	e := newEnv(t)
	e.post(t, "/api/tasks/a/queued", queuedBody, token)
	code, body := e.get(t, "/api/tasks/histogram?startDate=2026-01-02T09:00&endDate=2026-01-02T10:59")
	if code != 200 || !strings.Contains(body, `"state":"queued","count":1`) {
		t.Fatalf("%d %s", code, body)
	}
	if code, _ := e.get(t, "/api/tasks/histogram?bucket=1s&range=30d"); code != 400 {
		t.Fatalf("too many buckets: %d", code)
	}
}

func TestAPISettings(t *testing.T) {
	e := newEnv(t)
	req, _ := http.NewRequest(http.MethodPut, e.srv.URL+"/api/settings", strings.NewReader(`{"delete_old_ttl_minutes":30,"redis_trim_interval_minutes":5,"redis_keep_acked_minutes":0}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("%v %v", resp, err)
	}
	resp.Body.Close()
	_, body := e.get(t, "/api/settings")
	if strings.TrimSpace(body) != `{"delete_old_ttl_minutes":30,"stale_running_minutes":null,"stale_queued_minutes":null,"redis_trim_interval_minutes":5,"redis_keep_acked_minutes":0}` {
		t.Fatalf("%s", body)
	}
	req, _ = http.NewRequest(http.MethodPut, e.srv.URL+"/api/settings", strings.NewReader(`{"delete_old_ttl_minutes":0}`))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("invalid ttl: %d", resp.StatusCode)
	}
}

func TestBackup(t *testing.T) {
	e := newEnv(t)
	e.post(t, "/api/tasks/a/queued", queuedBody, token)
	code, body := e.get(t, "/api/tasks/backup")
	if code != 200 || !strings.HasPrefix(body, "SQLite format 3") {
		t.Fatalf("%d %.20q", code, body)
	}
}

func TestUIPages(t *testing.T) {
	e := newEnv(t)
	e.post(t, "/api/tasks/ok/queued", queuedBody, token)
	e.post(t, "/api/tasks/ok/executed", executedBody, token)
	e.post(t, "/api/tasks/bad/started", startedBody, token)
	e.post(t, "/api/tasks/bad/executed", failedBody, token)
	e.post(t, "/api/tasks/run/started", startedBody, token)
	e.post(t, "/api/tasks/"+url.PathEscape("weird id/<x>")+"/executed", executedBody, token)

	range7d := "/tasks?startDate=2026-01-01&endDate=2026-01-03"
	code, body := e.get(t, range7d)
	if code != 200 {
		t.Fatalf("tasks page %d: %s", code, body)
	}
	for _, want := range []string{"<!doctype html>", `id="results"`, "Tasks over time", "add_one", "ValueError(&#39;boom&#39;)", "unknown", `class="bar-slot"`, "s-failure", "weird%20id%2F%3Cx%3E"} {
		if !strings.Contains(body, want) {
			t.Errorf("tasks page misses %q", want)
		}
	}
	if strings.Contains(body, "<x>") {
		t.Error("unescaped task id in page")
	}

	code, body = e.get(t, range7d, "HX-Request", "true", "HX-Target", "results")
	if code != 200 || strings.Contains(body, "<!doctype html>") || !strings.HasPrefix(strings.TrimSpace(body), `<div id="results"`) {
		t.Fatalf("results partial %d: %.200s", code, body)
	}
	code, body = e.get(t, "/tasks?state=failure&search=add&argsSearch=zo%C3%A9&live=0&perPage=10", "HX-Request", "true", "HX-Target", "main")
	if code != 200 || !strings.Contains(body, `id="filters"`) || strings.Contains(body, "tasks-changed") || !strings.Contains(body, "bad") {
		t.Fatalf("main partial %d: %.300s", code, body)
	}
	// invalid params are ignored by the UI rather than failing (taskiq-admin#7/#14)
	if code, _ := e.get(t, "/tasks?state=nope&perPage=abc&startDate=garbage&cursorId=x"); code != 200 {
		t.Fatalf("tolerant params: %d", code)
	}

	code, body = e.get(t, "/tasks/bad")
	if code != 200 || !strings.Contains(body, "ValueError") || strings.Contains(body, "tasks-changed") {
		t.Fatalf("detail %d", code)
	}
	code, body = e.get(t, "/tasks/run")
	if code != 200 || !strings.Contains(body, "tasks-changed") || !strings.Contains(body, "tl-open") {
		t.Fatalf("running detail should be live with an open timeline: %d", code)
	}
	code, body = e.get(t, "/tasks/nope")
	if code != 200 || !strings.Contains(body, "Task not found") || !strings.Contains(body, "tasks-changed") {
		t.Fatalf("missing detail %d", code)
	}

	code, body = e.get(t, "/settings")
	if code != 200 || !strings.Contains(body, "Retention") {
		t.Fatalf("settings %d", code)
	}
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/settings", strings.NewReader("delete_old_ttl_minutes=99999999"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	req.Header.Set("HX-Target", "settings-card")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "alert-error") {
		t.Fatalf("expected validation error: %s", b)
	}

	if code, _ := e.get(t, "/static/htmx.min.js"); code != 200 {
		t.Fatalf("static %d", code)
	}
}

func TestCSRFProtection(t *testing.T) {
	e := newEnv(t)
	post := func(path, body string, headers ...string) int {
		req, _ := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		for i := 0; i+1 < len(headers); i += 2 {
			req.Header.Set(headers[i], headers[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("/settings", "delete_old_ttl_minutes=1", "Sec-Fetch-Site", "cross-site"); code != http.StatusForbidden {
		t.Fatalf("cross-site settings post: %d", code)
	}
	if code := post("/settings/purge", "", "Origin", "https://evil.example"); code != http.StatusForbidden {
		t.Fatalf("cross-origin purge: %d", code)
	}
	set, _ := e.store.GetSettings(context.Background())
	if set.DeleteOldTTLMinutes != nil {
		t.Fatal("cross-site request changed the settings")
	}
	post("/settings", "delete_old_ttl_minutes=60", "Sec-Fetch-Site", "same-origin")
	if set, _ := e.store.GetSettings(context.Background()); set.DeleteOldTTLMinutes == nil || *set.DeleteOldTTLMinutes != 60 {
		t.Fatal("same-origin post was not applied")
	}
	// The middleware sends no browser headers and must keep working.
	if r := e.post(t, "/api/tasks/x/queued", queuedBody, token); r.StatusCode != 200 {
		t.Fatalf("ingest: %d", r.StatusCode)
	}
}

func TestSSEPushesChanges(t *testing.T) {
	e := newEnv(t)
	resp, err := http.Get(e.srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	lines := make(chan string, 10)
	go func() {
		buf := make([]byte, 256)
		for {
			n, err := resp.Body.Read(buf)
			if err != nil {
				close(lines)
				return
			}
			lines <- string(buf[:n])
		}
	}()
	if first := <-lines; !strings.HasPrefix(first, "retry:") {
		t.Fatalf("first chunk %q", first)
	}
	e.post(t, "/api/tasks/sse/queued", queuedBody, token)
	select {
	case msg := <-lines:
		if !strings.Contains(msg, "event: changed") {
			t.Fatalf("got %q", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no change event after ingest")
	}
	e.hub.Close() // shutdown ends the stream
	select {
	case _, ok := <-lines:
		for ok {
			_, ok = <-lines
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream not closed on shutdown")
	}
}

func TestStaleTasks(t *testing.T) {
	e := newEnv(t)
	e.post(t, "/api/tasks/stuck/started", startedBody, token) // started in 2026-01-02: long ago
	e.post(t, "/api/tasks/fresh/started", strings.Replace(startedBody, "2026-01-02T10:00:01.000001", time.Now().UTC().Format("2006-01-02T15:04:05"), 1), token)
	range7d := "/tasks?startDate=2026-01-01"

	_, body := e.get(t, range7d)
	if strings.Contains(body, "badge-stale") || strings.Contains(body, "look stuck") {
		t.Fatal("stale shown while disabled")
	}
	ctx := context.Background()
	if err := e.store.SaveSettings(ctx, store.Settings{StaleRunningMinutes: ptrInt(30)}); err != nil {
		t.Fatal(err)
	}
	_, body = e.get(t, range7d)
	if !strings.Contains(body, "<b>1</b> task looks stuck") || strings.Count(body, `class="badge badge-stale"`) != 1 || !strings.Contains(body, `value="stale"`) {
		t.Fatalf("stale banner/badge missing")
	}
	_, body = e.get(t, range7d+"&state=stale")
	if !strings.Contains(body, "/tasks/stuck") || strings.Contains(body, "/tasks/fresh") {
		t.Fatal("stale filter wrong")
	}
	code, body := e.get(t, "/api/tasks?limit=10&state=stale")
	if code != 200 || !strings.Contains(body, `"id":"stuck"`) || strings.Contains(body, `"id":"fresh"`) {
		t.Fatalf("api stale filter: %d %s", code, body)
	}
	_, body = e.get(t, "/tasks/stuck")
	if !strings.Contains(body, "badge-stale") {
		t.Fatal("detail page misses stale badge")
	}
}

func TestTypePages(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	for i, id := range []string{"t1", "t2", "t3"} {
		q := now.Add(-time.Duration(i+1) * time.Minute).Format("2006-01-02T15:04:05.000")
		f := now.Add(-time.Duration(i+1)*time.Minute + 2*time.Second).Format("2006-01-02T15:04:05.000")
		e.post(t, "/api/tasks/"+id+"/queued", `{"taskName":"app.tasks:add_one","queuedAt":"`+q+`","args":[],"kwargs":{}}`, token)
		e.post(t, "/api/tasks/"+id+"/started", `{"taskName":"app.tasks:add_one","startedAt":"`+q+`","args":[],"kwargs":{}}`, token)
		errField := "null"
		if i == 0 {
			errField = `"boom"`
		}
		e.post(t, "/api/tasks/"+id+"/executed", `{"finishedAt":"`+f+`","executionTime":2,"error":`+errField+`}`, token)
	}
	e.post(t, "/api/tasks/other/queued", `{"taskName":"app.tasks:other","args":[],"kwargs":{}}`, token)

	code, body := e.get(t, "/types")
	if code != 200 || !strings.Contains(body, "/types/app.tasks:add_one") || !strings.Contains(body, "33.3%") || !strings.Contains(body, "app.tasks:other") {
		t.Fatalf("types index %d", code)
	}
	code, body = e.get(t, "/types/app.tasks:add_one")
	if code != 200 || !strings.Contains(body, "Runtime p95") || !strings.Contains(body, "2.00 s") || strings.Contains(body, "/tasks/other") || !strings.Contains(body, "/tasks/t1") {
		t.Fatalf("type page %d", code)
	}
	if strings.Contains(body, `name="search"`) {
		t.Fatal("type page must not show the name search")
	}
	// partial refresh keeps the type filter
	_, body = e.get(t, "/types/app.tasks:add_one?state=failure", "HX-Request", "true", "HX-Target", "results")
	if !strings.Contains(body, "/tasks/t1") || strings.Contains(body, "/tasks/t2") {
		t.Fatal("type page state filter")
	}
	// list rows link to type pages and to task details
	_, body = e.get(t, "/tasks")
	if !strings.Contains(body, `href="/types/app.tasks:add_one"`) || !strings.Contains(body, `data-href="/tasks/t1"`) {
		t.Fatal("tasks list links")
	}
	if !strings.Contains(body, "taskiq-go-admin v-test") {
		t.Fatal("version footer")
	}
	// a static URL is fingerprinted and cached for good
	if !strings.Contains(body, "/static/app.css?v="+assetVersion) {
		t.Fatal("fingerprinted asset URL")
	}
	resp, err := http.Get(e.srv.URL + "/static/app.css?v=" + assetVersion)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		t.Fatalf("cache-control %q", cc)
	}
	// clicking the active legend entry clears the state filter
	_, body = e.get(t, "/tasks?state=failure")
	if !strings.Contains(body, `aria-pressed="true"`) || !strings.Contains(body, `title="Show all states"`) {
		t.Fatal("legend toggle")
	}
}

func ptrInt(v int) *int { return &v }

func TestListValue(t *testing.T) {
	marker := json.RawMessage(`{"truncated":true,"bytes":2400005,"preview":"[[0.1, 0.2…"}`)
	for _, tc := range []struct {
		raw         json.RawMessage
		value, note string
	}{
		{nil, "", ""},
		{json.RawMessage(`[1,2]`), `[1,2]`, ""},
		{marker, `[[0.1, 0.2…`, " (truncated, 2.3 MB in total)"},
		// A real value that only looks like a marker stays as it is.
		{json.RawMessage(`{"truncated":true,"bytes":3,"preview":"x","more":1}`), `{"truncated":true,"bytes":3,"preview":"x","more":1}`, ""},
		{json.RawMessage(`{"truncated":false,"bytes":3,"preview":"x"}`), `{"truncated":false,"bytes":3,"preview":"x"}`, ""},
	} {
		if got := listValue(tc.raw); got != tc.value {
			t.Errorf("listValue(%s) = %q; want %q", tc.raw, got, tc.value)
		}
		if got := truncatedNote(tc.raw); got != tc.note {
			t.Errorf("truncatedNote(%s) = %q; want %q", tc.raw, got, tc.note)
		}
	}
}

// The task list shows the preview of a value stored truncated, not the
// marker object.
func TestListShowsTruncatedPreview(t *testing.T) {
	e := newEnv(t)
	vectors := `[` + strings.Repeat(`0.123456789,`, 20_000) + `0]`
	now := time.Now().UTC()
	for _, step := range []struct{ kind, body string }{
		{"queued", `{"args":[` + vectors + `],"kwargs":{},"labels":{},"queuedAt":"` + now.Format("2006-01-02T15:04:05") + `","taskName":"app:upsert","worker":"w"}`},
		{"executed", `{"finishedAt":"` + now.Format("2006-01-02T15:04:05") + `","executionTime":1,"error":null,"returnValue":{"return_value":` + vectors + `}}`},
	} {
		if r := e.post(t, "/api/tasks/huge/"+step.kind, step.body, token); r.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", step.kind, r.StatusCode)
		}
	}
	_, body := e.get(t, "/tasks")
	if strings.Contains(body, "&#34;truncated&#34;") || strings.Contains(body, `"truncated"`) {
		t.Errorf("list shows the truncation marker:\n%s", body)
	}
	for _, want := range []string{`[[0.123456789,0.123456789…`, `[0.123456789,0.…`, "(truncated, 234.4 KB in total)"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}
