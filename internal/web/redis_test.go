package web

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/redisadmin"
	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

func mustApply(t *testing.T, st store.Store, events ...task.Event) {
	t.Helper()
	if err := st.ApplyEvents(context.Background(), events); err != nil {
		t.Fatalf("ApplyEvents: %v", err)
	}
}

type fakeReader struct {
	mu        sync.Mutex
	overview  redisadmin.Overview
	err       error
	stream    redisadmin.StreamDetail
	streamErr error
	gotKey    string
}

func (f *fakeReader) Overview(context.Context) (redisadmin.Overview, error) {
	return f.overview, f.err
}

func (f *fakeReader) Stream(_ context.Context, key string) (redisadmin.StreamDetail, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gotKey = key
	return f.stream, f.streamErr
}

type fakeCleaner struct {
	mu        sync.Mutex
	status    redisadmin.CleanStatus
	triggers  int
	triggered bool // value returned by Trigger
	bumpSeq   bool // Trigger finishes a run before returning
}

func (f *fakeCleaner) Trigger(context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.triggers++
	if f.bumpSeq {
		f.status.Seq++
	}
	return f.triggered
}

func (f *fakeCleaner) Status(context.Context) redisadmin.CleanStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func minutes(n int) *int { return &n }

func okOverview() redisadmin.Overview {
	oldest := time.Now().Add(-22 * time.Minute)
	return redisadmin.Overview{
		Server: redisadmin.ServerInfo{Version: redisadmin.Version{Major: 8, Minor: 2, Patch: 1}, Supported: true, Mode: "sentinel", MasterName: "mymaster", DB: 3, UsedMemory: 412 << 20, Clients: 38, Uptime: 12 * 24 * time.Hour},
		Queues: []redisadmin.Queue{
			{Key: "taskiq:high", Type: "stream", Length: 8902, Groups: 1, Pending: 640, OldestPending: &oldest, MemoryBytes: 3 << 20},
			{Key: "emails", Type: "list", Length: 53, MemoryBytes: 40 << 10},
		},
	}
}

func newRedisEnv(t *testing.T, rd *fakeReader, rc *fakeCleaner) *env {
	t.Helper()
	return newEnvWithRedis(t, rd, rc)
}

func (e *env) postForm(t *testing.T, path string, form url.Values, headers ...string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.srv.URL+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp.StatusCode, sb.String()
}

func TestRedisDisabled(t *testing.T) {
	e := newEnv(t)
	if code, _ := e.get(t, "/redis"); code != http.StatusNotFound {
		t.Fatalf("/redis: %d", code)
	}
	_, tasks := e.get(t, "/tasks")
	if strings.Contains(tasks, `href="/redis"`) {
		t.Fatal("nav links to a disabled Redis section")
	}
	_, settings := e.get(t, "/settings")
	if strings.Contains(settings, "redis_trim_interval_minutes") {
		t.Fatal("settings show Redis fields while disabled")
	}
}

func TestRedisOverview(t *testing.T) {
	rd := &fakeReader{overview: okOverview()}
	rc := &fakeCleaner{status: redisadmin.CleanStatus{KeepAcked: minutes(60), Interval: minutes(5)}}
	e := newRedisEnv(t, rd, rc)
	if err := e.store.SaveSettings(context.Background(), store.Settings{StaleQueuedMinutes: minutes(5)}); err != nil {
		t.Fatal(err)
	}
	code, body := e.get(t, "/redis")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"taskiq:high", `href="/redis/streams/taskiq:high"`, "emails", "list", "8.2.1", "db 3", `href="/redis"`} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	if !regexp.MustCompile(`class="[^"]*\bwarn\b[^"]*"[^>]*>\s*640\b`).MatchString(body) {
		t.Errorf("640 is not flagged warn:\n%s", body)
	}
	if strings.Contains(body, `href="/redis/streams/emails"`) {
		t.Error("a list must not link to a stream page")
	}
	// Alone for the htmx refresh.
	_, part := e.get(t, "/redis", "HX-Request", "true", "HX-Target", "redis-body")
	if strings.Contains(part, "<html") || !strings.Contains(part, "taskiq:high") {
		t.Errorf("refresh not a fragment: %.200s", part)
	}
}

func TestRedisPageShowsReaderError(t *testing.T) {
	e := newRedisEnv(t, &fakeReader{err: errors.New("dial tcp: refused")}, &fakeCleaner{})
	code, body := e.get(t, "/redis")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"alert-error", "refused", "every 5s [document.visibilityState==='visible']"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestRedisUnsupported(t *testing.T) {
	ov := okOverview()
	ov.Server.Supported = false
	ov.Server.Version = redisadmin.Version{Major: 6, Minor: 2, Patch: 6}
	e := newRedisEnv(t, &fakeReader{overview: ov, err: redisadmin.ErrUnsupportedVersion}, &fakeCleaner{})
	_, body := e.get(t, "/redis")
	if !strings.Contains(body, "Redis 7.0 or later is required") || !strings.Contains(body, "6.2.6") {
		t.Errorf("banner missing:\n%s", body)
	}
	// Also when the reader reports it through Supported only.
	e = newRedisEnv(t, &fakeReader{overview: ov}, &fakeCleaner{})
	_, body = e.get(t, "/redis")
	if !strings.Contains(body, "Redis 7.0 or later is required") {
		t.Errorf("banner missing for Supported=false:\n%s", body)
	}
}

func TestRedisStreamKeyEscaping(t *testing.T) {
	const key = "a/b c%{x}"
	ov := okOverview()
	ov.Queues = []redisadmin.Queue{{Key: key, Type: "stream", Length: 1}}
	rd := &fakeReader{overview: ov, stream: redisadmin.StreamDetail{Key: key, Length: 1}}
	e := newRedisEnv(t, rd, &fakeCleaner{})
	_, body := e.get(t, "/redis")
	const link = `/redis/streams/a%2Fb%20c%25%7Bx%7D`
	if !strings.Contains(body, `href="`+link+`"`) {
		t.Fatalf("link not found:\n%s", body)
	}
	if code, _ := e.get(t, link); code != 200 {
		t.Fatalf("stream page: %d", code)
	}
	if rd.gotKey != key {
		t.Fatalf("reader got %q", rd.gotKey)
	}
}

func TestRedisStreamPage(t *testing.T) {
	first, last := time.Now().Add(-3*time.Hour), time.Now().Add(-2*time.Second)
	rd := &fakeReader{stream: redisadmin.StreamDetail{
		Key: "taskiq:high", Length: 8902, FirstEntry: &first, LastEntry: &last, Trimmable: 8100,
		Groups: []redisadmin.Group{{Name: "taskiq", LastDelivered: redisadmin.ID{Ms: 1793202850123}, Pending: 640,
			Consumers: []redisadmin.Consumer{{Name: "3f2a", Pending: 600, Idle: 22 * time.Minute}}}},
		OldestPending: []redisadmin.PendingMessage{{ID: redisadmin.ID{Ms: 1793201530000}, TaskID: "t-1", TaskName: "app:send_report", Consumer: "3f2a", Idle: 22 * time.Minute, Deliveries: 3}},
	}}
	e := newRedisEnv(t, rd, &fakeCleaner{})
	mustApply(t, e.store, task.Event{Kind: task.KindQueued, TaskID: "t-1", At: first, Name: "app:send_report"})
	code, body := e.get(t, "/redis/streams/taskiq:high")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{"8902", "8100", "taskiq", "1793202850123-0", "3f2a", `href="/tasks/t-1"`, "send_report", "22 min", "redis-stream-body"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(body, "never trimmed") || strings.Contains(body, "at least") {
		t.Errorf("a stream with a group on Redis < 8.2 shows an exact count:\n%s", body)
	}
}

// Only the task ids the admin has stored link to a task page.
func TestRedisStreamLinksKnownTasksOnly(t *testing.T) {
	rd := &fakeReader{stream: redisadmin.StreamDetail{
		Key: "q", Length: 2, Groups: []redisadmin.Group{{Name: "g", Pending: 2}},
		OldestPending: []redisadmin.PendingMessage{
			{ID: redisadmin.ID{Ms: 1, Seq: 1}, TaskID: "known", TaskName: "app:known_task"},
			{ID: redisadmin.ID{Ms: 1, Seq: 2}, TaskID: "unknown", TaskName: "app:other_task"},
		},
	}}
	e := newRedisEnv(t, rd, &fakeCleaner{})
	mustApply(t, e.store, task.Event{Kind: task.KindQueued, TaskID: "known", At: time.Now(), Name: "app:known_task"})
	_, body := e.get(t, "/redis/streams/q")
	if !strings.Contains(body, `href="/tasks/known"`) {
		t.Errorf("no link to the known task:\n%s", body)
	}
	if strings.Contains(body, `href="/tasks/unknown"`) || !strings.Contains(body, "other_task") {
		t.Errorf("the unknown task must show as plain text:\n%s", body)
	}
}

func TestRedisStreamWithoutGroupNeverTrimmed(t *testing.T) {
	rd := &fakeReader{stream: redisadmin.StreamDetail{Key: "q", Length: 5}}
	e := newRedisEnv(t, rd, &fakeCleaner{})
	_, body := e.get(t, "/redis/streams/q")
	if !regexp.MustCompile(`Trimmable now</span><span class="stat-value">never trimmed`).MatchString(body) {
		t.Errorf("no \"never trimmed\" for a stream without a group:\n%s", body)
	}
}

// On Redis 8.2+ XTRIM ACKED also removes acknowledged entries above a pending
// one, so the count is a lower bound.
func TestRedisStreamTrimmableAtLeast(t *testing.T) {
	rd := &fakeReader{stream: redisadmin.StreamDetail{Key: "q", Length: 5, Trimmable: 3, TrimmableAtLeast: true,
		Groups: []redisadmin.Group{{Name: "g"}}}}
	e := newRedisEnv(t, rd, &fakeCleaner{})
	_, body := e.get(t, "/redis/streams/q")
	if !strings.Contains(body, `<span class="stat-value">at least 3</span>`) {
		t.Errorf("no \"at least 3\":\n%s", body)
	}
}

func TestRedisStreamNotFound(t *testing.T) {
	e := newRedisEnv(t, &fakeReader{streamErr: redisadmin.ErrNotStream}, &fakeCleaner{})
	if code, _ := e.get(t, "/redis/streams/nope"); code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
}

func TestRedisCleanNow(t *testing.T) {
	rc := &fakeCleaner{triggered: true, status: redisadmin.CleanStatus{KeepAcked: minutes(60)}}
	e := newRedisEnv(t, &fakeReader{overview: okOverview()}, rc)
	code, body := e.postForm(t, "/redis/cleanup", nil, "HX-Request", "true", "HX-Target", "redis-cleanup")
	if code != 200 || rc.triggers != 1 || !strings.Contains(body, "Running…") {
		t.Fatalf("code %d triggers %d body %s", code, rc.triggers, body)
	}
	code, _ = e.postForm(t, "/redis/cleanup", nil, "Origin", "https://evil.example", "Sec-Fetch-Site", "cross-site")
	if code != http.StatusForbidden || rc.triggers != 1 {
		t.Fatalf("cross-site: code %d triggers %d", code, rc.triggers)
	}
	// Without JavaScript: redirect back to the page.
	code, _ = e.postForm(t, "/redis/cleanup", nil)
	if code != http.StatusSeeOther || rc.triggers != 2 {
		t.Fatalf("no-js: code %d triggers %d", code, rc.triggers)
	}
}

func TestRedisCleanupDefaultKeepAge(t *testing.T) {
	e := newRedisEnv(t, &fakeReader{overview: okOverview()}, &fakeCleaner{})
	_, body := e.get(t, "/redis")
	if !strings.Contains(body, "keep acknowledged entries <b>0 minutes</b> (default)") {
		t.Fatalf("no default keep-age:\n%s", body)
	}
	if strings.Contains(body, "Set a keep-acked age") {
		t.Fatalf("Clean now still disabled:\n%s", body)
	}
}

func TestRedisCleanupToast(t *testing.T) {
	rc := &fakeCleaner{status: redisadmin.CleanStatus{Seq: 2, Last: &redisadmin.RunSummary{Started: time.Now(), Removed: 42}}}
	e := newEnvWithRedis(t, &fakeReader{}, rc)
	get := func(path string) http.Header {
		resp, err := http.Get(e.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.Header
	}
	if h := get("/redis/cleanup?seen=1&running=1").Get("HX-Trigger"); !strings.Contains(h, "Cleanup removed 42 entries") {
		t.Fatalf("HX-Trigger %q", h)
	}
	if h := get("/redis/cleanup?seen=2&running=1").Get("HX-Trigger"); h != "" {
		t.Fatalf("unexpected HX-Trigger %q", h)
	}
	if h := get("/redis/cleanup?seen=1").Get("HX-Trigger"); h != "" {
		t.Fatalf("toast without running=1: %q", h)
	}
	rc.mu.Lock()
	rc.status.Last = &redisadmin.RunSummary{Err: "boom"}
	rc.mu.Unlock()
	if h := get("/redis/cleanup?seen=1&running=1").Get("HX-Trigger"); !strings.Contains(h, "Cleanup failed: boom") {
		t.Fatalf("HX-Trigger %q", h)
	}
}

func TestRedisSettingsSave(t *testing.T) {
	e := newRedisEnv(t, &fakeReader{}, &fakeCleaner{})
	code, _ := e.postForm(t, "/settings", url.Values{"redis_keep_acked_minutes": {"0"}, "redis_trim_interval_minutes": {"5"}}, "HX-Request", "true", "HX-Target", "settings-card")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	set, _ := e.store.GetSettings(context.Background())
	if set.RedisKeepAckedMinutes == nil || *set.RedisKeepAckedMinutes != 0 || set.RedisTrimIntervalMinutes == nil || *set.RedisTrimIntervalMinutes != 5 {
		t.Fatalf("stored %+v", set)
	}
	_, body := e.postForm(t, "/settings", url.Values{"redis_keep_acked_minutes": {"-1"}}, "HX-Request", "true", "HX-Target", "settings-card")
	if !strings.Contains(body, "Keep acknowledged entries") {
		t.Fatalf("no form error:\n%s", body)
	}
	_, body = e.postForm(t, "/settings", url.Values{"redis_trim_interval_minutes": {"0"}}, "HX-Request", "true", "HX-Target", "settings-card")
	if !strings.Contains(body, "Trim every") {
		t.Fatalf("no form error:\n%s", body)
	}
	_, body = e.get(t, "/settings")
	for _, want := range []string{"redis_trim_interval_minutes", "Saving does not trigger a run"} {
		if !strings.Contains(body, want) {
			t.Errorf("settings page missing %q", want)
		}
	}
}

func TestRedisDisabledSettingsSaveKeepsRedisValues(t *testing.T) {
	e := newEnv(t)
	if err := e.store.SaveSettings(context.Background(), store.Settings{RedisTrimIntervalMinutes: minutes(5), RedisKeepAckedMinutes: minutes(60)}); err != nil {
		t.Fatal(err)
	}
	code, _ := e.postForm(t, "/settings", url.Values{"delete_old_ttl_minutes": {"120"}}, "HX-Request", "true", "HX-Target", "settings-card")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	set, _ := e.store.GetSettings(context.Background())
	if set.DeleteOldTTLMinutes == nil || *set.DeleteOldTTLMinutes != 120 {
		t.Fatalf("ttl not saved: %+v", set)
	}
	if set.RedisTrimIntervalMinutes == nil || *set.RedisTrimIntervalMinutes != 5 || set.RedisKeepAckedMinutes == nil || *set.RedisKeepAckedMinutes != 60 {
		t.Fatalf("Redis settings wiped: %+v", set)
	}
}

func TestRedisCleanNowSeenIsReadBeforeTrigger(t *testing.T) {
	rc := &fakeCleaner{triggered: true, bumpSeq: true, status: redisadmin.CleanStatus{Seq: 1, KeepAcked: minutes(60)}}
	e := newRedisEnv(t, &fakeReader{overview: okOverview()}, rc)
	_, body := e.postForm(t, "/redis/cleanup", nil, "HX-Request", "true", "HX-Target", "redis-cleanup")
	if !strings.Contains(body, "seen=1&running=1") {
		t.Fatalf("seen must be the Seq from before Trigger:\n%s", body)
	}
}

func TestRedisCleanNowDisabledWhenRedisUnavailable(t *testing.T) {
	rc := &fakeCleaner{status: redisadmin.CleanStatus{KeepAcked: minutes(60)}}
	for name, rd := range map[string]*fakeReader{
		"error":       {err: errors.New("refused")},
		"unsupported": {overview: redisadmin.Overview{Server: redisadmin.ServerInfo{Version: redisadmin.Version{Major: 6}}}},
	} {
		e := newRedisEnv(t, rd, rc)
		_, body := e.get(t, "/redis")
		if !regexp.MustCompile(`<button[^>]*disabled`).MatchString(body) || !strings.Contains(body, "off=1") {
			t.Errorf("%s: Clean now not disabled:\n%s", name, body)
		}
		// The box's own polls keep the state.
		_, box := e.get(t, "/redis/cleanup?off=1")
		if !regexp.MustCompile(`<button[^>]*disabled`).MatchString(box) {
			t.Errorf("%s: polled box enabled", name)
		}
	}
	e := newRedisEnv(t, &fakeReader{overview: okOverview()}, rc)
	_, body := e.get(t, "/redis")
	if regexp.MustCompile(`<button[^>]*disabled`).MatchString(body) {
		t.Error("healthy Redis: button disabled")
	}
}
