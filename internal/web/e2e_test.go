//go:build e2e

package web

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
	"github.com/louis-t/taskiq-go-admin/internal/events"
	"github.com/louis-t/taskiq-go-admin/internal/ingest"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/redisadmin"
	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/store/cache"
	"github.com/louis-t/taskiq-go-admin/internal/store/sqlite"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// The worker and kicker services of e2e/docker-compose.yml report to
// http://host.docker.internal:3999 and use redis82 db 2.
const (
	workerAdminAddr = "0.0.0.0:3999" // not 127.0.0.1: the containers come through the docker bridge
	workerDB        = 2
	workerStream    = "taskiq"
	composeFile     = "../../e2e/docker-compose.yml"
)

func redis82Addr() string {
	if v := os.Getenv("E2E_REDIS82"); v != "" {
		return v
	}
	return "localhost:6382"
}

// startWorkerAdmin runs the admin like main does, with the Redis section on
// redis82 db 2 and a keep-age of 0, listening where the worker reports.
func startWorkerAdmin(t *testing.T) (base string, st store.Store, reader *redisadmin.Reader) {
	t.Helper()
	ctx := context.Background()
	st, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSettings(ctx, store.Settings{RedisKeepAckedMinutes: ptrInt(0)}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := events.NewHub()
	ing := ingest.New(st, 100, log)
	ing.OnCommit = hub.Publish
	go ing.Run()
	purger := maintenance.NewPurger(st, time.Minute, maintenance.BatchConfig{Size: 100}, log)

	cached := cache.New(st)
	client, err := redisadmin.Connect(config.Redis{URL: "redis://" + redis82Addr(), DB: workerDB})
	if err != nil {
		t.Fatal(err)
	}
	reader = redisadmin.NewReader(client, func(ctx context.Context) (time.Duration, bool) {
		s, err := cached.GetSettings(ctx)
		if err != nil || s.RedisKeepAckedMinutes == nil {
			return 0, false
		}
		return time.Duration(*s.RedisKeepAckedMinutes) * time.Minute, true
	})
	cleaner := redisadmin.NewCleaner(reader, cached, maintenance.BatchConfig{Size: 1000}, log)
	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() { cleaner.Run(runCtx); close(runDone) }()

	s, err := NewServer(Options{
		APIToken: "e2e", MaxBodyBytes: 4 << 20, MaxValueBytes: 64 << 10,
		PurgeInterval: time.Minute, PurgeBatchSize: 100, Version: "v-e2e",
		Redis: reader, RedisCleaner: cleaner,
	}, cached, ing, purger, hub, log)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", workerAdminAddr)
	if err != nil {
		t.Fatalf("listen %s: %v", workerAdminAddr, err)
	}
	hs := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go hs.Serve(ln)
	t.Cleanup(func() {
		hub.Close()
		shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		hs.Shutdown(shutCtx)
		stopRun()
		<-runDone
		ing.Close(ctx)
		client.Close()
		st.Close()
	})
	_, port, _ := net.SplitHostPort(workerAdminAddr)
	return "http://127.0.0.1:" + port, st, reader
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// eventually polls cond every 200 ms until it holds or timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, state := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s (last: %s)", timeout, what, state)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tasksIn(t *testing.T, st store.Store, state task.State) []task.Task {
	t.Helper()
	res, err := st.ListTasks(context.Background(), store.ListQuery{Filter: store.Filter{State: state}, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return res.Tasks
}

var removedRe = regexp.MustCompile(`Cleanup removed (\d+) entries`)

// TestRealWorkerEndToEnd checks the Redis section against a real taskiq
// worker (RedisStreamBroker + TaskiqAdminMiddleware): the kicker sends 20
// fast tasks then 2 that sleep 30 s; the fast ones end acknowledged, the
// slow ones stay pending while this test runs.
func TestRealWorkerEndToEnd(t *testing.T) {
	base, st, reader := startWorkerAdmin(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	kick := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "run", "--rm", "kicker")
	if out, err := kick.CombinedOutput(); err != nil {
		t.Fatalf("kicker: %v\n%s", err, out)
	}

	eventually(t, 60*time.Second, "20 ok tasks reported as successful", func() (bool, string) {
		n := 0
		for _, tk := range tasksIn(t, st, task.Success) {
			if strings.HasSuffix(tk.Name, ":ok") {
				n++
			}
		}
		return n == 20, fmt.Sprintf("%d successful ok tasks", n)
	})
	running := map[string]bool{}
	eventually(t, 20*time.Second, "2 slow tasks reported as running", func() (bool, string) {
		clear(running)
		for _, tk := range tasksIn(t, st, task.Running) {
			if strings.HasSuffix(tk.Name, ":slow") {
				running[tk.ID] = true
			}
		}
		return len(running) == 2, fmt.Sprintf("%d running slow tasks", len(running))
	})

	// The broker acks after the task's result is saved, which can come after
	// the "executed" report: wait until only the slow entries are pending.
	rc := redis.NewClient(&redis.Options{Addr: redis82Addr(), DB: workerDB})
	defer rc.Close()
	eventually(t, 10*time.Second, "only the 2 slow entries pending", func() (bool, string) {
		p, err := rc.XPending(ctx, workerStream, "taskiq").Result()
		if err != nil {
			return false, err.Error()
		}
		return p.Count == 2, fmt.Sprintf("%d pending", p.Count)
	})

	code, body := httpGet(t, base+"/redis")
	if code != http.StatusOK || !strings.Contains(body, `href="/redis/streams/taskiq"`) {
		t.Fatalf("GET /redis: %d, no link to the taskiq stream\n%s", code, body)
	}

	// The stream page lists the pending entries with a link to each task.
	code, body = httpGet(t, base+"/redis/streams/taskiq")
	if code != http.StatusOK {
		t.Fatalf("GET /redis/streams/taskiq: %d\n%s", code, body)
	}
	for id := range running {
		link := fmt.Sprintf(`<a href="/tasks/%s">`, id)
		if !strings.Contains(body, link) {
			t.Errorf("stream page has no pending entry linking %s", link)
		}
	}
	detail, err := reader.Stream(ctx, workerStream)
	if err != nil {
		t.Fatal(err)
	}
	var slowEntries []string
	for _, p := range detail.OldestPending {
		if strings.HasSuffix(p.TaskName, "slow") && running[p.TaskID] {
			slowEntries = append(slowEntries, p.ID.String())
		}
	}
	if len(slowEntries) != 2 {
		t.Errorf("pending entries %+v: %d slow tasks of this run; want 2", detail.OldestPending, len(slowEntries))
	}

	// Clean now, then poll the box like the page does until the run's toast.
	resp, err := http.Post(base+"/redis/cleanup", "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /redis/cleanup: %d", resp.StatusCode)
	}
	var removed int
	eventually(t, 20*time.Second, "the cleanup run to end", func() (bool, string) {
		resp, err := http.Get(base + "/redis/cleanup?running=1&seen=0")
		if err != nil {
			return false, err.Error()
		}
		resp.Body.Close()
		toast := resp.Header.Get("HX-Trigger")
		if strings.Contains(toast, "failed") {
			t.Fatalf("cleanup: %s", toast)
		}
		m := removedRe.FindStringSubmatch(toast)
		if m == nil {
			return false, "no toast yet"
		}
		removed, _ = strconv.Atoi(m[1])
		return true, ""
	})
	if removed < 20 {
		t.Errorf("cleanup removed %d entries; want at least 20", removed)
	}
	for _, id := range slowEntries {
		entries, err := rc.XRange(ctx, workerStream, id, id).Result()
		if err != nil || len(entries) != 1 {
			t.Errorf("after cleanup: XRANGE %s = %v, %v; the pending entry must stay", id, entries, err)
		}
	}
	p, err := rc.XPending(ctx, workerStream, "taskiq").Result()
	if err != nil || p.Count != 2 {
		t.Errorf("after cleanup: pending %+v, err %v; want the 2 slow entries", p, err)
	}
}
