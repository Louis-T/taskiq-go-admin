//go:build e2e

package redisadmin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
)

type readServer struct {
	name string
	addr string
	db   int
}

// readServers are the servers every read test runs against; each uses its own
// db so tests never see one another's keys.
func readServers() []readServer {
	return []readServer{{"redis70", addrRedis70, 11}, {"redis82", addrRedis82, 11}}
}

// seedQueues creates stream q1 (3 entries, group g read 2: first acked,
// second pending), list l1 (2 items) and string res:1. It returns the entry IDs.
func seedQueues(t *testing.T, ctx context.Context, rc *redis.Client) []string {
	t.Helper()
	msg := `{"task_id":"t-2","task_name":"app:work","labels":{},"args":[],"kwargs":{}}`
	var ids []string
	for i, data := range []string{`{"task_id":"t-1","task_name":"app:work"}`, msg, `{"task_id":"t-3","task_name":"app:work"}`} {
		id, err := rc.XAdd(ctx, &redis.XAddArgs{Stream: "q1", Values: map[string]any{"data": data}}).Result()
		if err != nil {
			t.Fatalf("XADD %d: %v", i, err)
		}
		ids = append(ids, id)
		time.Sleep(3 * time.Millisecond)
	}
	if err := rc.XGroupCreate(ctx, "q1", "g", "0").Err(); err != nil {
		t.Fatalf("XGROUP CREATE: %v", err)
	}
	res, err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c1", Streams: []string{"q1", ">"}, Count: 2}).Result()
	if err != nil || len(res[0].Messages) != 2 {
		t.Fatalf("XREADGROUP: %v %v", res, err)
	}
	if err := rc.XAck(ctx, "q1", "g", ids[0]).Err(); err != nil {
		t.Fatalf("XACK: %v", err)
	}
	if err := rc.RPush(ctx, "l1", "a", "b").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := rc.Set(ctx, "res:1", "x", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	return ids
}

func newTestReader(t *testing.T, s readServer) (*Reader, *redis.Client) {
	t.Helper()
	rc := rawDB(t, s.addr, s.db)
	c := connect(t, config.Redis{URL: "redis://" + s.addr, DB: s.db})
	r := NewReader(c, func(context.Context) (time.Duration, bool) { return 0, false })
	r.overviewTTL = 0 // each call reads Redis
	return r, rc
}

func TestOverviewFindsQueues(t *testing.T) {
	for _, s := range readServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			r, rc := newTestReader(t, s)
			ids := seedQueues(t, ctx, rc)

			ov, err := r.Overview(ctx)
			if err != nil {
				t.Fatalf("Overview: %v", err)
			}
			if !ov.Server.Supported {
				t.Errorf("Server.Supported = false")
			}
			if len(ov.Queues) != 2 || ov.Queues[0].Key != "l1" || ov.Queues[1].Key != "q1" {
				t.Fatalf("Queues = %+v; want exactly l1, q1", ov.Queues)
			}
			l, q := ov.Queues[0], ov.Queues[1]
			if l.Type != "list" || l.Length != 2 || l.Groups != 0 || l.Pending != 0 || l.OldestPending != nil || l.MemoryBytes <= 0 {
				t.Errorf("l1 = %+v", l)
			}
			if q.Type != "stream" || q.Length != 3 || q.Groups != 1 || q.Pending != 1 || q.MemoryBytes <= 0 {
				t.Errorf("q1 = %+v", q)
			}
			want, _ := ParseID(ids[1])
			if q.OldestPending == nil || !q.OldestPending.Equal(want.Time()) {
				t.Errorf("q1.OldestPending = %v; want %v", q.OldestPending, want.Time())
			}
		})
	}
}

func TestOverviewScanCache(t *testing.T) {
	for _, s := range readServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			r, rc := newTestReader(t, s)
			r.scanTTL = 50 * time.Millisecond
			seedQueues(t, ctx, rc)

			if ov, err := r.Overview(ctx); err != nil || len(ov.Queues) != 2 {
				t.Fatalf("first Overview = %+v, %v", ov.Queues, err)
			}
			if err := rc.XAdd(ctx, &redis.XAddArgs{Stream: "q2", Values: map[string]any{"foo": "bar"}}).Err(); err != nil {
				t.Fatal(err)
			}
			if ov, _ := r.Overview(ctx); len(ov.Queues) != 2 {
				t.Errorf("new stream visible before TTL: %+v", ov.Queues)
			}
			time.Sleep(80 * time.Millisecond)
			if ov, _ := r.Overview(ctx); len(ov.Queues) != 3 {
				t.Errorf("new stream not visible after TTL: %+v", ov.Queues)
			}
		})
	}
}

func TestStreamDetail(t *testing.T) {
	for _, s := range readServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			r, rc := newTestReader(t, s)
			ids := seedQueues(t, ctx, rc)

			d, err := r.Stream(ctx, "q1")
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			first, _ := ParseID(ids[0])
			last, _ := ParseID(ids[2])
			if d.Key != "q1" || d.Length != 3 || d.FirstEntry == nil || !d.FirstEntry.Equal(first.Time()) || d.LastEntry == nil || !d.LastEntry.Equal(last.Time()) {
				t.Errorf("detail = %+v", d)
			}
			if len(d.Groups) != 1 {
				t.Fatalf("Groups = %+v", d.Groups)
			}
			g := d.Groups[0]
			if g.Name != "g" || g.Pending != 1 || len(g.Consumers) != 1 || g.Consumers[0].Name != "c1" || g.Consumers[0].Pending != 1 {
				t.Errorf("group = %+v", g)
			}
			if g.LastDelivered.String() != ids[1] {
				t.Errorf("LastDelivered = %v; want %s", g.LastDelivered, ids[1])
			}
			if g.Lag == nil || *g.Lag != 1 {
				t.Errorf("Lag = %v; want 1", g.Lag)
			}
			if len(d.OldestPending) != 1 {
				t.Fatalf("OldestPending = %+v", d.OldestPending)
			}
			p := d.OldestPending[0]
			if p.ID.String() != ids[1] || p.TaskID != "t-2" || p.TaskName != "app:work" || p.Consumer != "c1" || p.Deliveries != 1 {
				t.Errorf("pending = %+v", p)
			}
			if d.Trimmable != 1 || d.TrimmableCapped {
				t.Errorf("Trimmable = %d capped=%v; want 1, false", d.Trimmable, d.TrimmableCapped)
			}
		})
	}
}

func TestStreamDetailTrimmableCap(t *testing.T) {
	s := readServers()[1]
	ctx := e2eCtx(t)
	r, rc := newTestReader(t, s)
	for i := 0; i < TrimmableCap+5; i++ {
		rc.XAdd(ctx, &redis.XAddArgs{Stream: "big", Values: map[string]any{"foo": "bar"}})
	}
	rc.XGroupCreate(ctx, "big", "g", "$") // everything so far counts as delivered
	time.Sleep(5 * time.Millisecond)
	d, err := r.Stream(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	if d.Trimmable != TrimmableCap || !d.TrimmableCapped {
		t.Errorf("Trimmable = %d capped=%v; want %d, true", d.Trimmable, d.TrimmableCapped, TrimmableCap)
	}
}

func TestStreamDetailForeignEntries(t *testing.T) {
	for _, s := range readServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			r, rc := newTestReader(t, s)
			rc.XAdd(ctx, &redis.XAddArgs{Stream: "f", Values: map[string]any{"foo": "bar"}})
			rc.XAdd(ctx, &redis.XAddArgs{Stream: "f", Values: map[string]any{"data": "\x80\x04\x95\x00pickled"}})
			rc.XGroupCreate(ctx, "f", "g", "0")
			rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"f", ">"}, Count: 2})

			d, err := r.Stream(ctx, "f")
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			if len(d.OldestPending) != 2 {
				t.Fatalf("OldestPending = %+v", d.OldestPending)
			}
			for _, p := range d.OldestPending {
				if p.TaskID != "" || p.TaskName != "" {
					t.Errorf("pending %+v; want empty task fields", p)
				}
			}
		})
	}
}

func TestStreamDetailNotStream(t *testing.T) {
	for _, s := range readServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			r, rc := newTestReader(t, s)
			seedQueues(t, ctx, rc)
			if _, err := r.Stream(ctx, "l1"); !errors.Is(err, ErrNotStream) {
				t.Errorf("Stream(l1) err = %v; want ErrNotStream", err)
			}
		})
	}
}
