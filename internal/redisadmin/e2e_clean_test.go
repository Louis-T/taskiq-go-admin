//go:build e2e

package redisadmin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
)

// cleanDB is the db every cleaner test uses (read tests use 11).
const cleanDB = 12

type cleanServer struct {
	name  string
	addr  string
	acked bool // Redis >= 8.2: XTRIM ... ACKED
}

func cleanServers() []cleanServer {
	return []cleanServer{{"redis70", addrRedis70, false}, {"redis82", addrRedis82, true}}
}

func (s cleanServer) mode() string {
	if s.acked {
		return modeAcked
	}
	return modeSafeCutoff
}

func newE2ECleaner(t *testing.T, cfg config.Redis, keepMinutes int, batch int) *Cleaner {
	t.Helper()
	r := NewReader(connect(t, cfg), nil)
	fs := &fakeSettings{s: store.Settings{RedisKeepAckedMinutes: intp(keepMinutes)}}
	return NewCleaner(r, fs, maintenance.BatchConfig{Size: batch}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// addEntries adds n entries to key with IDs <base ms>-1 .. <base ms>-n.
func addEntries(t *testing.T, ctx context.Context, rc *redis.Client, key string, base time.Time, n int) []string {
	t.Helper()
	ms := IDAt(base).Ms
	ids := make([]string, n)
	pipe := rc.Pipeline()
	for i := range n {
		ids[i] = ID{Ms: ms, Seq: uint64(i + 1)}.String()
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: ids[i], Values: []any{"data", i}})
		if pipe.Len() == 10000 {
			if _, err := pipe.Exec(ctx); err != nil {
				t.Fatalf("XADD: %v", err)
			}
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("XADD: %v", err)
	}
	return ids
}

// readAndAck makes group g (created at 0) read count entries and ack them.
func readAndAck(t *testing.T, ctx context.Context, rc *redis.Client, key, group string, count int) {
	t.Helper()
	if err := rc.XGroupCreate(ctx, key, group, "0").Err(); err != nil {
		t.Fatalf("XGROUP CREATE: %v", err)
	}
	for count > 0 {
		n := min(count, 10000)
		res, err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c", Streams: []string{key, ">"}, Count: int64(n)}).Result()
		if err != nil {
			t.Fatalf("XREADGROUP: %v", err)
		}
		ids := make([]string, len(res[0].Messages))
		for i, m := range res[0].Messages {
			ids[i] = m.ID
		}
		if err := rc.XAck(ctx, key, group, ids...).Err(); err != nil {
			t.Fatalf("XACK: %v", err)
		}
		count -= n
	}
}

func streamIDs(t *testing.T, ctx context.Context, rc *redis.Client, key string) []string {
	t.Helper()
	entries, err := rc.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE: %v", err)
	}
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	return ids
}

// assertNothingUnackedRemoved checks the cleaner's invariant: every pending
// entry and every entry not yet delivered to some group is still there.
func assertNothingUnackedRemoved(t *testing.T, ctx context.Context, rc *redis.Client, key string, before []string) {
	t.Helper()
	after := streamIDs(t, ctx, rc, key)
	groups, err := rc.XInfoGroups(ctx, key).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS: %v", err)
	}
	for _, g := range groups {
		pending, err := rc.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: key, Group: g.Name, Start: "-", End: "+", Count: 1 << 20}).Result()
		if err != nil {
			t.Fatalf("XPENDING: %v", err)
		}
		for _, p := range pending {
			if !slices.Contains(after, p.ID) {
				t.Errorf("group %s: pending entry %s was removed", g.Name, p.ID)
			}
		}
		last, _ := ParseID(g.LastDeliveredID)
		for _, id := range before {
			if pid, _ := ParseID(id); last.Less(pid) && !slices.Contains(after, id) {
				t.Errorf("group %s: undelivered entry %s was removed", g.Name, id)
			}
		}
	}
}

func checkSummary(t *testing.T, sum RunSummary, err error, mode string, streams int, removed int64) {
	t.Helper()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if sum.Mode != mode || sum.Streams != streams || sum.Removed != removed || sum.Err != "" {
		t.Errorf("summary = %+v; want mode %q, %d streams, %d removed", sum, mode, streams, removed)
	}
}

// Redis 8.2 behavior, checked on redis:8.2 with this exact setup:
// `XTRIM s MINID <id> ACKED` skips the unacknowledged entries (pending or
// undelivered) and removes the acknowledged entries above them, so 5 is
// removed although 4 is pending. (On a stream with no group at all it removes
// everything, which is why the cleaner checks for groups before each step.)
func TestCleanerKeepsUnacked(t *testing.T) {
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			ids := addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), 10)
			if err := rc.XGroupCreate(ctx, "q", "g", "0").Err(); err != nil {
				t.Fatalf("XGROUP CREATE: %v", err)
			}
			if err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"q", ">"}, Count: 6}).Err(); err != nil {
				t.Fatalf("XREADGROUP: %v", err)
			}
			if err := rc.XAck(ctx, "q", "g", ids[0], ids[1], ids[2], ids[4]).Err(); err != nil {
				t.Fatalf("XACK: %v", err)
			}

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 100)
			sum, err := c.RunOnce(ctx)

			want, removed := ids[3:], int64(3) // stops at the oldest pending entry
			if s.acked {
				want, removed = append([]string{ids[3]}, ids[5:]...), 4
			}
			checkSummary(t, sum, err, s.mode(), 1, removed)
			if got := streamIDs(t, ctx, rc, "q"); !slices.Equal(got, want) {
				t.Errorf("remaining = %v; want %v", got, want)
			}
			assertNothingUnackedRemoved(t, ctx, rc, "q", ids)
		})
	}
}

func TestCleanerKeepAge(t *testing.T) {
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			addEntries(t, ctx, rc, "q", time.Now().Add(-2*time.Hour), 5)
			recent := addEntries(t, ctx, rc, "q", time.Now().Add(-10*time.Minute), 5)
			readAndAck(t, ctx, rc, "q", "g", 10)

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 60, 100)
			sum, err := c.RunOnce(ctx)
			checkSummary(t, sum, err, s.mode(), 1, 5)
			if got := streamIDs(t, ctx, rc, "q"); !slices.Equal(got, recent) {
				t.Errorf("remaining = %v; want the recent half %v", got, recent)
			}
		})
	}
}

func TestCleanerNoGroup(t *testing.T) {
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			ids := addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), 5)

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 100)
			sum, err := c.RunOnce(ctx)
			checkSummary(t, sum, err, s.mode(), 0, 0)
			if got := streamIDs(t, ctx, rc, "q"); !slices.Equal(got, ids) {
				t.Errorf("remaining = %v; want untouched %v", got, ids)
			}
		})
	}
}

func TestCleanerTwoGroups(t *testing.T) {
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			ids := addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), 5)
			readAndAck(t, ctx, rc, "q", "g", 5)
			if err := rc.XGroupCreate(ctx, "q", "g2", "0").Err(); err != nil {
				t.Fatalf("XGROUP CREATE g2: %v", err)
			}

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 100)
			sum, err := c.RunOnce(ctx)
			checkSummary(t, sum, err, s.mode(), 1, 0)
			if got := streamIDs(t, ctx, rc, "q"); !slices.Equal(got, ids) {
				t.Errorf("remaining = %v; want untouched %v", got, ids)
			}
		})
	}
}

// measurePings PINGs addr from its own connection until stop is called, and
// returns how many PINGs were sent and the slowest round trip.
func measurePings(t *testing.T, ctx context.Context, addr string) (stop func() (int, time.Duration)) {
	t.Helper()
	ping := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 1})
	var maxPing time.Duration
	var pings int
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			start := time.Now()
			if err := ping.Ping(ctx).Err(); err != nil {
				t.Errorf("PING: %v", err)
				return
			}
			maxPing = max(maxPing, time.Since(start))
			pings++
		}
	}()
	return func() (int, time.Duration) {
		close(done)
		wg.Wait()
		ping.Close()
		return pings, maxPing
	}
}

func TestCleanerLargeStreamInSteps(t *testing.T) {
	const n = 200_000
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			rc := rawDB(t, s.addr, cleanDB)
			addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), n)
			readAndAck(t, ctx, rc, "q", "g", n)

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 10_000)
			stop := measurePings(t, ctx, s.addr)
			sum, err := c.RunOnce(ctx)
			pings, maxPing := stop()
			checkSummary(t, sum, err, s.mode(), 1, n)
			if l, err := rc.XLen(ctx, "q").Result(); err != nil || l != 0 {
				t.Errorf("XLEN = %d, %v; want 0", l, err)
			}
			if pings == 0 || maxPing > 200*time.Millisecond {
				t.Errorf("%d PINGs during the run, slowest %v; want some, all under 200ms", pings, maxPing)
			}
			t.Logf("run took %v, %d PINGs, slowest %v", sum.Took, pings, maxPing)
		})
	}
}

func TestCleanerSentinelDB(t *testing.T) {
	ctx := e2eCtx(t)
	master := sentinelMaster(t)
	db4, db0 := rawDB(t, master, 4), rawDB(t, master, 0)
	base := time.Now().Add(-time.Hour)
	for _, rc := range []*redis.Client{db4, db0} {
		addEntries(t, ctx, rc, "q", base, 5)
		readAndAck(t, ctx, rc, "q", "g", 5)
	}

	c := newE2ECleaner(t, config.Redis{Sentinels: []string{addrSentinel}, MasterName: "mymaster", DB: 4}, 0, 100)
	sum, err := c.RunOnce(ctx)
	checkSummary(t, sum, err, modeAcked, 1, 5)
	if l, _ := db4.XLen(ctx, "q").Result(); l != 0 {
		t.Errorf("db 4 XLEN = %d; want 0", l)
	}
	if l, _ := db0.XLen(ctx, "q").Result(); l != 5 {
		t.Errorf("db 0 XLEN = %d; want 5 (untouched)", l)
	}
}

func TestCleanerUnsupported(t *testing.T) {
	ctx := e2eCtx(t)
	rc := rawDB(t, addrRedis62, cleanDB)
	addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), 5)
	readAndAck(t, ctx, rc, "q", "g", 5)

	c := newE2ECleaner(t, config.Redis{URL: "redis://" + addrRedis62, DB: cleanDB}, 0, 100)
	sum, err := c.RunOnce(ctx)
	if !errors.Is(err, ErrUnsupportedVersion) || sum.Err != ErrUnsupportedVersion.Error() {
		t.Errorf("RunOnce = %+v, %v; want ErrUnsupportedVersion", sum, err)
	}
	if l, _ := rc.XLen(ctx, "q").Result(); l != 5 {
		t.Errorf("XLEN = %d; want 5 (untouched)", l)
	}
}

func TestCleanerRedisDown(t *testing.T) {
	ctx := e2eCtx(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	c := newE2ECleaner(t, config.Redis{URL: "redis://" + addr}, 0, 100)
	if _, err := c.RunOnce(ctx); err == nil {
		t.Error("RunOnce succeeded against a closed port")
	}
	if st := c.Status(ctx); st.Last == nil || st.Last.Err == "" || st.Seq != 1 {
		t.Errorf("Status = %+v; want a recorded error", st)
	}
}

// A key from the cached scan that has since vanished or changed type is
// skipped; the other streams are still trimmed.
func TestCleanerSkipsVanishedKeys(t *testing.T) {
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			base := time.Now().Add(-time.Hour)
			for _, k := range []string{"a", "b", "c"} {
				addEntries(t, ctx, rc, k, base, 3)
				readAndAck(t, ctx, rc, k, "g", 3)
			}
			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 100)
			if _, err := c.RunOnce(ctx); err != nil { // fills the scan cache
				t.Fatalf("first RunOnce: %v", err)
			}
			// New acked entries in c; a vanishes; b becomes a string.
			addEntries(t, ctx, rc, "c", base.Add(time.Minute), 3)
			res, err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"c", ">"}}).Result()
			if err != nil || len(res[0].Messages) != 3 {
				t.Fatalf("XREADGROUP: %v %v", res, err)
			}
			if err := rc.XAck(ctx, "c", "g", streamIDs(t, ctx, rc, "c")...).Err(); err != nil {
				t.Fatalf("XACK: %v", err)
			}
			rc.Del(ctx, "a")
			rc.Del(ctx, "b")
			rc.Set(ctx, "b", "now a string", 0)

			sum, err := c.RunOnce(ctx)
			checkSummary(t, sum, err, s.mode(), 1, 3)
			if l, _ := rc.XLen(ctx, "c").Result(); l != 0 {
				t.Errorf("XLEN c = %d; want 0", l)
			}
			if v, _ := rc.Get(ctx, "b").Result(); v != "now a string" {
				t.Errorf("b = %q; the string key was touched", v)
			}
		})
	}
}

// With the workers down, a stream holds a large undelivered backlog: on 8.2
// too the run must stop at the last delivered entry instead of walking the
// backlog in steps every run.
func TestCleanerUndeliveredBacklog(t *testing.T) {
	const n, delivered = 200_000, 10
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			rc := rawDB(t, s.addr, cleanDB)
			ids := addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), n)
			readAndAck(t, ctx, rc, "q", "g", delivered)

			c := newE2ECleaner(t, config.Redis{URL: "redis://" + s.addr, DB: cleanDB}, 0, 10_000)
			stop := measurePings(t, ctx, s.addr)
			res, err := c.trim.trimAll(ctx, time.Now(), 0)
			pings, maxPing := stop()
			if err != nil || res.removed != delivered || res.streams != 1 {
				t.Fatalf("trimAll = %+v, %v; want %d removed from 1 stream", res, err, delivered)
			}
			if res.steps != 1 {
				t.Errorf("%d trim steps; want 1 (stop at the last delivered entry)", res.steps)
			}
			// The remaining entries are a subset of ids: with this length,
			// first and last, they are exactly the undelivered ones.
			l, err := rc.XLen(ctx, "q").Result()
			first, err1 := rc.XRangeN(ctx, "q", "-", "+", 1).Result()
			last, err2 := rc.XRevRangeN(ctx, "q", "+", "-", 1).Result()
			if err != nil || err1 != nil || err2 != nil || l != n-delivered || first[0].ID != ids[delivered] || last[0].ID != ids[n-1] {
				t.Errorf("after the run: XLEN %d, first %v, last %v (%v %v %v); want the %d undelivered entries", l, first, last, err, err1, err2, n-delivered)
			}
			if pings == 0 || maxPing > 200*time.Millisecond {
				t.Errorf("%d PINGs during the run, slowest %v; want some, all under 200ms", pings, maxPing)
			}
		})
	}
}

// The trim script re-reads the groups in the same atomic step as XTRIM.
func TestTrimScript(t *testing.T) {
	const maxSeq = "18446744073709551615"
	far := ID{Ms: 1 << 50}
	add := func(t *testing.T, ctx context.Context, rc *redis.Client, ids ...string) {
		t.Helper()
		for _, id := range ids {
			if err := rc.XAdd(ctx, &redis.XAddArgs{Stream: "q", ID: id, Values: []any{"data", id}}).Err(); err != nil {
				t.Fatalf("XADD %s: %v", id, err)
			}
		}
	}
	for _, s := range cleanServers() {
		t.Run(s.name, func(t *testing.T) {
			ctx := e2eCtx(t)
			rc := rawDB(t, s.addr, cleanDB)
			check := func(what string, acked bool, wantRemoved int64, want ...string) {
				t.Helper()
				n, err := runTrimScript(ctx, rc, "q", far, acked)
				if err != nil || n != wantRemoved {
					t.Errorf("%s: removed %d, %v; want %d", what, n, err, wantRemoved)
				}
				if got := streamIDs(t, ctx, rc, "q"); !slices.Equal(got, want) {
					t.Errorf("%s: remaining %v; want %v", what, got, want)
				}
			}

			// No group: nothing is removed, on either path.
			add(t, ctx, rc, "1-1", "1-2")
			check("no group, cut-off", false, 0, "1-1", "1-2")
			if s.acked {
				check("no group, ACKED", true, 0, "1-1", "1-2")
			}

			// Cut-off path: stops at the oldest pending entry.
			add(t, ctx, rc, "1-3", "1-4")
			if err := rc.XGroupCreate(ctx, "q", "g", "0").Err(); err != nil {
				t.Fatal(err)
			}
			if err := rc.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"q", ">"}, Count: 3}).Err(); err != nil {
				t.Fatal(err)
			}
			rc.XAck(ctx, "q", "g", "1-1", "1-3")
			check("pending 1-2", false, 1, "1-2", "1-3", "1-4")

			// Last delivered + 1, with a carry in the sequence and at its maximum.
			rc.XAck(ctx, "q", "g", "1-2")
			check("delivered up to 1-3", false, 2, "1-4")
			rc.Del(ctx, "q")
			add(t, ctx, rc, "9-98", "9-99", "9-100")
			rc.XGroupCreate(ctx, "q", "g", "9-99")
			check("delivered up to 9-99", false, 2, "9-100")
			rc.Del(ctx, "q")
			add(t, ctx, rc, "5-"+maxSeq, "6-0")
			rc.XGroupCreate(ctx, "q", "g", "5-"+maxSeq)
			check("delivered up to 5-max", false, 1, "6-0")
		})
	}
}

// A group destroyed between XINFO GROUPS and XPENDING no longer constrains
// anything: it is skipped instead of failing the run.
func TestGroupStatesSkipsVanishedGroup(t *testing.T) {
	ctx := e2eCtx(t)
	rc := rawDB(t, addrRedis70, cleanDB)
	addEntries(t, ctx, rc, "q", time.Now().Add(-time.Hour), 3)
	readAndAck(t, ctx, rc, "q", "g", 2)
	groups, err := rc.XInfoGroups(ctx, "q").Result()
	if err != nil {
		t.Fatal(err)
	}
	gone := redis.XInfoGroup{Name: "gone", LastDeliveredID: "0-0"}
	r := NewReader(connect(t, config.Redis{URL: "redis://" + addrRedis70, DB: cleanDB}), nil)

	states, err := r.groupStatesOf(ctx, "q", append(groups, gone))
	if err != nil || len(states) != 1 || states[0].Name != "g" {
		t.Errorf("groupStatesOf = %+v, %v; want only g", states, err)
	}
	if states, err := r.groupStatesOf(ctx, "q", []redis.XInfoGroup{gone}); err != nil || len(states) != 0 {
		t.Errorf("all groups gone: %+v, %v; want none", states, err)
	}
}
