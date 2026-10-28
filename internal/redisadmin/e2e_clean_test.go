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
			// PING from its own connection for the whole run and keep the
			// slowest round trip.
			ping := redis.NewClient(&redis.Options{Addr: s.addr, PoolSize: 1})
			defer ping.Close()
			var maxPing time.Duration
			var pings int
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
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

			sum, err := c.RunOnce(ctx)
			close(stop)
			wg.Wait()
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
	db4, db0 := rawDB(t, addrSentinelMaster, 4), rawDB(t, addrSentinelMaster, 0)
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
