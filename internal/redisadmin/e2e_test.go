//go:build e2e

package redisadmin

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
)

// e2eAddr returns the address of an e2e server, overridable by env (e.g.
// E2E_REDIS82=host:port).
func e2eAddr(env, def string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return def
}

var (
	addrRedis82  = e2eAddr("E2E_REDIS82", "localhost:6382")
	addrRedis70  = e2eAddr("E2E_REDIS70", "localhost:6370")
	addrRedis62  = e2eAddr("E2E_REDIS62", "localhost:6362")
	addrSentinel = e2eAddr("E2E_SENTINEL", "localhost:26379")
)

// sentinelMaster returns the address Sentinel gives for mymaster now: the
// failover test swaps master and replica, so no test assumes which is which.
func sentinelMaster(t *testing.T) string {
	t.Helper()
	sc := redis.NewSentinelClient(&redis.Options{Addr: addrSentinel})
	defer sc.Close()
	addr, err := sc.GetMasterAddrByName(context.Background(), "mymaster").Result()
	if err != nil {
		t.Fatalf("SENTINEL GET-MASTER-ADDR-BY-NAME: %v", err)
	}
	return net.JoinHostPort(addr[0], addr[1])
}

func e2eCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// rawDB opens a plain client on addr/db and flushes that db now and after
// the test.
func rawDB(t *testing.T, addr string, db int) *redis.Client {
	t.Helper()
	rc := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	ctx := context.Background()
	if err := rc.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush %s db %d: %v", addr, db, err)
	}
	t.Cleanup(func() {
		rc.FlushDB(context.Background())
		rc.Close()
	})
	return rc
}

func connect(t *testing.T, cfg config.Redis) *Client {
	t.Helper()
	c, err := Connect(cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// assertKeyOnlyIn checks that key exists in db want and not in db 0.
func assertKeyOnlyIn(t *testing.T, ctx context.Context, want, db0 *redis.Client, key string) {
	t.Helper()
	if n, err := want.Exists(ctx, key).Result(); err != nil || n != 1 {
		t.Errorf("key %q in target db: exists=%d err=%v; want 1", key, n, err)
	}
	if n, err := db0.Exists(ctx, key).Result(); err != nil || n != 0 {
		t.Errorf("key %q in db 0: exists=%d err=%v; want 0", key, n, err)
	}
}

func TestConnectStandaloneDB(t *testing.T) {
	ctx := e2eCtx(t)
	db3, db0 := rawDB(t, addrRedis82, 3), rawDB(t, addrRedis82, 0)
	c := connect(t, config.Redis{URL: "redis://" + addrRedis82 + "/3", DB: 3})

	if err := c.Redis().Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	assertKeyOnlyIn(t, ctx, db3, db0, "k")

	info, err := c.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if info.DB != 3 || info.Mode != "standalone" || !info.Supported {
		t.Errorf("ServerInfo = %+v; want DB 3, standalone, supported", info)
	}
	if info.Version.Major != 8 || info.UsedMemory <= 0 || info.Clients < 1 || info.Uptime <= 0 {
		t.Errorf("ServerInfo = %+v; want version 8.x and non-zero memory, clients, uptime", info)
	}
}

func TestConnectDatabaseOverride(t *testing.T) {
	ctx := e2eCtx(t)
	db5, db3, db0 := rawDB(t, addrRedis82, 5), rawDB(t, addrRedis82, 3), rawDB(t, addrRedis82, 0)
	c := connect(t, config.Redis{URL: "redis://" + addrRedis82 + "/3", DB: 5})

	if err := c.Redis().Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	assertKeyOnlyIn(t, ctx, db5, db0, "k")
	if n, _ := db3.Exists(ctx, "k").Result(); n != 0 {
		t.Errorf("key landed in db 3 (URL path) instead of db 5")
	}
}

func TestConnectSentinelDB(t *testing.T) {
	ctx := e2eCtx(t)
	master := sentinelMaster(t)
	db4, db0 := rawDB(t, master, 4), rawDB(t, master, 0)
	c := connect(t, config.Redis{Sentinels: []string{addrSentinel}, MasterName: "mymaster", DB: 4})

	if err := c.Redis().Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	assertKeyOnlyIn(t, ctx, db4, db0, "k")

	info, err := c.ServerInfo(ctx)
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if info.Mode != "sentinel" || info.MasterName != "mymaster" || info.DB != 4 || !info.Supported {
		t.Errorf("ServerInfo = %+v; want sentinel, mymaster, DB 4, supported", info)
	}
}

func TestServerInfoRedis70Supported(t *testing.T) {
	c := connect(t, config.Redis{URL: "redis://" + addrRedis70})
	info, err := c.ServerInfo(e2eCtx(t))
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if !info.Supported || info.Version.Major != 7 || info.Version.Minor != 0 {
		t.Errorf("ServerInfo = %+v; want supported 7.0.x", info)
	}
}

func TestServerInfoOldRedis(t *testing.T) {
	c := connect(t, config.Redis{URL: "redis://" + addrRedis62})
	info, err := c.ServerInfo(e2eCtx(t))
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if info.Supported || info.Version.Major != 6 {
		t.Errorf("ServerInfo = %+v; want unsupported 6.x", info)
	}
}

// failover makes Sentinel promote the replica and waits until the address it
// gives for mymaster is no longer from and that server is a master.
func failover(t *testing.T, ctx context.Context, from string) string {
	t.Helper()
	sc := redis.NewSentinelClient(&redis.Options{Addr: addrSentinel})
	defer sc.Close()
	deadline := time.Now().Add(60 * time.Second)
	// NOGOODSLAVE or INPROG while the replica resyncs after a previous failover.
	for err := sc.Failover(ctx, "mymaster").Err(); err != nil; err = sc.Failover(ctx, "mymaster").Err() {
		if time.Now().After(deadline) {
			t.Fatalf("SENTINEL FAILOVER: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Sentinel promotes the replica first and demotes the old master a moment
	// later; until then both accept writes. Wait for both, so a client still
	// connected to the old master gets READONLY instead of writing there.
	for {
		if addr := sentinelMaster(t); addr != from && hasRole(ctx, addr, "master") && hasRole(ctx, from, "slave") {
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatalf("no new master, or old master %s not demoted, after SENTINEL FAILOVER", from)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// hasRole reports whether the server at addr has this replication role
// ("master" or "slave").
func hasRole(ctx context.Context, addr, role string) bool {
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	info, err := rc.Info(ctx, "replication").Result()
	return err == nil && strings.Contains(info, "role:"+role+"\r\n")
}

// After a failover the client follows the new master, still on db 4: reads,
// ServerInfo and a cleaner run work there and db 0 stays untouched. The test
// fails back at the end.
func TestSentinelFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg := config.Redis{Sentinels: []string{addrSentinel}, MasterName: "mymaster", DB: 4}
	c := connect(t, cfg)
	if _, err := c.ServerInfo(ctx); err != nil {
		t.Fatalf("ServerInfo before the failover: %v", err)
	}

	old := sentinelMaster(t)
	master := failover(t, ctx, old)
	// Registered before rawDB: its flushes run first, on this master.
	t.Cleanup(func() { failover(t, context.Background(), master) })

	db4, db0 := rawDB(t, master, 4), rawDB(t, master, 0)
	base := time.Now().Add(-time.Hour)
	for _, rc := range []*redis.Client{db4, db0} {
		addEntries(t, ctx, rc, "q", base, 5)
		readAndAck(t, ctx, rc, "q", "g", 5)
	}

	// Connections to the old master answer READONLY until the client has
	// switched.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := c.Redis().Set(ctx, "probe", "x", 0).Err()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client still not writing to the new master: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n, _ := db4.Exists(ctx, "probe").Result(); n != 1 {
		t.Fatal("the client's write did not land in db 4 of the new master")
	}

	info, err := c.ServerInfo(ctx)
	if err != nil || info.Mode != "sentinel" || info.DB != 4 || !info.Supported {
		t.Errorf("ServerInfo = %+v, %v; want sentinel, DB 4, supported", info, err)
	}
	r := NewReader(c, nil)
	ov, err := r.Overview(ctx)
	if err != nil || len(ov.Queues) != 1 || ov.Queues[0].Key != "q" || ov.Queues[0].Length != 5 {
		t.Errorf("Overview = %+v, %v; want stream q with 5 entries", ov, err)
	}
	cl := NewCleaner(r, &fakeSettings{s: store.Settings{RedisKeepAckedMinutes: intp(0)}}, maintenance.BatchConfig{Size: 100}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sum, err := cl.RunOnce(ctx)
	checkSummary(t, sum, err, modeAcked, 1, 5)
	if l, _ := db4.XLen(ctx, "q").Result(); l != 0 {
		t.Errorf("db 4 XLEN = %d; want 0", l)
	}
	if l, _ := db0.XLen(ctx, "q").Result(); l != 5 {
		t.Errorf("db 0 XLEN = %d; want 5 (untouched)", l)
	}
}
