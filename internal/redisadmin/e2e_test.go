//go:build e2e

package redisadmin

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
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
	addrRedis82        = e2eAddr("E2E_REDIS82", "localhost:6382")
	addrRedis70        = e2eAddr("E2E_REDIS70", "localhost:6370")
	addrRedis62        = e2eAddr("E2E_REDIS62", "localhost:6362")
	addrSentinel       = e2eAddr("E2E_SENTINEL", "localhost:26379")
	addrSentinelMaster = e2eAddr("E2E_SENTINEL_MASTER", "localhost:6390")
)

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
	db4, db0 := rawDB(t, addrSentinelMaster, 4), rawDB(t, addrSentinelMaster, 0)
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
