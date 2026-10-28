package redisadmin

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
)

func TestConnectDoesNoIO(t *testing.T) {
	// Nothing listens on port 1: Connect must still succeed.
	c, err := Connect(config.Redis{URL: "redis://127.0.0.1:1/3", DB: 3})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	if db := c.Redis().(*redis.Client).Options().DB; db != 3 {
		t.Errorf("DB = %d; want 3", db)
	}
}

func TestConnectInvalidURL(t *testing.T) {
	if _, err := Connect(config.Redis{URL: "http://h"}); err == nil {
		t.Error("Connect(http://h): want error")
	}
}

func TestConnectSentinelUsesURLCredentialsOnly(t *testing.T) {
	c, err := Connect(config.Redis{
		URL:        "rediss://user:pw@ignored.example:1/2",
		Sentinels:  []string{"127.0.0.1:1"},
		MasterName: "mymaster",
		DB:         4,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer c.Close()
	o := c.Redis().(*redis.Client).Options()
	if o.Username != "user" || o.Password != "pw" || o.TLSConfig == nil || o.DB != 4 {
		t.Errorf("options = user %q pw %q tls %v db %d; want user, pw, TLS, 4",
			o.Username, o.Password, o.TLSConfig != nil, o.DB)
	}
	if o.Addr == "ignored.example:1" {
		t.Error("sentinel mode used the REDIS_URL host")
	}
}

func TestParseServerInfo(t *testing.T) {
	raw := "# Server\r\nredis_version:7.2.4\r\nuptime_in_seconds:3600\r\n\r\n" +
		"# Clients\r\nconnected_clients:5\r\n\r\n" +
		"# Memory\r\nused_memory:1048576\r\nmaxmemory:0\r\n"
	got, err := parseServerInfo(raw)
	if err != nil {
		t.Fatalf("parseServerInfo: %v", err)
	}
	want := ServerInfo{Version: Version{7, 2, 4}, Supported: true, UsedMemory: 1048576,
		MaxMemory: 0, Clients: 5, Uptime: time.Hour}
	if got != want {
		t.Errorf("got %+v; want %+v", got, want)
	}

	got, err = parseServerInfo("redis_version:6.2.14\r\nmaxmemory:100\r\n")
	if err != nil || got.Supported || got.Version.Major != 6 || got.MaxMemory != 100 {
		t.Errorf("6.2: got %+v, %v; want unsupported 6.x with maxmemory 100", got, err)
	}

	if _, err := parseServerInfo("uptime_in_seconds:1\r\n"); err == nil {
		t.Error("missing redis_version: want error")
	}
}
