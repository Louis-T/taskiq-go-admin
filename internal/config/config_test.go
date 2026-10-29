package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestDefaults(t *testing.T) {
	c, err := load(env(map[string]string{"TASKIQ_ADMIN_API_TOKEN": "s"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != "0.0.0.0:3000" || c.DBDriver != "sqlite" || c.PurgeInterval != time.Minute || c.PurgeBatchSize != 1000 || c.MaxBodyBytes != 64<<20 || c.MaxValueBytes != 64<<10 {
		t.Fatalf("%+v", c)
	}
}

func TestErrors(t *testing.T) {
	cases := []map[string]string{
		{},
		{"TASKIQ_ADMIN_API_TOKEN": "s", "TASKIQ_ADMIN_DB_DRIVER": "postgres"},
		{"TASKIQ_ADMIN_API_TOKEN": "s", "TASKIQ_ADMIN_PURGE_BATCH_SIZE": "0"},
		{"TASKIQ_ADMIN_API_TOKEN": "s", "TASKIQ_ADMIN_PURGE_INTERVAL": "soon"},
		{"TASKIQ_ADMIN_API_TOKEN": "s", "TASKIQ_ADMIN_LOG_LEVEL": "loud"},
	}
	for _, m := range cases {
		if _, err := load(env(m)); err == nil {
			t.Errorf("%v: expected an error", m)
		}
	}
}

func TestLoadRedis(t *testing.T) {
	ok := []struct {
		name string
		env  map[string]string
		want *Redis
	}{
		{"unset", map[string]string{}, nil},
		{"url db", map[string]string{"REDIS_URL": "redis://h:6379/3"}, &Redis{URL: "redis://h:6379/3", DB: 3}},
		{"url no db", map[string]string{"REDIS_URL": "redis://h:6379"}, &Redis{URL: "redis://h:6379"}},
		{"unix query db", map[string]string{"REDIS_URL": "unix:///tmp/redis.sock?db=2"}, &Redis{URL: "unix:///tmp/redis.sock?db=2", DB: 2}},
		{"host", map[string]string{"REDIS_HOST": "h"}, &Redis{Addr: "h:6379"}},
		{"host full", map[string]string{"REDIS_HOST": "h", "REDIS_PORT": "6380", "REDIS_USERNAME": "u", "REDIS_PASSWORD": "pw", "REDIS_DATABASE": "5", "REDIS_TLS": "true"},
			&Redis{Addr: "h:6380", Username: "u", Password: "pw", DB: 5, TLS: true}},
		{"host ipv6", map[string]string{"REDIS_HOST": "::1"}, &Redis{Addr: "[::1]:6379"}},
		{"host ipv6 bracketed", map[string]string{"REDIS_HOST": "[::1]"}, &Redis{Addr: "[::1]:6379"}},
		{"sentinel", map[string]string{"REDIS_SENTINELS": " s1:26379, s2:26380 ", "REDIS_SENTINEL_MASTER": "mymaster", "REDIS_DATABASE": "4"},
			&Redis{Sentinels: []string{"s1:26379", "s2:26380"}, MasterName: "mymaster", DB: 4}},
		{"sentinel default port", map[string]string{"REDIS_SENTINELS": "s1,[::1],[::2]:1", "REDIS_SENTINEL_MASTER": "m"},
			&Redis{Sentinels: []string{"s1:26379", "[::1]:26379", "[::2]:1"}, MasterName: "m"}},
		{"sentinel auth", map[string]string{"REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER": "m", "REDIS_SENTINEL_USERNAME": "su", "REDIS_SENTINEL_PASSWORD": "spw",
			"REDIS_USERNAME": "u", "REDIS_PASSWORD": "pw", "REDIS_TLS": "1"},
			&Redis{Sentinels: []string{"s1:26379"}, MasterName: "m", SentinelUsername: "su", SentinelPassword: "spw", Username: "u", Password: "pw", TLS: true}},
	}
	for _, tc := range ok {
		t.Run(tc.name, func(t *testing.T) {
			tc.env["TASKIQ_ADMIN_API_TOKEN"] = "x"
			c, err := load(env(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.Redis, tc.want) {
				t.Fatalf("got %+v, want %+v", c.Redis, tc.want)
			}
		})
	}

	bad := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"url + database", map[string]string{"REDIS_URL": "redis://h/3", "REDIS_DATABASE": "5"}, "REDIS_DATABASE cannot be combined with REDIS_URL"},
		{"url + sentinels", map[string]string{"REDIS_URL": "redis://h", "REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER": "m"}, "REDIS_SENTINELS cannot be combined with REDIS_URL"},
		{"url + password", map[string]string{"REDIS_URL": "redis://h", "REDIS_PASSWORD": "pw"}, "REDIS_PASSWORD cannot be combined with REDIS_URL"},
		{"scheme", map[string]string{"REDIS_URL": "http://h"}, "REDIS_URL"},
		{"path", map[string]string{"REDIS_URL": "redis://h/x"}, "REDIS_URL"},
		{"old master name", map[string]string{"REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER_NAME": "m"}, "renamed to REDIS_SENTINEL_MASTER"},
		{"no address", map[string]string{"REDIS_PASSWORD": "pw"}, "REDIS_HOST or REDIS_SENTINELS is required when REDIS_PASSWORD is set"},
		{"host + sentinels", map[string]string{"REDIS_HOST": "h", "REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER": "m"}, "cannot both be set"},
		{"host with port", map[string]string{"REDIS_HOST": "h:6379"}, "REDIS_HOST"},
		{"bad port", map[string]string{"REDIS_HOST": "h", "REDIS_PORT": "0"}, "REDIS_PORT"},
		{"host + sentinel master", map[string]string{"REDIS_HOST": "h", "REDIS_SENTINEL_MASTER": "m"}, "REDIS_SENTINEL_MASTER needs REDIS_SENTINELS"},
		{"sentinels + port", map[string]string{"REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER": "m", "REDIS_PORT": "1"}, "REDIS_PORT"},
		{"bad sentinel port", map[string]string{"REDIS_SENTINELS": "s1:abc", "REDIS_SENTINEL_MASTER": "m"}, "REDIS_SENTINELS"},
		{"bare ipv6 sentinel", map[string]string{"REDIS_SENTINELS": "::1", "REDIS_SENTINEL_MASTER": "m"}, "REDIS_SENTINELS"},
		{"no master", map[string]string{"REDIS_SENTINELS": "s1:26379"}, "REDIS_SENTINEL_MASTER is required"},
		{"db abc", map[string]string{"REDIS_HOST": "h", "REDIS_DATABASE": "abc"}, "REDIS_DATABASE"},
		{"db negative", map[string]string{"REDIS_HOST": "h", "REDIS_DATABASE": "-1"}, "REDIS_DATABASE"},
		{"tls", map[string]string{"REDIS_HOST": "h", "REDIS_TLS": "yes"}, "REDIS_TLS"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			tc.env["TASKIQ_ADMIN_API_TOKEN"] = "x"
			_, err := load(env(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want mention of %q", err, tc.want)
			}
		})
	}
}
