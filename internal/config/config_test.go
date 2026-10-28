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
		name  string
		env   map[string]string
		nil_  bool
		url   string
		sent  []string
		db    int
		isSen bool
	}{
		{name: "unset", env: map[string]string{}, nil_: true},
		{name: "url db", env: map[string]string{"REDIS_URL": "redis://h:6379/3"}, url: "redis://h:6379/3", db: 3},
		{name: "url no db", env: map[string]string{"REDIS_URL": "redis://h:6379"}, url: "redis://h:6379"},
		{name: "override", env: map[string]string{"REDIS_URL": "redis://h:6379/3", "REDIS_DATABASE": "5"}, url: "redis://h:6379/3", db: 5},
		{name: "unix query db", env: map[string]string{"REDIS_URL": "unix:///tmp/redis.sock?db=2"}, url: "unix:///tmp/redis.sock?db=2", db: 2},
		{name: "sentinel", env: map[string]string{"REDIS_SENTINELS": " s1:26379, s2:26379 ", "REDIS_SENTINEL_MASTER_NAME": "mymaster", "REDIS_DATABASE": "4"},
			sent: []string{"s1:26379", "s2:26379"}, db: 4, isSen: true},
		{name: "sentinel url db", env: map[string]string{"REDIS_SENTINELS": "s1:26379", "REDIS_SENTINEL_MASTER_NAME": "m", "REDIS_URL": "redis://:pw@ignored:1/2"},
			url: "redis://:pw@ignored:1/2", sent: []string{"s1:26379"}, db: 2, isSen: true},
	}
	for _, tc := range ok {
		t.Run(tc.name, func(t *testing.T) {
			tc.env["TASKIQ_ADMIN_API_TOKEN"] = "x"
			c, err := load(env(tc.env))
			if err != nil {
				t.Fatal(err)
			}
			if tc.nil_ {
				if c.Redis != nil {
					t.Fatalf("expected nil, got %+v", c.Redis)
				}
				return
			}
			r := c.Redis
			if r == nil {
				t.Fatal("Redis is nil")
			}
			if r.DB != tc.db || r.IsSentinel() != tc.isSen || !reflect.DeepEqual(r.Sentinels, tc.sent) {
				t.Fatalf("%+v", r)
			}
			if tc.name == "sentinel" && r.URL != "" {
				t.Fatalf("URL = %q", r.URL)
			}
		})
	}

	bad := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"db abc", map[string]string{"REDIS_URL": "redis://h", "REDIS_DATABASE": "abc"}, "REDIS_DATABASE"},
		{"db negative", map[string]string{"REDIS_URL": "redis://h", "REDIS_DATABASE": "-1"}, "REDIS_DATABASE"},
		{"scheme", map[string]string{"REDIS_URL": "http://h"}, "REDIS_URL"},
		{"path", map[string]string{"REDIS_URL": "redis://h/x"}, "REDIS_URL"},
		{"no port", map[string]string{"REDIS_SENTINELS": "s1", "REDIS_SENTINEL_MASTER_NAME": "m"}, "REDIS_SENTINELS"},
		{"bad port", map[string]string{"REDIS_SENTINELS": "s1:abc", "REDIS_SENTINEL_MASTER_NAME": "m"}, "REDIS_SENTINELS"},
		{"no master", map[string]string{"REDIS_SENTINELS": "s1:26379"}, "REDIS_SENTINEL_MASTER_NAME"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			tc.env["TASKIQ_ADMIN_API_TOKEN"] = "x"
			_, err := load(env(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want mention of %s", err, tc.want)
			}
		})
	}
}
