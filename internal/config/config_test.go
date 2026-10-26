package config

import (
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
