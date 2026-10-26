// Package config loads the configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	APIToken string

	DBDriver   string // only "sqlite" for now
	DBFilePath string

	Addr string

	MaxBodyBytes   int64
	IngestMaxBatch int

	PurgeInterval   time.Duration
	PurgeBatchSize  int
	PurgeBatchPause time.Duration

	LogLevel slog.Level
}

// Load reads the configuration from the environment.
func Load() (Config, error) { return load(os.Getenv) }

func load(getenv func(string) string) (Config, error) {
	var errs []error
	get := func(key, def string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return v
		}
		return def
	}
	intVar := func(key string, def int) int {
		v, err := strconv.Atoi(get(key, strconv.Itoa(def)))
		if err != nil || v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive integer", key))
		}
		return v
	}
	durVar := func(key string, def time.Duration) time.Duration {
		v, err := time.ParseDuration(get(key, def.String()))
		if err != nil || v < 0 {
			errs = append(errs, fmt.Errorf("%s must be a duration such as 1m or 500ms", key))
		}
		return v
	}

	c := Config{
		APIToken:        get("TASKIQ_ADMIN_API_TOKEN", ""),
		DBDriver:        get("TASKIQ_ADMIN_DB_DRIVER", "sqlite"),
		DBFilePath:      get("TASKIQ_ADMIN_DB_FILE_PATH", "database/database.db"),
		Addr:            net.JoinHostPort(get("HOST", "0.0.0.0"), get("PORT", "3000")),
		MaxBodyBytes:    int64(intVar("TASKIQ_ADMIN_MAX_BODY_BYTES", 8<<20)),
		IngestMaxBatch:  intVar("TASKIQ_ADMIN_INGEST_MAX_BATCH", 500),
		PurgeInterval:   durVar("TASKIQ_ADMIN_PURGE_INTERVAL", time.Minute),
		PurgeBatchSize:  intVar("TASKIQ_ADMIN_PURGE_BATCH_SIZE", 1000),
		PurgeBatchPause: durVar("TASKIQ_ADMIN_PURGE_BATCH_PAUSE", 50*time.Millisecond),
	}
	if c.APIToken == "" {
		errs = append(errs, errors.New("TASKIQ_ADMIN_API_TOKEN is required"))
	}
	if c.DBDriver != "sqlite" {
		errs = append(errs, fmt.Errorf("TASKIQ_ADMIN_DB_DRIVER %q is not supported (supported: sqlite)", c.DBDriver))
	}
	if c.PurgeInterval < time.Second {
		errs = append(errs, errors.New("TASKIQ_ADMIN_PURGE_INTERVAL must be at least 1s"))
	}
	if err := c.LogLevel.UnmarshalText([]byte(get("TASKIQ_ADMIN_LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, errors.New("TASKIQ_ADMIN_LOG_LEVEL must be debug, info, warn or error"))
	}
	return c, errors.Join(errs...)
}
