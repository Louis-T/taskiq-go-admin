package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Redis describes how to reach the Redis instance that holds taskiq data.
type Redis struct {
	URL        string   // raw REDIS_URL, "" allowed in sentinel mode
	Sentinels  []string // "host:port"; empty = standalone
	MasterName string
	DB         int
}

// IsSentinel reports whether Redis is reached through Sentinel.
func (r *Redis) IsSentinel() bool { return len(r.Sentinels) > 0 }

// parseRedis reads the REDIS_* variables. It returns nil when neither
// REDIS_URL nor REDIS_SENTINELS is set.
func parseRedis(get func(key, def string) string) (*Redis, []error) {
	rawURL := get("REDIS_URL", "")
	rawSentinels := get("REDIS_SENTINELS", "")
	if rawURL == "" && rawSentinels == "" {
		return nil, nil
	}
	var errs []error
	r := &Redis{URL: rawURL, MasterName: get("REDIS_SENTINEL_MASTER_NAME", "")}

	if rawURL != "" {
		db, err := parseRedisURL(rawURL)
		if err != nil {
			errs = append(errs, fmt.Errorf("REDIS_URL is invalid: %w", err))
		}
		r.DB = db
	}

	if rawSentinels != "" {
		for _, s := range strings.Split(rawSentinels, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			host, port, err := net.SplitHostPort(s)
			p, perr := strconv.Atoi(port)
			if err != nil || host == "" || perr != nil || p < 1 || p > 65535 {
				errs = append(errs, fmt.Errorf("REDIS_SENTINELS entry %q must be host:port", s))
				continue
			}
			r.Sentinels = append(r.Sentinels, s)
		}
		if len(r.Sentinels) == 0 && len(errs) == 0 {
			errs = append(errs, errors.New("REDIS_SENTINELS must list at least one host:port"))
		}
		if r.MasterName == "" {
			errs = append(errs, errors.New("REDIS_SENTINEL_MASTER_NAME is required when REDIS_SENTINELS is set"))
		}
	}

	if v := get("REDIS_DATABASE", ""); v != "" {
		db, err := strconv.Atoi(v)
		if err != nil || db < 0 {
			errs = append(errs, errors.New("REDIS_DATABASE must be a non-negative integer"))
		} else {
			r.DB = db
		}
	}
	return r, errs
}

// parseRedisURL validates the URL like redis.ParseURL would and returns the
// database it selects.
func parseRedisURL(raw string) (int, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return 0, errors.New("not a valid URL")
	}
	switch u.Scheme {
	case "redis", "rediss":
		path := strings.TrimPrefix(u.Path, "/")
		if path == "" {
			return 0, nil
		}
		db, err := strconv.Atoi(path)
		if err != nil || db < 0 {
			return 0, fmt.Errorf("path %q must be empty or a non-negative database number", u.Path)
		}
		return db, nil
	case "unix":
		// The path is the socket; the database comes from ?db=N.
		q := u.Query().Get("db")
		if q == "" {
			return 0, nil
		}
		db, err := strconv.Atoi(q)
		if err != nil || db < 0 {
			return 0, fmt.Errorf("db %q must be a non-negative integer", q)
		}
		return db, nil
	default:
		return 0, fmt.Errorf("scheme %q must be redis, rediss or unix", u.Scheme)
	}
}
