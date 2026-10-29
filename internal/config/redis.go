package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Redis describes how to reach the Redis instance that holds taskiq data:
// either a self-contained REDIS_URL, or the separate variables.
type Redis struct {
	URL string // raw REDIS_URL; when set, every field below but DB is empty

	Addr       string   // REDIS_HOST:REDIS_PORT; empty in sentinel mode
	Sentinels  []string // "host:port"; empty = standalone
	MasterName string

	Username, Password                 string // master
	SentinelUsername, SentinelPassword string
	TLS                                bool // master only
	DB                                 int  // resolved from the URL in URL mode
}

// IsSentinel reports whether Redis is reached through Sentinel.
func (r *Redis) IsSentinel() bool { return len(r.Sentinels) > 0 }

const defaultSentinelPort = "26379"

// redisVars are the separate variables, all rejected next to REDIS_URL.
var redisVars = []string{
	"REDIS_HOST", "REDIS_PORT", "REDIS_SENTINELS", "REDIS_SENTINEL_MASTER",
	"REDIS_SENTINEL_USERNAME", "REDIS_SENTINEL_PASSWORD",
	"REDIS_USERNAME", "REDIS_PASSWORD", "REDIS_DATABASE", "REDIS_TLS",
}

// parseRedis reads the REDIS_* variables. It returns nil when none is set.
func parseRedis(get func(key, def string) string) (*Redis, []error) {
	var errs []error
	if get("REDIS_SENTINEL_MASTER_NAME", "") != "" {
		errs = append(errs, errors.New("REDIS_SENTINEL_MASTER_NAME was renamed to REDIS_SENTINEL_MASTER"))
	}
	var set []string
	for _, k := range redisVars {
		if get(k, "") != "" {
			set = append(set, k)
		}
	}

	if rawURL := get("REDIS_URL", ""); rawURL != "" {
		r := &Redis{URL: rawURL}
		db, err := parseRedisURL(rawURL)
		if err != nil {
			errs = append(errs, fmt.Errorf("REDIS_URL is invalid: %w", err))
		}
		r.DB = db
		for _, k := range set {
			errs = append(errs, fmt.Errorf("%s cannot be combined with REDIS_URL, which already holds the whole connection", k))
		}
		return r, errs
	}
	if len(set) == 0 {
		return nil, errs
	}

	r := &Redis{
		MasterName:       get("REDIS_SENTINEL_MASTER", ""),
		Username:         get("REDIS_USERNAME", ""),
		Password:         get("REDIS_PASSWORD", ""),
		SentinelUsername: get("REDIS_SENTINEL_USERNAME", ""),
		SentinelPassword: get("REDIS_SENTINEL_PASSWORD", ""),
	}
	host, rawSentinels := get("REDIS_HOST", ""), get("REDIS_SENTINELS", "")
	switch {
	case host != "" && rawSentinels != "":
		errs = append(errs, errors.New("REDIS_HOST and REDIS_SENTINELS cannot both be set"))
	case host != "":
		// An IPv6 literal may come bracketed or not; any other colon is a
		// port given in the wrong variable.
		h := strings.Trim(host, "[]")
		if h == "" || strings.ContainsAny(h, "/ ") || (strings.Contains(h, ":") && net.ParseIP(h) == nil) {
			errs = append(errs, fmt.Errorf("REDIS_HOST %q must be a host name or IP, with the port in REDIS_PORT", host))
		}
		port := get("REDIS_PORT", "6379")
		if !validPort(port) {
			errs = append(errs, errors.New("REDIS_PORT must be a port number"))
		}
		r.Addr = net.JoinHostPort(h, port)
		for _, k := range []string{"REDIS_SENTINEL_MASTER", "REDIS_SENTINEL_USERNAME", "REDIS_SENTINEL_PASSWORD"} {
			if get(k, "") != "" {
				errs = append(errs, fmt.Errorf("%s needs REDIS_SENTINELS", k))
			}
		}
	case rawSentinels != "":
		if get("REDIS_PORT", "") != "" {
			errs = append(errs, errors.New("REDIS_PORT applies to REDIS_HOST: give each sentinel as host:port in REDIS_SENTINELS"))
		}
		for _, s := range strings.Split(rawSentinels, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			addr, ok := sentinelAddr(s)
			if !ok {
				errs = append(errs, fmt.Errorf("REDIS_SENTINELS entry %q must be host or host:port", s))
				continue
			}
			r.Sentinels = append(r.Sentinels, addr)
		}
		if len(r.Sentinels) == 0 && len(errs) == 0 {
			errs = append(errs, errors.New("REDIS_SENTINELS must list at least one host"))
		}
		if r.MasterName == "" {
			errs = append(errs, errors.New("REDIS_SENTINEL_MASTER is required when REDIS_SENTINELS is set"))
		}
	default:
		errs = append(errs, fmt.Errorf("REDIS_HOST or REDIS_SENTINELS is required when %s is set", strings.Join(set, ", ")))
	}

	if v := get("REDIS_DATABASE", ""); v != "" {
		db, err := strconv.Atoi(v)
		if err != nil || db < 0 {
			errs = append(errs, errors.New("REDIS_DATABASE must be a non-negative integer"))
		}
		r.DB = db
	}
	if v := get("REDIS_TLS", ""); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, errors.New("REDIS_TLS must be true or false"))
		}
		r.TLS = b
	}
	return r, errs
}

// sentinelAddr normalizes a REDIS_SENTINELS entry to host:port, adding the
// default Sentinel port when it has none ("h", "[::1]").
func sentinelAddr(s string) (string, bool) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		bracketed := strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
		if !bracketed && strings.Contains(s, ":") {
			return "", false
		}
		host, port = strings.Trim(s, "[]"), defaultSentinelPort
	}
	if host == "" || !validPort(port) {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}

func validPort(s string) bool {
	p, err := strconv.Atoi(s)
	return err == nil && p >= 1 && p <= 65535
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
