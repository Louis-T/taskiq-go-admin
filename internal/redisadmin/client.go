package redisadmin

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/config"
)

// ErrUnsupportedVersion is returned by operations that need Redis 7.0+.
var ErrUnsupportedVersion = errors.New("Redis 7.0 or later is required")

// Client is a connection to the configured Redis database, direct or
// through Sentinel.
type Client struct {
	rdb redis.UniversalClient
	cfg config.Redis
}

// Connect builds the client for cfg; it does no I/O.
func Connect(cfg config.Redis) (*Client, error) {
	var fromURL *redis.Options
	if cfg.URL != "" {
		o, err := redis.ParseURL(cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL: %w", err)
		}
		fromURL = o
	}

	var rdb redis.UniversalClient
	if cfg.IsSentinel() {
		// The URL host is ignored; its credentials and TLS apply to the
		// master only, sentinels are reached without auth.
		fo := &redis.FailoverOptions{
			MasterName:    cfg.MasterName,
			SentinelAddrs: cfg.Sentinels,
			DB:            cfg.DB,
		}
		if fromURL != nil {
			fo.Username, fo.Password, fo.TLSConfig = fromURL.Username, fromURL.Password, fromURL.TLSConfig
		}
		rdb = redis.NewFailoverClient(fo)
	} else {
		if fromURL == nil {
			return nil, errors.New("REDIS_URL is required without REDIS_SENTINELS")
		}
		fromURL.DB = cfg.DB
		rdb = redis.NewClient(fromURL)
	}
	return &Client{rdb: rdb, cfg: cfg}, nil
}

func (c *Client) Close() error { return c.rdb.Close() }

// Redis returns the underlying go-redis client.
func (c *Client) Redis() redis.UniversalClient { return c.rdb }

// ServerInfo describes the server currently answering for the configured
// database.
type ServerInfo struct {
	Version    Version
	Supported  bool   // Version >= 7.0.0
	Mode       string // "standalone" | "sentinel"
	MasterName string
	DB         int
	UsedMemory int64
	MaxMemory  int64 // 0 = no limit
	Clients    int
	Uptime     time.Duration
}

// ServerInfo reads INFO from the server. It is never cached, so after a
// Sentinel failover it describes the new master.
func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error) {
	// Plain INFO returns the default sections (server, clients, memory, ...).
	// Multi-section INFO needs Redis 7, and 6.x must still be reported.
	raw, err := c.rdb.Info(ctx).Result()
	if err != nil {
		return ServerInfo{}, err
	}
	info, err := parseServerInfo(raw)
	if err != nil {
		return ServerInfo{}, err
	}
	info.Mode, info.DB = "standalone", c.cfg.DB
	if c.cfg.IsSentinel() {
		info.Mode, info.MasterName = "sentinel", c.cfg.MasterName
	}
	return info, nil
}

// parseServerInfo extracts the fields of ServerInfo that come from INFO.
func parseServerInfo(raw string) (ServerInfo, error) {
	fields := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[k] = v
		}
	}
	rawVersion, ok := fields["redis_version"]
	if !ok {
		return ServerInfo{}, errors.New("INFO has no redis_version")
	}
	v, err := ParseVersion(rawVersion)
	if err != nil {
		return ServerInfo{}, err
	}
	num := func(k string) int64 {
		n, _ := strconv.ParseInt(fields[k], 10, 64)
		return n
	}
	return ServerInfo{
		Version:    v,
		Supported:  v.AtLeast(7, 0, 0),
		UsedMemory: num("used_memory"),
		MaxMemory:  num("maxmemory"),
		Clients:    int(num("connected_clients")),
		Uptime:     time.Duration(num("uptime_in_seconds")) * time.Second,
	}, nil
}
