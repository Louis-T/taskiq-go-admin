package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/louis-t/taskiq-go-admin/internal/redisadmin"
)

// RedisReader reads the Redis queues; *redisadmin.Reader implements it.
type RedisReader interface {
	Overview(ctx context.Context) (redisadmin.Overview, error)
	Stream(ctx context.Context, key string) (redisadmin.StreamDetail, error)
}

// RedisCleaner runs the stream cleanup; *redisadmin.Cleaner implements it.
type RedisCleaner interface {
	Trigger(ctx context.Context) bool
	Status(ctx context.Context) redisadmin.CleanStatus
}

var (
	_ RedisReader  = (*redisadmin.Reader)(nil)
	_ RedisCleaner = (*redisadmin.Cleaner)(nil)
)

func streamURL(key string) string { return "/redis/streams/" + url.PathEscape(key) }

// humanBytes renders a size with binary units: "412 MB".
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	v, suffix := float64(n), "KMGTPE"
	i := -1
	for v >= unit && i < len(suffix)-1 {
		v /= unit
		i++
	}
	s := strconv.FormatFloat(v, 'f', 1, 64)
	return strings.TrimSuffix(s, ".0") + " " + string(suffix[i]) + "B"
}

// age renders a duration coarsely: "4 s", "22 min", "3 h", "12 d".
func age(d time.Duration) string {
	switch {
	case d < time.Minute:
		return strconv.Itoa(int(max(d, 0).Seconds())) + " s"
	case d < time.Hour:
		return strconv.Itoa(int(d.Minutes())) + " min"
	case d < 24*time.Hour:
		return strconv.Itoa(int(d.Hours())) + " h"
	}
	return strconv.Itoa(int(d.Hours()/24)) + " d"
}

func ageSince(t *time.Time) string {
	if t == nil {
		return "—"
	}
	return age(time.Since(*t))
}

func agoSince(t *time.Time) string {
	if t == nil {
		return "—"
	}
	return age(time.Since(*t)) + " ago"
}

func minutesText(v *int) string {
	switch {
	case v == nil:
		return ""
	case *v == 0:
		return "0 minutes"
	}
	return humanDuration(time.Duration(*v) * time.Minute)
}

type queueRow struct {
	redisadmin.Queue
	Warn bool
}

type redisPage struct {
	layoutData
	Server redisadmin.ServerInfo
	Queues []queueRow
	Error  string
	Clean  cleanBox
	Stale  time.Duration
}

type streamPage struct {
	layoutData
	Key    string
	Detail redisadmin.StreamDetail
	Error  string
	Stale  time.Duration // 0: no threshold
}

type cleanBox struct {
	redisadmin.CleanStatus
	Loc *time.Location
}

// readError is the banner text for a reader failure.
func readError(err error, ver redisadmin.Version) string {
	if errors.Is(err, redisadmin.ErrUnsupportedVersion) {
		return unsupportedText(ver)
	}
	return "Could not read Redis: " + err.Error()
}

func unsupportedText(ver redisadmin.Version) string {
	msg := redisadmin.ErrUnsupportedVersion.Error()
	if ver != (redisadmin.Version{}) {
		msg += " (detected " + ver.String() + ")"
	}
	return msg
}

// staleQueued is the "queued for too long" threshold, 0 when unset.
func (s *Server) staleQueued(r *http.Request) time.Duration {
	set, err := s.store.GetSettings(r.Context())
	if err != nil || set.StaleQueuedMinutes == nil {
		return 0
	}
	return time.Duration(*set.StaleQueuedMinutes) * time.Minute
}

func (s *Server) cleanBox(r *http.Request) cleanBox {
	return cleanBox{CleanStatus: s.opts.RedisCleaner.Status(r.Context()), Loc: location(r)}
}

func (s *Server) uiRedis(w http.ResponseWriter, r *http.Request) {
	ov, err := s.opts.Redis.Overview(r.Context())
	if r.Context().Err() != nil {
		return
	}
	data := redisPage{layoutData: s.layout(r, "Redis", "redis"), Server: ov.Server, Stale: s.staleQueued(r)}
	switch {
	case err != nil:
		data.Error = readError(err, ov.Server.Version)
	case !ov.Server.Supported:
		data.Error = unsupportedText(ov.Server.Version)
	default:
		for _, q := range ov.Queues {
			warn := data.Stale > 0 && q.OldestPending != nil && time.Since(*q.OldestPending) > data.Stale
			data.Queues = append(data.Queues, queueRow{Queue: q, Warn: warn})
		}
	}
	data.Clean = s.cleanBox(r)
	if htmxTarget(r) == "redis-body" {
		s.render(w, r, "redis", "redis-body", data)
		return
	}
	s.render(w, r, "redis", "layout", data)
}

func (s *Server) uiRedisStream(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	detail, err := s.opts.Redis.Stream(r.Context(), key)
	if r.Context().Err() != nil {
		return
	}
	if errors.Is(err, redisadmin.ErrNotStream) {
		http.NotFound(w, r)
		return
	}
	data := streamPage{layoutData: s.layout(r, key, "redis"), Key: key, Detail: detail, Stale: s.staleQueued(r)}
	if err != nil {
		data.Error = readError(err, redisadmin.Version{})
	}
	if htmxTarget(r) == "redis-stream-body" {
		s.render(w, r, "redis-stream", "redis-stream-body", data)
		return
	}
	s.render(w, r, "redis-stream", "layout", data)
}

// uiRedisCleanup serves the cleanup box alone, polled by the page. When a
// run the page was watching has ended, it also asks the browser for a toast.
func (s *Server) uiRedisCleanup(w http.ResponseWriter, r *http.Request) {
	box := s.cleanBox(r)
	q := r.URL.Query()
	seen, _ := strconv.Atoi(q.Get("seen"))
	if q.Get("running") == "1" && box.Seq > seen && box.Last != nil {
		msg := fmt.Sprintf("Cleanup removed %d entries", box.Last.Removed)
		if box.Last.Err != "" {
			msg = "Cleanup failed: " + box.Last.Err
		}
		w.Header().Set("HX-Trigger", toastTrigger(msg))
	}
	s.render(w, r, "redis", "redis-cleanup", box)
}

func (s *Server) uiRedisCleanNow(w http.ResponseWriter, r *http.Request) {
	started := s.opts.RedisCleaner.Trigger(r.Context())
	if htmxTarget(r) == "" {
		http.Redirect(w, r, "/redis", http.StatusSeeOther)
		return
	}
	box := s.cleanBox(r)
	box.Running = box.Running || started
	s.render(w, r, "redis", "redis-cleanup", box)
}

// toastTrigger is the HX-Trigger value firing the "toast" event. Header
// values must stay ASCII, so other characters are \u-escaped.
func toastTrigger(msg string) string {
	b, _ := json.Marshal(map[string]string{"toast": msg})
	var sb strings.Builder
	for _, r := range string(b) {
		if r < 0x80 {
			sb.WriteRune(r)
			continue
		}
		for _, u := range utf16.Encode([]rune{r}) {
			fmt.Fprintf(&sb, `\u%04x`, u)
		}
	}
	return sb.String()
}
