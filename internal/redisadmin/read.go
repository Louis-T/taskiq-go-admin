package redisadmin

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// TrimmableCap bounds the count of trimmable entries, which is computed with
// a capped XRANGE rather than a full scan of the stream.
const TrimmableCap = 1000

const (
	defaultScanTTL     = 30 * time.Second
	defaultOverviewTTL = 5 * time.Second
	overviewTimeout    = 30 * time.Second
	maxPendingList     = 20
)

// ErrNotStream is returned by Stream for a key that is not a stream.
var ErrNotStream = errors.New("not a stream")

type Queue struct {
	Key           string
	Type          string // "stream" | "list"
	Length        int64
	Groups        int        // 0 for lists
	Pending       int64      // 0 for lists
	OldestPending *time.Time // entry time of the oldest pending ID across groups
	MemoryBytes   int64
}

type Overview struct {
	Server    ServerInfo
	Queues    []Queue // sorted by Key
	FetchedAt time.Time
}

type Consumer struct {
	Name    string
	Pending int64
	Idle    time.Duration
}

type Group struct {
	Name          string
	LastDelivered ID
	Lag           *int64 // nil when Redis can't tell
	Pending       int64
	Consumers     []Consumer
}

type PendingMessage struct {
	ID         ID
	TaskID     string // "" when the entry isn't a taskiq message
	TaskName   string
	Consumer   string
	Idle       time.Duration
	Deliveries int64
}

type StreamDetail struct {
	Key             string
	Length          int64
	FirstEntry      *time.Time
	LastEntry       *time.Time
	Trimmable       int64 // entries below the safe cutoff, counted up to TrimmableCap
	TrimmableCapped bool
	// TrimmableAtLeast: on Redis 8.2+ XTRIM ACKED also removes acknowledged
	// entries above a pending one, so Trimmable is a lower bound.
	TrimmableAtLeast bool
	Groups           []Group
	OldestPending    []PendingMessage // at most 20, oldest first
}

type scannedKey struct{ key, typ string }

// Reader inspects the queues of the configured database.
type Reader struct {
	c           *Client
	keepAcked   func(ctx context.Context) (time.Duration, bool)
	scanTTL     time.Duration
	overviewTTL time.Duration
	fetch       func(ctx context.Context) (Overview, error) // fetchOverview; replaced in tests

	mu        sync.Mutex
	scannedAt time.Time
	scanned   []scannedKey

	ovMu sync.Mutex
	ov   *overviewCall // the last fetch, done or in flight
}

// overviewCall is one fetch of the overview, shared by every caller that
// arrives while it runs or within overviewTTL after it succeeded.
type overviewCall struct {
	done chan struct{}
	ov   Overview
	err  error
}

// NewReader returns a Reader. keepAcked supplies the retention age used for
// the "trimmable" figure; ok=false (or a nil func) means 0.
func NewReader(c *Client, keepAcked func(ctx context.Context) (time.Duration, bool)) *Reader {
	r := &Reader{c: c, keepAcked: keepAcked, scanTTL: defaultScanTTL, overviewTTL: defaultOverviewTTL}
	r.fetch = r.fetchOverview
	return r
}

// scan lists the streams and lists of the db, cached for scanTTL.
func (r *Reader) scan(ctx context.Context) ([]scannedKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scanned != nil && time.Since(r.scannedAt) < r.scanTTL {
		return r.scanned, nil
	}
	keys := []scannedKey{}
	for _, typ := range []string{"stream", "list"} {
		var cursor uint64
		for {
			batch, next, err := r.c.rdb.ScanType(ctx, cursor, "*", 1000, typ).Result()
			if err != nil {
				return nil, err
			}
			for _, k := range batch {
				keys = append(keys, scannedKey{k, typ})
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
	}
	// SCAN may return a key more than once.
	sort.Slice(keys, func(i, j int) bool { return keys[i].key < keys[j].key })
	keys = dedupeKeys(keys)
	r.scanned, r.scannedAt = keys, time.Now()
	return keys, nil
}

func dedupeKeys(keys []scannedKey) []scannedKey {
	out := keys[:0]
	for i, k := range keys {
		if i == 0 || k != keys[i-1] {
			out = append(out, k)
		}
	}
	return out
}

// Overview describes every stream and list of the database. On a server
// older than 7.0 only Server is filled. Viewers share one fetch: a result is
// reused for overviewTTL, and callers arriving during a fetch wait for it,
// so the Redis load doesn't grow with the number of open pages.
func (r *Reader) Overview(ctx context.Context) (Overview, error) {
	r.ovMu.Lock()
	c := r.ov
	if c == nil || !c.fresh(r.overviewTTL) {
		c = &overviewCall{done: make(chan struct{})}
		r.ov = c
		// Detached from ctx: a caller leaving must not fail the others.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), overviewTimeout)
		go func() {
			defer cancel()
			c.ov, c.err = r.fetch(fctx)
			close(c.done)
		}()
	}
	r.ovMu.Unlock()
	select {
	case <-c.done:
		return c.ov, c.err
	case <-ctx.Done():
		return Overview{}, ctx.Err()
	}
}

// fresh reports whether c is in flight, or done without error less than ttl
// ago.
func (c *overviewCall) fresh(ttl time.Duration) bool {
	select {
	case <-c.done:
		return c.err == nil && time.Since(c.ov.FetchedAt) < ttl
	default:
		return true
	}
}

func (r *Reader) fetchOverview(ctx context.Context) (Overview, error) {
	info, err := r.c.ServerInfo(ctx)
	if err != nil {
		return Overview{}, err
	}
	ov := Overview{Server: info, FetchedAt: time.Now()}
	if !info.Supported {
		return ov, nil
	}
	keys, err := r.scan(ctx)
	if err != nil {
		return Overview{}, err
	}

	// Stage 1: length, memory and groups of every key in one round trip.
	type stage1 struct {
		length *redis.IntCmd
		mem    *redis.IntCmd
		groups *redis.XInfoGroupsCmd
	}
	pipe := r.c.rdb.Pipeline()
	s1 := make([]stage1, len(keys))
	for i, k := range keys {
		s1[i].mem = pipe.MemoryUsage(ctx, k.key)
		if k.typ == "stream" {
			s1[i].length = pipe.XLen(ctx, k.key)
			s1[i].groups = pipe.XInfoGroups(ctx, k.key)
		} else {
			s1[i].length = pipe.LLen(ctx, k.key)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && !isPerKeyError(err) {
		return Overview{}, err
	}

	// Stage 2: the pending summary of every group.
	type groupPending struct {
		i   int
		cmd *redis.XPendingCmd
	}
	var summaries []groupPending
	pipe = r.c.rdb.Pipeline()
	for i, k := range keys {
		if s1[i].groups == nil || s1[i].groups.Err() != nil {
			continue
		}
		for _, g := range s1[i].groups.Val() {
			summaries = append(summaries, groupPending{i, pipe.XPending(ctx, k.key, g.Name)})
		}
	}
	if len(summaries) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && !isPerKeyError(err) {
			return Overview{}, err
		}
	}
	oldest := map[int]ID{}
	for _, sp := range summaries {
		p, err := sp.cmd.Result()
		if err != nil || p.Count == 0 {
			continue
		}
		id, err := ParseID(p.Lower)
		if err != nil {
			continue
		}
		if cur, ok := oldest[sp.i]; !ok || id.Less(cur) {
			oldest[sp.i] = id
		}
	}

	for i, k := range keys {
		// A key deleted since the scan fails every command: skip it.
		if err := s1[i].length.Err(); err != nil {
			continue
		}
		q := Queue{Key: k.key, Type: k.typ, Length: s1[i].length.Val()}
		if s1[i].mem.Err() == nil {
			q.MemoryBytes = s1[i].mem.Val()
		}
		if s1[i].groups != nil && s1[i].groups.Err() == nil {
			groups := s1[i].groups.Val()
			q.Groups = len(groups)
			for _, g := range groups {
				q.Pending += g.Pending
			}
		}
		if id, ok := oldest[i]; ok {
			t := id.Time()
			q.OldestPending = &t
		}
		ov.Queues = append(ov.Queues, q)
	}
	return ov, nil
}

// isPerKeyError reports whether a pipeline error is only a command-level
// failure (e.g. a key that vanished), which callers check per command.
func isPerKeyError(err error) bool {
	var re redis.Error
	return errors.Is(err, redis.Nil) || errors.As(err, &re)
}

// groupStates reads, for every group of the stream, the last delivered ID and
// the oldest pending ID. A group destroyed between the two reads is left out.
func (r *Reader) groupStates(ctx context.Context, key string) ([]GroupState, error) {
	groups, err := r.c.rdb.XInfoGroups(ctx, key).Result()
	if err != nil {
		return nil, wrongType(err)
	}
	return r.groupStatesOf(ctx, key, groups)
}

func (r *Reader) groupStatesOf(ctx context.Context, key string, groups []redis.XInfoGroup) ([]GroupState, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	pipe := r.c.rdb.Pipeline()
	cmds := make([]*redis.XPendingCmd, len(groups))
	for i, g := range groups {
		cmds[i] = pipe.XPending(ctx, key, g.Name)
	}
	if _, err := pipe.Exec(ctx); err != nil && !isPerKeyError(err) {
		return nil, err
	}
	var states []GroupState
	for i, g := range groups {
		last, err := ParseID(g.LastDeliveredID)
		if err != nil {
			return nil, err
		}
		p, err := cmds[i].Result()
		if err != nil && strings.HasPrefix(err.Error(), "NOGROUP") {
			continue // destroyed since XINFO GROUPS: it constrains nothing now
		}
		if err != nil {
			return nil, err
		}
		st := GroupState{Name: g.Name, LastDelivered: last}
		if p.Count > 0 {
			id, err := ParseID(p.Lower)
			if err != nil {
				return nil, err
			}
			st.OldestPending = &id
		}
		states = append(states, st)
	}
	return states, nil
}

func wrongType(err error) error {
	if strings.HasPrefix(err.Error(), "WRONGTYPE") {
		return ErrNotStream
	}
	return err
}

// Stream describes one stream in detail.
func (r *Reader) Stream(ctx context.Context, key string) (StreamDetail, error) {
	info, err := r.c.ServerInfo(ctx)
	if err != nil {
		return StreamDetail{}, err
	}
	if !info.Supported {
		return StreamDetail{}, ErrUnsupportedVersion
	}
	if typ, err := r.c.rdb.Type(ctx, key).Result(); err != nil {
		return StreamDetail{}, err
	} else if typ != "stream" {
		return StreamDetail{}, ErrNotStream
	}

	si, err := r.c.rdb.XInfoStream(ctx, key).Result()
	if err != nil {
		return StreamDetail{}, wrongType(err)
	}
	d := StreamDetail{Key: key, Length: si.Length}
	if t, ok := entryTime(si.FirstEntry.ID); ok {
		d.FirstEntry = &t
	}
	if t, ok := entryTime(si.LastEntry.ID); ok {
		d.LastEntry = &t
	}

	groups, err := r.c.rdb.XInfoGroups(ctx, key).Result()
	if err != nil {
		return StreamDetail{}, wrongType(err)
	}
	for _, g := range groups {
		last, err := ParseID(g.LastDeliveredID)
		if err != nil {
			return StreamDetail{}, err
		}
		out := Group{Name: g.Name, LastDelivered: last, Pending: g.Pending}
		if g.Lag >= 0 {
			lag := g.Lag
			out.Lag = &lag
		}
		consumers, err := r.c.rdb.XInfoConsumers(ctx, key, g.Name).Result()
		if err != nil {
			return StreamDetail{}, err
		}
		for _, c := range consumers {
			out.Consumers = append(out.Consumers, Consumer{Name: c.Name, Pending: c.Pending, Idle: c.Idle})
		}
		d.Groups = append(d.Groups, out)
	}

	if d.OldestPending, err = r.oldestPending(ctx, key, groups); err != nil {
		return StreamDetail{}, err
	}

	states, err := r.groupStatesOf(ctx, key, groups)
	if err != nil {
		return StreamDetail{}, err
	}
	var keep time.Duration
	if r.keepAcked != nil {
		if k, ok := r.keepAcked(ctx); ok {
			keep = k
		}
	}
	if cutoff, ok := SafeCutoff(IDAt(time.Now().Add(-keep)), states); ok {
		entries, err := r.c.rdb.XRangeN(ctx, key, "-", "("+cutoff.String(), TrimmableCap+1).Result()
		if err != nil {
			return StreamDetail{}, err
		}
		d.Trimmable = int64(len(entries))
		if d.Trimmable > TrimmableCap {
			d.Trimmable, d.TrimmableCapped = TrimmableCap, true
		}
		d.TrimmableAtLeast = info.Version.AtLeast(8, 2, 0)
	}
	return d, nil
}

func entryTime(id string) (time.Time, bool) {
	if id == "" {
		return time.Time{}, false
	}
	parsed, err := ParseID(id)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.Time(), true
}

// oldestPending merges the oldest pending entries of every group and reads
// each entry's task fields.
func (r *Reader) oldestPending(ctx context.Context, key string, groups []redis.XInfoGroup) ([]PendingMessage, error) {
	var msgs []PendingMessage
	for _, g := range groups {
		if g.Pending == 0 {
			continue
		}
		ext, err := r.c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key, Group: g.Name, Start: "-", End: "+", Count: maxPendingList,
		}).Result()
		if err != nil {
			return nil, err
		}
		for _, e := range ext {
			id, err := ParseID(e.ID)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, PendingMessage{ID: id, Consumer: e.Consumer, Idle: e.Idle, Deliveries: e.RetryCount})
		}
	}
	sort.SliceStable(msgs, func(i, j int) bool { return msgs[i].ID.Less(msgs[j].ID) })
	if len(msgs) > maxPendingList {
		msgs = msgs[:maxPendingList]
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	pipe := r.c.rdb.Pipeline()
	cmds := make([]*redis.XMessageSliceCmd, len(msgs))
	for i, m := range msgs {
		cmds[i] = pipe.XRange(ctx, key, m.ID.String(), m.ID.String())
	}
	if _, err := pipe.Exec(ctx); err != nil && !isPerKeyError(err) {
		return nil, err
	}
	for i := range msgs {
		entries, err := cmds[i].Result()
		if err != nil || len(entries) == 0 {
			continue // trimmed since: the entry has no data left to show
		}
		if data, ok := entries[0].Values["data"].(string); ok {
			if id, name, ok := ParseTaskMessage([]byte(data)); ok {
				msgs[i].TaskID, msgs[i].TaskName = id, name
			}
		}
	}
	return msgs, nil
}
