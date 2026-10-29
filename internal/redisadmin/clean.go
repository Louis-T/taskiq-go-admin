package redisadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/louis-t/taskiq-go-admin/internal/maintenance"
	"github.com/louis-t/taskiq-go-admin/internal/store"
)

const (
	modeAcked      = "XTRIM ACKED"
	modeSafeCutoff = "safe cut-off (Redis < 8.2)"

	defaultTrimBatch = 1000
)

var (
	errRunning = errors.New("a cleaning run is already running")
)

// SettingsSource supplies the current settings.
type SettingsSource interface {
	GetSettings(ctx context.Context) (store.Settings, error)
}

// RunSummary describes one cleaning run.
type RunSummary struct {
	Started time.Time
	Took    time.Duration
	Mode    string // "XTRIM ACKED" | "safe cut-off (Redis < 8.2)"
	Streams int    // streams that had at least one group
	Removed int64
	Err     string
}

// CleanStatus is the state of the cleaner for the admin page.
type CleanStatus struct {
	Running   bool        // running, or triggered and about to run
	Seq       int         // increments at the end of every run
	Last      *RunSummary // nil before the first run since start
	Interval  *int        // copies of the current settings, for the page
	KeepAcked *int
}

type trimResult struct {
	mode    string
	streams int
	removed int64
	steps   int // trim commands sent
}

// trimmer does the trimming of a run; the Redis one is replaced in tests.
type trimmer interface {
	trimAll(ctx context.Context, now time.Time, keep time.Duration) (trimResult, error)
}

// Cleaner trims the acknowledged entries of the streams, on a schedule set in
// the settings or on demand. It never removes an entry that is pending or not
// yet delivered for any consumer group, and never touches a stream without
// groups.
type Cleaner struct {
	Tick   time.Duration // how often the schedule is checked; default time.Minute
	Now    func() time.Time
	OnDone func() // when set, called at the end of every run

	settings SettingsSource
	log      *slog.Logger
	trim     trimmer
	trigger  chan struct{}

	mu        sync.Mutex
	running   bool
	pending   bool // a trigger is waiting for the loop
	seq       int
	last      *RunSummary
	lastStart time.Time
}

func NewCleaner(r *Reader, s SettingsSource, batch maintenance.BatchConfig, log *slog.Logger) *Cleaner {
	if batch.Size <= 0 {
		batch.Size = defaultTrimBatch
	}
	return &Cleaner{
		Tick:     time.Minute,
		Now:      time.Now,
		settings: s,
		log:      log,
		trim:     &redisTrimmer{r: r, batch: batch, log: log},
		trigger:  make(chan struct{}, 1),
	}
}

// Run checks the schedule every Tick and serves Trigger until ctx is done.
// Runs never overlap.
func (c *Cleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(c.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		case <-c.trigger:
			c.mu.Lock()
			c.pending = false
			start := !c.running // a RunOnce may be running
			c.running = true
			c.mu.Unlock()
			if start {
				c.run(ctx, c.Now())
			}
		}
	}
}

// tick starts a run when an interval is set and has elapsed
// since the last run started.
func (c *Cleaner) tick(ctx context.Context) {
	s, err := c.settings.GetSettings(ctx)
	if err != nil {
		if ctx.Err() == nil {
			c.log.Error("trim redis streams: read settings", "err", err)
		}
		return
	}
	if s.RedisTrimIntervalMinutes == nil {
		return
	}
	interval := time.Duration(*s.RedisTrimIntervalMinutes) * time.Minute
	now := c.Now()
	c.mu.Lock()
	if c.running || now.Sub(c.lastStart) < interval {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()
	c.run(ctx, now)
}

// Trigger asks the Run loop for a run now. It returns false, doing nothing,
// when a run is already pending or running.
func (c *Cleaner) Trigger(ctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.pending {
		return false
	}
	select {
	case c.trigger <- struct{}{}:
		c.pending = true
		return true
	default:
		return false
	}
}

// RunOnce runs now, outside the loop; it fails if a run is already running.
func (c *Cleaner) RunOnce(ctx context.Context) (RunSummary, error) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return RunSummary{}, errRunning
	}
	c.running = true
	c.mu.Unlock()
	return c.run(ctx, c.Now())
}

// run does one run; the caller has set c.running.
func (c *Cleaner) run(ctx context.Context, started time.Time) (RunSummary, error) {
	c.mu.Lock()
	c.lastStart = started
	c.mu.Unlock()

	begin := time.Now()
	var res trimResult
	s, err := c.settings.GetSettings(ctx)
	if err == nil {
		res, err = c.trim.trimAll(ctx, started, s.RedisKeepAcked())
	}
	sum := RunSummary{Started: started, Took: time.Since(begin), Mode: res.mode, Streams: res.streams, Removed: res.removed}
	if err != nil {
		sum.Err = err.Error()
		if ctx.Err() == nil {
			c.log.Error("trim redis streams", "removed", sum.Removed, "err", err)
		}
	}

	c.mu.Lock()
	c.running = false
	c.seq++
	last := sum
	c.last = &last
	c.mu.Unlock()
	if c.OnDone != nil {
		c.OnDone()
	}
	return sum, err
}

// Status returns the current state; Interval and KeepAcked stay nil when the
// settings can't be read.
func (c *Cleaner) Status(ctx context.Context) CleanStatus {
	s, err := c.settings.GetSettings(ctx)
	c.mu.Lock()
	st := CleanStatus{Running: c.running || c.pending, Seq: c.seq}
	if c.last != nil {
		last := *c.last
		st.Last = &last
	}
	c.mu.Unlock()
	if err == nil {
		st.Interval, st.KeepAcked = s.RedisTrimIntervalMinutes, s.RedisKeepAckedMinutes
	}
	return st
}

type redisTrimmer struct {
	r     *Reader
	batch maintenance.BatchConfig
	log   *slog.Logger
}

// trimAll trims every stream of the db that has at least one group.
func (t *redisTrimmer) trimAll(ctx context.Context, now time.Time, keep time.Duration) (trimResult, error) {
	info, err := t.r.c.ServerInfo(ctx)
	if err != nil {
		return trimResult{}, err
	}
	if !info.Supported {
		return trimResult{}, ErrUnsupportedVersion
	}
	acked := info.Version.AtLeast(8, 2, 0)
	res := trimResult{mode: modeSafeCutoff}
	if acked {
		res.mode = modeAcked
	}
	// The scan may be up to 30 s old: a newer stream is trimmed next run,
	// a vanished or retyped key is skipped below.
	keys, err := t.r.scan(ctx)
	if err != nil {
		return res, err
	}
	ageCutoff := IDAt(now.Add(-keep))
	for _, k := range keys {
		if k.typ != "stream" {
			continue
		}
		n, steps, hadGroups, err := t.trimStream(ctx, k.key, acked, ageCutoff)
		res.steps += steps
		res.removed += n
		if hadGroups {
			res.streams++
		}
		if n > 0 {
			t.log.Info("trimmed stream", "key", k.key, "removed", n, "mode", res.mode)
		}
		if err != nil {
			if ctx.Err() == nil && !t.isStream(ctx, k.key) {
				continue
			}
			return res, fmt.Errorf("trim %s: %w", k.key, err)
		}
	}
	return res, nil
}

// isStream reports whether key is (still) a stream; false only when Redis
// says it isn't.
func (t *redisTrimmer) isStream(ctx context.Context, key string) bool {
	typ, err := t.r.c.rdb.Type(ctx, key).Result()
	return err != nil || typ == "stream"
}

// trimStream removes, in steps of batch.Size entries, the entries of key that
// are older than ageCutoff and acknowledged by every group.
//
// Each step reads the groups to pick where it ends (XRANGE), then trims with
// trimScript, which re-reads the groups in the same atomic step as the XTRIM:
// a stream whose last group was destroyed meanwhile is left alone (XTRIM ACKED
// would empty it), and below 8.2 the cut-off follows a group created, moved
// back with XGROUP SETID or delivered to since the read. No window remains
// between the group check and the trim. What it can't do: an entry already
// removed is not brought back for a group created or moved back below it
// later.
func (t *redisTrimmer) trimStream(ctx context.Context, key string, acked bool, ageCutoff ID) (removed int64, steps int, hadGroups bool, err error) {
	rdb := t.r.c.rdb
	start := "-"
	for {
		states, err := t.r.groupStates(ctx, key)
		if err != nil || len(states) == 0 {
			return removed, steps, hadGroups, err
		}
		hadGroups = true
		// Below 8.2, stop below every group's oldest pending and first
		// undelivered entry. XTRIM ACKED skips the pending ones itself
		// (8.2+), but never removes an undelivered entry either: stopping
		// at the first one keeps a large backlog from being walked in
		// steps on every run.
		target, _ := SafeCutoff(ageCutoff, states)
		if acked {
			target = deliveredCutoff(ageCutoff, states)
		}
		if target == (ID{}) {
			return removed, steps, hadGroups, nil // nothing is below 0-0
		}
		entries, err := rdb.XRangeN(ctx, key, start, "("+target.String(), int64(t.batch.Size)).Result()
		if err != nil || len(entries) == 0 {
			return removed, steps, hadGroups, err
		}
		last, err := ParseID(entries[len(entries)-1].ID)
		if err != nil {
			return removed, steps, hadGroups, err
		}
		n, err := runTrimScript(ctx, rdb, key, last.Next(), acked)
		removed += n
		steps++
		if err != nil || len(entries) < t.batch.Size {
			return removed, steps, hadGroups, err
		}
		start = "(" + last.String()
		if t.batch.Pause > 0 {
			timer := time.NewTimer(t.batch.Pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return removed, steps, hadGroups, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// trimScript removes the entries of KEYS[1] below the MINID ARGV[1], unless
// the stream has no group. With ARGV[2] = "ACKED" (8.2+) Redis keeps the
// unacknowledged entries; otherwise the MINID is first lowered to every
// group's oldest pending entry and last delivered ID + 1, read here. IDs are
// compared as decimal strings, exact over the whole uint64 range.
var trimScript = redis.NewScript(`
local key, minid = KEYS[1], ARGV[1]
local groups = redis.call('XINFO', 'GROUPS', key)
if #groups == 0 then return 0 end
if ARGV[2] == 'ACKED' then
  return redis.call('XTRIM', key, 'MINID', minid, 'ACKED')
end
local function split(id) return string.match(id, '^(%d+)-(%d+)$') end
local function numless(a, b)
  if #a ~= #b then return #a < #b end
  return a < b
end
local function idless(a, b)
  local ams, aseq = split(a)
  local bms, bseq = split(b)
  if ams ~= bms then return numless(ams, bms) end
  return numless(aseq, bseq)
end
local function incr(s)
  local i = #s
  while i > 0 and string.sub(s, i, i) == '9' do i = i - 1 end
  if i == 0 then return '1' .. string.rep('0', #s) end
  return string.sub(s, 1, i - 1) .. string.char(string.byte(s, i) + 1) .. string.rep('0', #s - i)
end
local function nextid(id)
  local ms, seq = split(id)
  if seq == '18446744073709551615' then return incr(ms) .. '-0' end
  return ms .. '-' .. incr(seq)
end
for _, g in ipairs(groups) do
  local name, last
  for i = 1, #g, 2 do
    if g[i] == 'name' then name = g[i + 1] elseif g[i] == 'last-delivered-id' then last = g[i + 1] end
  end
  local nxt = nextid(last)
  if idless(nxt, minid) then minid = nxt end
  local p = redis.call('XPENDING', key, name)
  if p[1] > 0 and idless(p[2], minid) then minid = p[2] end
end
return redis.call('XTRIM', key, 'MINID', minid)
`)

// runTrimScript runs trimScript on key and returns how many entries it removed.
func runTrimScript(ctx context.Context, rdb redis.Scripter, key string, minID ID, acked bool) (int64, error) {
	mode := ""
	if acked {
		mode = "ACKED"
	}
	return trimScript.Run(ctx, rdb, []string{key}, minID.String(), mode).Int64()
}
