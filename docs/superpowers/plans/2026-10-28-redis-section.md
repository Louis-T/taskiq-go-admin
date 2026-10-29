# Redis Section Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `/redis` section that shows taskiq queues' backlog and a recurring, never-lossy trim of acknowledged stream entries, for standalone Redis and Sentinel.

**Architecture:** A new `internal/redisadmin` package owns the connection (go-redis v9), the read models (overview, stream detail) and a `Cleaner` shaped like `maintenance.Purger`. `internal/config` parses the `REDIS_*` variables; `internal/store.Settings` gets two runtime settings; `internal/web` gets the pages behind two small interfaces so unit tests use fakes. Everything that talks to Redis is tested end-to-end against Docker behind the `e2e` build tag.

**Tech Stack:** Go 1.27, `github.com/redis/go-redis/v9`, htmx (already vendored), Docker Compose for e2e, Python taskiq + taskiq-redis for the real-worker e2e.

**Spec:** `docs/superpowers/specs/2026-10-28-redis-section-design.md`

## Global Constraints

- Env vars: `REDIS_URL`, `REDIS_SENTINELS` (comma-separated `host:port`), `REDIS_SENTINEL_MASTER_NAME`, `REDIS_DATABASE`.
- Section enabled iff `REDIS_URL` or `REDIS_SENTINELS` is set; otherwise no Redis code runs and no nav entry shows.
- Database: `REDIS_DATABASE` if set, else `REDIS_URL` path `/N`, else `0`; same rule in Sentinel mode (applied to the master).
- Sentinel mode: `REDIS_URL` host ignored; its username/password/TLS apply to the master only. No sentinel auth.
- Invalid config stops startup (joined with the other config errors); unreachable Redis never does, and never affects `/healthz`.
- Minimum Redis 7.0. Below: banner "Redis 7.0 or later is required" and no cleanup.
- Redis ≥ 8.2.0: `XTRIM … MINID <id> ACKED`. Below: `XTRIM … MINID <safe cutoff>` where safe cutoff = min(age cutoff, every group's oldest pending ID, every group's last-delivered ID + 1).
- Never delete an unacknowledged or undelivered entry. A stream without consumer groups is never trimmed. All streams in the DB with a group are trimmed (no filtering).
- Cleanup settings `redis_trim_interval_minutes` (nil = scheduled job off, 1..525600) and `redis_keep_acked_minutes` (nil = unset, 0..525600). Off by default.
- The page is read-only except "Clean now". Runs never overlap.
- `go test ./...` must not need Redis; e2e tests use `//go:build e2e` and `make e2e`.

## Review Focus

1. **Redis goes down while the page is open, or at startup** → page shows an error banner and keeps polling; cleanup logs and retries next tick; the rest of the app is unaffected. (Task 7 test `TestRedisPageShowsReaderError`, Task 6 e2e `TestCleanerRedisDown`.)
2. **A stream whose entries aren't taskiq messages** (no `data` field, non-JSON, pickled) → listed and trimmed like any other, task name shown as "—", no error. (Task 2 `TestParseTaskMessage`, Task 4 e2e `TestStreamDetailForeignEntries`.)
3. **Key names with `/`, spaces, `%` or `{}`** → the stream page link round-trips and opens the right key. (Task 7 `TestRedisStreamKeyEscaping`.)
4. **"Clean now" clicked twice quickly, or while a scheduled run is going** → one run, the second click is a no-op returning the running box. (Task 6 `TestCleanerTriggerWhileRunning`.)
5. **A huge stream on the first run** (≥ 200k acked entries) → removed in bounded steps; Redis keeps answering other clients between steps. (Task 6 e2e `TestCleanerLargeStreamInSteps`.)

---

## File Structure

- `internal/config/redis.go` — `Redis` config type and its parsing (new). `internal/config/config.go` gains `Redis *Redis`.
- `internal/redisadmin/streamid.go` — stream ID value type and arithmetic.
- `internal/redisadmin/version.go` — Redis version parsing/comparison.
- `internal/redisadmin/cutoff.go` — the pure `< 8.2` safe-cutoff computation.
- `internal/redisadmin/message.go` — extracting task id/name from an entry.
- `internal/redisadmin/client.go` — `Connect`, `Client`, `ServerInfo`.
- `internal/redisadmin/read.go` — scan + overview + stream detail.
- `internal/redisadmin/clean.go` — `Cleaner`.
- `internal/redisadmin/e2e_test.go` (+ helpers) — `//go:build e2e`.
- `internal/store/store.go`, `internal/store/sqlite/sqlite.go`, `internal/store/sqlite/schema.go`, `internal/store/storetest/storetest.go` — two new settings.
- `internal/web/redis.go`, `internal/web/templates/redis.html`, `internal/web/templates/redis_stream.html` — pages.
- `internal/web/templates/layout.html`, `settings.html`, `ui.go`, `server.go`, `view.go`, `static/app.js`, `static/app.css` — wiring.
- `cmd/taskiq-admin/main.go` — wiring.
- `e2e/docker-compose.yml`, `e2e/worker/` (Dockerfile, `worker.py`), `Makefile`, `.github/workflows/ci.yml`, `README.md`, `env-example`.

---

### Task 1: Redis configuration

**Files:**
- Create: `internal/config/redis.go`
- Modify: `internal/config/config.go` (add field, call parser inside `load`, join its errors)
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces:
  ```go
  type Redis struct {
      URL        string   // raw REDIS_URL, "" allowed in sentinel mode
      Sentinels  []string // "host:port"; empty = standalone
      MasterName string
      DB         int
  }
  func (r *Redis) IsSentinel() bool
  // Config.Redis is nil when neither REDIS_URL nor REDIS_SENTINELS is set.
  func parseRedis(get func(key, def string) string) (*Redis, []error)
  ```

- [ ] **Step 1: Write failing table test `TestLoadRedis`** with `load(getenv)` and these cases (base env always has `TASKIQ_ADMIN_API_TOKEN=x`):
  - nothing set → `c.Redis == nil`, no error.
  - `REDIS_URL=redis://h:6379/3` → `DB==3`, `IsSentinel()==false`.
  - `REDIS_URL=redis://h:6379` → `DB==0`.
  - `REDIS_URL=redis://h:6379/3`, `REDIS_DATABASE=5` → `DB==5`.
  - `REDIS_SENTINELS=" s1:26379, s2:26379 "`, `REDIS_SENTINEL_MASTER_NAME=mymaster`, `REDIS_DATABASE=4` → `Sentinels==[]string{"s1:26379","s2:26379"}`, `DB==4`, `URL==""`.
  - sentinels + `REDIS_URL=redis://:pw@ignored:1/2` → `DB==2`.
  - errors (each asserts `err` mentions the variable name): `REDIS_DATABASE=abc`, `REDIS_DATABASE=-1`, `REDIS_URL=http://h`, `REDIS_URL=redis://h/x`, `REDIS_SENTINELS=s1` (no port), `REDIS_SENTINELS=s1:abc`, sentinels without `REDIS_SENTINEL_MASTER_NAME`.
- [ ] **Step 2: Run** `go test ./internal/config/ -run TestLoadRedis` → FAIL (undefined).
- [ ] **Step 3: Implement `parseRedis`.** Validate the URL with `redis.ParseURL` semantics but without importing go-redis here: accept schemes `redis`, `rediss`, `unix`; path must be empty, `/` or `/<non-negative int>`. Sentinel entries via `net.SplitHostPort` + `strconv.Atoi` port 1..65535.
- [ ] **Step 4: Run** `go test ./internal/config/` → PASS.
- [ ] **Step 5: Commit** `feat(config): parse REDIS_* variables`.

### Task 2: Pure helpers (stream IDs, versions, safe cutoff, task messages)

**Files:**
- Create: `internal/redisadmin/streamid.go`, `version.go`, `cutoff.go`, `message.go`
- Test: `internal/redisadmin/helpers_test.go`

**Interfaces:**
- Produces:
  ```go
  type ID struct{ Ms, Seq uint64 }
  func ParseID(s string) (ID, error)          // "1700000000000-3"; also accepts "1700000000000" (Seq 0)
  func (a ID) String() string
  func (a ID) Less(b ID) bool
  func (a ID) Next() ID                        // Seq+1, carrying into Ms on overflow
  func (a ID) Time() time.Time                 // UnixMilli(Ms)
  func IDAt(t time.Time) ID                    // {Ms: t.UnixMilli(), Seq: 0}

  type Version struct{ Major, Minor, Patch int }
  func ParseVersion(s string) (Version, error) // "8.2.1", "7.0.15", "7.2.4-valkey"→7.2.4
  func (v Version) AtLeast(major, minor, patch int) bool
  func (v Version) String() string

  type GroupState struct {
      Name          string
      LastDelivered ID
      OldestPending *ID // nil when the group has nothing pending
  }
  // SafeCutoff returns the MINID below which every entry is acknowledged by
  // every group and at least as old as ageCutoff; ok=false when groups is empty.
  func SafeCutoff(ageCutoff ID, groups []GroupState) (cutoff ID, ok bool)

  // ParseTaskMessage reads the taskiq BrokerMessage JSON stored in an entry's
  // "data" field. ok=false for anything else.
  func ParseTaskMessage(data []byte) (taskID, taskName string, ok bool)
  ```

- [ ] **Step 1: Write failing tests:**
  - `TestParseID`: `"1700000000000-3"` → `{1700000000000,3}`; `"5"` → `{5,0}`; `"a-1"`, `"1-"`, `""` → error; `String()` round-trips.
  - `TestIDNext`: `{5,3}.Next()=={5,4}`; `{5,math.MaxUint64}.Next()=={6,0}`.
  - `TestParseVersion`: `"8.2.0"` AtLeast(8,2,0) true; `"8.1.9"` false; `"7.2.4-valkey"` → 7.2.4; `"7"` → 7.0.0; `"x"` error.
  - `TestSafeCutoff`:
    - no groups → `ok==false`.
    - age `{100,0}`, one group lastDelivered `{200,0}` nothing pending → `{100,0}`.
    - age `{300,0}`, group lastDelivered `{200,5}` nothing pending → `{200,6}`.
    - age `{300,0}`, group lastDelivered `{250,0}`, oldest pending `{150,2}` → `{150,2}`.
    - two groups, second lastDelivered `{0,0}` (created at "0", read nothing) → `{0,1}` (trims nothing).
  - `TestParseTaskMessage`: `{"task_id":"abc","task_name":"mod:fn","labels":{},"args":[],"kwargs":{}}` → `"abc","mod:fn",true`; `"\x80\x04pickle"`, `"{}"`, `"[1]"` → `ok==false`.
- [ ] **Step 2: Run** `go test ./internal/redisadmin/` → FAIL.
- [ ] **Step 3: Implement.** `SafeCutoff`: start from `ageCutoff`; for each group take `min(cutoff, LastDelivered.Next())` and, if set, `min(cutoff, *OldestPending)`. `ParseTaskMessage` requires a non-empty `task_id` string.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(redisadmin): stream id, version and safe-cutoff helpers`.

### Task 3: Connection, server info, and the e2e harness

**Files:**
- Create: `internal/redisadmin/client.go`, `internal/redisadmin/e2e_test.go`, `e2e/docker-compose.yml`
- Modify: `go.mod`/`go.sum` (`go get github.com/redis/go-redis/v9@latest`), `Makefile` (`e2e` target)

**Interfaces:**
- Consumes: `config.Redis` (Task 1), `ParseVersion` (Task 2).
- Produces:
  ```go
  type Client struct{ /* redis.UniversalClient, cfg */ }
  func Connect(cfg config.Redis) (*Client, error)   // builds the client only, no I/O
  func (c *Client) Close() error
  func (c *Client) Redis() redis.UniversalClient     // for Tasks 4 and 6
  type ServerInfo struct {
      Version     Version
      Supported   bool          // Version ≥ 7.0.0
      Mode        string        // "standalone" | "sentinel"
      MasterName  string
      DB          int
      UsedMemory  int64
      MaxMemory   int64         // 0 = no limit
      Clients     int
      Uptime      time.Duration
  }
  func (c *Client) ServerInfo(ctx context.Context) (ServerInfo, error)
  var ErrUnsupportedVersion = errors.New("Redis 7.0 or later is required")
  ```

- [ ] **Step 1: Write `e2e/docker-compose.yml`** with services on fixed host ports: `redis82` (`redis:8.2`, 6382), `redis70` (`redis:7.0`, 6370), `redis62` (`redis:6.2`, 6362), and a Sentinel set: `sentinel-master` (`redis:8.2`), `sentinel-replica` (replicaof master), `sentinel` (`redis:8.2 redis-sentinel`, monitoring `mymaster`, quorum 1, host port 26379). Sentinel must announce an address reachable from the host (`sentinel announce-hostnames` / `replica-announce-ip` set to `127.0.0.1` with host-mapped ports, or run the Go tests in a container on the compose network — builder's choice, document it in the file header).
- [ ] **Step 2: Add Makefile target** `e2e: docker compose -f e2e/docker-compose.yml up -d --wait && go test -race -tags e2e ./... ; status=$$?; docker compose -f e2e/docker-compose.yml down -v; exit $$status`.
- [ ] **Step 3: Write failing e2e tests** (`//go:build e2e`, addresses overridable by env `E2E_REDIS82` etc.):
  - `TestConnectStandaloneDB`: `Connect({URL:"redis://localhost:6382/3"})`; `SET k v` via `Redis()`; a raw client on db 3 sees `k`, db 0 does not; `ServerInfo` reports `DB==3`, `Mode=="standalone"`, `Supported==true`.
  - `TestConnectDatabaseOverride`: URL `/3` + `DB:5` → writes land in db 5.
  - `TestConnectSentinelDB`: `Connect({Sentinels:["localhost:26379"], MasterName:"mymaster", DB:4})` → write visible on the master's db 4, not db 0; `Mode=="sentinel"`, `MasterName=="mymaster"`.
  - `TestServerInfoOldRedis`: on 6.2 → `Supported==false`, `Version.Major==6`.
- [ ] **Step 4: Run** `make e2e` → FAIL (undefined).
- [ ] **Step 5: Implement.** Standalone: `redis.ParseURL(cfg.URL)` then override `DB`. Sentinel: `redis.NewFailoverClient(&redis.FailoverOptions{MasterName, SentinelAddrs, DB, Username, Password, TLSConfig})`, credentials/TLS taken from `redis.ParseURL(cfg.URL)` when URL is set. `ServerInfo` parses `INFO server`, `INFO memory`, `INFO clients` (one `INFO` call with those sections) and is called fresh by every caller (that is how a failover onto another version is picked up; no caching).
- [ ] **Step 6: Run** `make e2e` and `go test ./...` → PASS.
- [ ] **Step 7: Commit** `feat(redisadmin): connect to Redis or Sentinel on the configured db`.

### Task 4: Reading — scan, overview, stream detail

**Files:**
- Create: `internal/redisadmin/read.go`
- Test: `internal/redisadmin/e2e_test.go` (extend)

**Interfaces:**
- Consumes: `Client` (Task 3), `ID`, `GroupState`, `SafeCutoff`, `ParseTaskMessage` (Task 2).
- Produces:
  ```go
  type Queue struct {
      Key           string
      Type          string     // "stream" | "list"
      Length        int64
      Groups        int        // 0 for lists
      Pending       int64      // 0 for lists
      OldestPending *time.Time // entry time of the oldest pending ID across groups
      MemoryBytes   int64
  }
  type Overview struct {
      Server ServerInfo
      Queues []Queue // sorted by Key
  }
  type Consumer struct{ Name string; Pending int64; Idle time.Duration }
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
      Key               string
      Length            int64
      FirstEntry        *time.Time
      LastEntry         *time.Time
      Trimmable         int64 // entries below the safe cutoff, counted up to TrimmableCap
      TrimmableCapped   bool
      Groups            []Group
      OldestPending     []PendingMessage // at most 20, oldest first
  }
  const TrimmableCap = 1000
  type Reader struct{ /* client, key cache */ }
  func NewReader(c *Client, keepAcked func(ctx context.Context) (time.Duration, bool)) *Reader
  func (r *Reader) Overview(ctx context.Context) (Overview, error)
  func (r *Reader) Stream(ctx context.Context, key string) (StreamDetail, error)
  func (r *Reader) groupStates(ctx context.Context, key string) ([]GroupState, error) // reused by the Cleaner
  var ErrNotStream = errors.New("not a stream")
  ```

- [ ] **Step 1: Write failing e2e tests** on `redis70` and `redis82` (run each as a subtest per server):
  - `TestOverviewFindsQueues`: create stream `q1` with 3 entries, group `g` read 2 (one acked, one pending), list `l1` with 2 items, string key `res:1`. Overview lists exactly `l1` (list, len 2) and `q1` (stream, len 3, groups 1, pending 1, `OldestPending` == entry time of the pending ID); no `res:1`. `Server.Supported` true.
  - `TestOverviewScanCache`: after an Overview, a new stream appears only after the cache TTL (inject TTL via an unexported field set to 50 ms in the test).
  - `TestStreamDetail`: on the same `q1`, `Groups[0].Pending==1`, one consumer with Pending 1, `OldestPending[0].TaskID` set when the entry's `data` holds a taskiq JSON message, `Trimmable==1` with keepAcked 0.
  - `TestStreamDetailForeignEntries`: entries with field `foo` only, or pickled bytes in `data` → `TaskID==""`, no error.
  - `TestStreamDetailNotStream`: `Stream(ctx,"l1")` → `errors.Is(err, ErrNotStream)`.
- [ ] **Step 2: Run** `make e2e` → FAIL.
- [ ] **Step 3: Implement.** Scan: `SCAN cursor MATCH * COUNT 1000 TYPE stream` and `… TYPE list`, cached 30 s. Per stream: `XINFO STREAM`, `XINFO GROUPS`, `XPENDING key group` (summary) per group; per key `MEMORY USAGE`; pipeline per key. Detail: `XINFO STREAM FULL` is **not** used (cost); use `XINFO CONSUMERS` per group, `XPENDING key group - + 20` per group merged and sorted, then `XRANGE id id` for each to read `data`. Trimmable: `SafeCutoff(IDAt(now-keep), groupStates)` then `XRANGE key - (cutoff COUNT 1001` → count (capped). If keep-age is unset, use 0 for this number. If `!Server.Supported` return the `Overview` with `Server` filled and `Queues` nil, and `Stream` returns `ErrUnsupportedVersion`.
- [ ] **Step 4: Run** `make e2e` → PASS.
- [ ] **Step 5: Commit** `feat(redisadmin): overview and stream detail`.

### Task 5: Cleanup settings

**Files:**
- Modify: `internal/store/store.go` (`Settings`, `Validate`), `internal/store/sqlite/sqlite.go` (`settingFields`), `internal/store/sqlite/schema.go` (two `INSERT OR IGNORE` rows), `internal/store/storetest/storetest.go` (`testSettings`)

**Interfaces:**
- Produces on `store.Settings`:
  ```go
  RedisTrimIntervalMinutes *int `json:"redis_trim_interval_minutes"` // 1..TTLMaxMinutes
  RedisKeepAckedMinutes    *int `json:"redis_keep_acked_minutes"`    // 0..TTLMaxMinutes
  ```

- [ ] **Step 1: Extend `testSettings`**: save `{RedisTrimIntervalMinutes: ptr(5), RedisKeepAckedMinutes: ptr(0)}` → read back equal; `RedisKeepAckedMinutes: ptr(-1)` → error; `RedisTrimIntervalMinutes: ptr(0)` → error; empty settings → both nil.
- [ ] **Step 2: Run** `go test ./internal/store/...` → FAIL.
- [ ] **Step 3: Implement** (keep-age lower bound 0, interval lower bound `TTLMinMinutes`).
- [ ] **Step 4: Run** `go test ./...` → PASS (also `TestAPISettings` still passes; extend it to PUT and GET the two new keys).
- [ ] **Step 5: Commit** `feat(store): Redis cleanup settings`.

### Task 6: Cleaner

**Files:**
- Create: `internal/redisadmin/clean.go`
- Test: `internal/redisadmin/clean_test.go` (unit, fake trimmer), `internal/redisadmin/e2e_test.go` (extend)

**Interfaces:**
- Consumes: `Reader.groupStates`, `Client.ServerInfo`, `SafeCutoff`, `store.Settings` (Task 5), `maintenance.BatchConfig`.
- Produces:
  ```go
  type SettingsSource interface{ GetSettings(ctx context.Context) (store.Settings, error) }
  type RunSummary struct {
      Started time.Time
      Took    time.Duration
      Mode    string // "XTRIM ACKED" | "safe cut-off (Redis < 8.2)"
      Streams int    // streams that had at least one group
      Removed int64
      Err     string
  }
  type CleanStatus struct {
      Running  bool
      Seq      int         // increments at the end of every run
      Last     *RunSummary // nil before the first run since start
      Interval *int        // copies of the current settings, for the page
      KeepAcked *int
  }
  type Cleaner struct{ Tick time.Duration /* default time.Minute */; Now func() time.Time; OnDone func(); /* … */ }
  func NewCleaner(r *Reader, s SettingsSource, batch maintenance.BatchConfig, log *slog.Logger) *Cleaner
  func (c *Cleaner) Run(ctx context.Context)
  func (c *Cleaner) Trigger() bool // false (no-op) when keep-age is unset or a run is pending/running
  func (c *Cleaner) RunOnce(ctx context.Context) (RunSummary, error)
  func (c *Cleaner) Status(ctx context.Context) CleanStatus
  ```

Run loop: wakes every `Tick` or on `Trigger`; a tick runs when `RedisTrimIntervalMinutes` is set, `RedisKeepAckedMinutes` is set, and `Now()-lastStart ≥ interval`; a trigger runs when keep-age is set. Runs never overlap (single goroutine). A failed `ServerInfo` (Redis down) records `Err` in the summary, logs at error level, and waits for the next tick. Unsupported version → `Err: ErrUnsupportedVersion.Error()`, nothing trimmed.

Per stream (skip streams with no group), bounded steps, same algorithm on both versions:

```
cursor := "-"
loop:
    if v < 8.2: target = SafeCutoff(IDAt(now-keep), groupStates(key))   // re-read every step: picks up new groups
    else:       target = IDAt(now-keep)
    entries := XRANGE key <cursor exclusive "(id" or "-"> "(target" COUNT batch.Size
    if none: break
    last := entries[len-1].ID
    removed += XTRIM key MINID last.Next() [ACKED if v ≥ 8.2]
    if len(entries) < batch.Size: break
    cursor = last; sleep batch.Pause (ctx-aware)
```

Log one info line per stream with removals: `"trimmed stream", "key", k, "removed", n, "mode", mode`.

- [ ] **Step 1: Write failing unit tests** in `clean_test.go` using a fake settings source and `Tick: 10ms` (the trimming itself is behind an unexported interface so the loop can be tested without Redis):
  - `TestCleanerScheduleOff`: interval nil → no run after 5 ticks.
  - `TestCleanerScheduleInterval`: interval 1 min, `Now` advanced 30 s per tick → runs at the 1st tick and the 3rd.
  - `TestCleanerTriggerNeedsKeepAge`: keep nil → `Trigger()==false`, no run.
  - `TestCleanerTriggerWhileRunning`: fake trim blocks on a channel; `Trigger()` twice → second returns false; `Status().Running==true`; release → exactly one run, `Seq==1`.
- [ ] **Step 2: Write failing e2e tests** (each on `redis70` and `redis82` unless noted):
  - `TestCleanerKeepsUnacked`: stream with 10 entries; group `g` reads 6, acks entries 1–3 and 5; keep 0 → after `RunOnce`: on 8.2 entries 4,6–10 remain (4 and 6 pending, 7–10 undelivered); on 7.0 entries 4–10 remain (stops at oldest pending). `Removed` 4 (8.2) / 3 (7.0). `Mode` matches. (If Redis 8.2 turns out to stop `ACKED` trimming at the first unacknowledged entry instead of skipping it, the 8.2 expectation becomes the 7.0 one; check the real behavior first and record it in a comment next to the test.)
  - `TestCleanerKeepAge`: all entries acked, half older than keep (entries added with explicit IDs from `IDAt(now-2h)`), keep 60 → only the old half removed.
  - `TestCleanerNoGroup`: stream without group untouched, `Streams==0`.
  - `TestCleanerTwoGroups`: second group has read nothing → nothing removed on 7.0; on 8.2 nothing removed either.
  - `TestCleanerLargeStreamInSteps` (redis82 and redis70): 200 000 entries all acked, batch 10 000 → all removed, and a concurrent `PING` loop never waits > 200 ms during the run.
  - `TestCleanerSentinelDB`: through Sentinel on db 4 → db 4's stream trimmed, an identical stream in db 0 untouched.
  - `TestCleanerUnsupported` (redis62): `RunOnce` returns `ErrUnsupportedVersion`, stream untouched.
  - `TestCleanerRedisDown`: client pointed at a closed port → `RunOnce` returns an error, `Status().Last.Err != ""`.
- [ ] **Step 3: Run** `go test ./internal/redisadmin/` and `make e2e` → FAIL.
- [ ] **Step 4: Implement.**
- [ ] **Step 5: Run** both → PASS.
- [ ] **Step 6: Commit** `feat(redisadmin): trim acknowledged stream entries`.

### Task 7: Web pages, settings section, Clean now

**Files:**
- Create: `internal/web/redis.go`, `internal/web/templates/redis.html`, `internal/web/templates/redis_stream.html`, `internal/web/redis_test.go`
- Modify: `internal/web/server.go` (Options/NewServer/routes), `internal/web/view.go` (`parsePages` gets `"redis"` and `"redis-stream"`; funcs `humanBytes`, `streamURL`), `internal/web/ui.go` (`layoutData.Redis bool`; settings page + save handle the two new fields), `internal/web/templates/layout.html` (nav entry before Settings, only when `.Redis`), `internal/web/templates/settings.html` (a "Redis streams" section only when `.Redis`), `internal/web/static/app.js` (toast event), `internal/web/static/app.css`, `internal/web/web_test.go` (`newEnv` passes nil Redis)

**Interfaces:**
- Consumes: `redisadmin.Overview`, `StreamDetail`, `CleanStatus`, `ErrNotStream`, `ErrUnsupportedVersion`.
- Produces:
  ```go
  // in package web
  type RedisReader interface {
      Overview(ctx context.Context) (redisadmin.Overview, error)
      Stream(ctx context.Context, key string) (redisadmin.StreamDetail, error)
  }
  type RedisCleaner interface {
      Trigger() bool
      Status(ctx context.Context) redisadmin.CleanStatus
  }
  // Options gains: Redis RedisReader; RedisCleaner RedisCleaner (both nil = section disabled)
  ```
- Routes (registered only when enabled): `GET /redis`, `GET /redis/streams/{key}` (key is `url.PathEscape`d, read with `r.PathValue`), `GET /redis/cleanup` (the box alone), `POST /redis/cleanup` (Clean now).

Page behavior:
- Overview renders the mockup `redis-page-v2.html`: server strip, queues table (key links to the stream page for streams), cleanup box with **Clean now** (disabled with title "Set a keep-acked age in Settings first" when keep-age is nil; shows "Running…" disabled while `Running`). Oldest-pending cell and pending count get class `warn` when the oldest pending is older than `StaleQueuedMinutes` (if set).
- Body block `redis-body` refreshes with `hx-get` `hx-trigger="every 5s [document.visibilityState==='visible']"`, rendered alone when `htmxTarget(r)=="redis-body"`. Same for the stream page (`redis-stream-body`).
- Cleanup box `#redis-cleanup` polls `GET /redis/cleanup?seen=<Seq>` every 1 s while running. When the response's `Seq > seen` and the request had `running=1`, set header `HX-Trigger: {"toast":"Cleanup removed N entries"}` (or `"Cleanup failed: <err>"`). `app.js`: `document.body.addEventListener("toast", e => toast(e.detail.value))`.
- `POST /redis/cleanup` calls `Trigger()` and returns the box (running state when it returned true). No JS: redirect to `/redis`.
- Reader error → page renders with an error banner (`alert alert-error`) instead of the tables, still polling. `ErrUnsupportedVersion` → banner text exactly "Redis 7.0 or later is required" plus the detected version. `ErrNotStream` / missing key → 404.
- Settings page "Redis streams" section: "Trim every (minutes)" (`redis_trim_interval_minutes`, empty = off) and "Keep acknowledged entries for (minutes)" (`redis_keep_acked_minutes`, 0 allowed, empty = unset), with a note: "Every stream in this Redis database with a consumer group is trimmed. Only entries acknowledged by every group are removed." Saving does not trigger a run; the cleaner reads the settings on its next tick.

- [ ] **Step 1: Write failing tests** in `redis_test.go` with fakes for both interfaces and a helper `newRedisEnv(t, fakeReader, fakeCleaner)`:
  - `TestRedisDisabled`: nil Redis → `GET /redis` 404, `/tasks` HTML has no `href="/redis"`, settings page has no `redis_trim_interval_minutes`.
  - `TestRedisOverview`: fake with one stream `taskiq:high` (pending 640, oldest 22 min, StaleQueued 5 saved) and one list → body contains `taskiq:high`, `href="/redis/streams/taskiq:high"`, `640` inside an element with class `warn`, `list`, server version, `db 3`.
  - `TestRedisPageShowsReaderError`: reader returns `errors.New("dial tcp: refused")` → 200, contains `alert-error` and `refused`, still contains the polling `hx-trigger`.
  - `TestRedisUnsupported`: reader returns overview with `Supported:false` → contains `Redis 7.0 or later is required`.
  - `TestRedisStreamKeyEscaping`: key `a/b c%{x}` → overview link is `/redis/streams/a%2Fb%20c%25%7Bx%7D`; GET of that path makes the fake receive exactly `a/b c%{x}`.
  - `TestRedisStreamNotFound`: fake returns `ErrNotStream` → 404.
  - `TestRedisCleanNow`: POST with `HX-Request: true`, `HX-Target: redis-cleanup` → fake `Trigger` called once, body contains `Running…`; a POST with `Origin: https://evil.example` and `Sec-Fetch-Site: cross-site` → 403 and no trigger.
  - `TestRedisCleanupToast`: GET `/redis/cleanup?seen=1&running=1` with fake Seq 2, Removed 42 → header `HX-Trigger` contains `Cleanup removed 42 entries`; with `seen=2` → no header.
  - `TestRedisSettingsSave`: POST `/settings` with `redis_keep_acked_minutes=0`, `redis_trim_interval_minutes=5` → stored values 0 and 5; `redis_keep_acked_minutes=-1` → form error mentioning "Keep acknowledged entries".
- [ ] **Step 2: Run** `go test ./internal/web/` → FAIL.
- [ ] **Step 3: Implement** templates and handlers, matching the existing card/table/badge classes in `app.css`.
- [ ] **Step 4: Run** `go test -race ./...` and `node --check internal/web/static/app.js` → PASS.
- [ ] **Step 5: Commit** `feat(web): Redis section`.

### Task 8: Wiring, real-worker e2e, docs, CI

**Files:**
- Modify: `cmd/taskiq-admin/main.go`, `README.md`, `env-example`, `.github/workflows/ci.yml`, `e2e/docker-compose.yml`
- Create: `e2e/worker/Dockerfile`, `e2e/worker/worker.py`, `internal/web/e2e_test.go` (`//go:build e2e`)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Wire `main.go`.** When `cfg.Redis != nil`: `redisadmin.Connect`, `NewReader(client, keepAcked from store settings)`, `NewCleaner(reader, cachedStore, batch, log)`, `bg.Go(func(){ cleaner.Run(bgCtx) })`, pass both to `web.Options`, `defer client.Close()`. Log `"redis section enabled", "mode", …, "db", …` at startup (never the URL: it may hold a password). `Connect` errors are startup errors; nothing does I/O at startup.
- [ ] **Step 2: Real worker.** `e2e/worker`: Python 3.12 image with `taskiq`, `taskiq-redis`, `aiohttp`; `worker.py` defines `RedisStreamBroker(url=$REDIS_URL, queue_name="taskiq")` with `TaskiqAdminMiddleware(url=$ADMIN_URL, api_token="e2e")`, tasks `ok()` and `slow()` (sleeps 30 s), and a `__main__` that kicks 20 `ok` and 2 `slow`. Compose service `worker` (`taskiq worker worker:broker`) on `redis82` db 2, plus a one-shot `kicker`.
- [ ] **Step 3: Write failing `TestRealWorkerEndToEnd`** in `internal/web/e2e_test.go`: start the real `web.Server` (sqlite in temp dir, Redis section on `redis82` db 2, keep 0), wait for the 20 `ok` tasks to be reported; `GET /redis` lists `taskiq`; `GET /redis/streams/taskiq` shows the 2 `slow` entries as pending with `TaskName` ending in `slow` and a link `/tasks/<id>`; `POST /redis/cleanup` then poll status → `Removed ≥ 20`, `XLEN taskiq ≥ 2`.
- [ ] **Step 4: Run** `make e2e` → PASS after any fixes.
- [ ] **Step 5: README.** New "Redis" feature bullet; env vars table rows for the four `REDIS_*` variables (the DB precedence rule, Sentinel host ignored, no sentinel auth); a "Redis stream cleanup" note: off by default, every stream with a consumer group in that database is trimmed, only fully acknowledged entries, Redis 7.0+, `XTRIM ACKED` on 8.2+. `env-example`: commented `REDIS_*` lines.
- [ ] **Step 6: CI.** New job `e2e` (needs `test`): checkout, setup-go, `make e2e`. `image` job keeps `needs: test` only.
- [ ] **Step 7: Run** `make lint`, `go test -race ./...`, `make e2e` → all PASS.
- [ ] **Step 8: Commit** `feat: wire the Redis section, real-worker e2e, docs and CI`.
