# Redis section: design

Date: 2026-10-28

## Why

`RedisStreamBroker` (taskiq-redis) `XADD`s every task to a stream and `XACK`s it once processed, but never deletes
it: streams grow forever. Its `maxlen` option caps the length, but a burst larger than `maxlen` trims messages that
were never processed. We want taskiq-go-admin to keep those streams small safely (only acknowledged entries go), and,
while it is connected to Redis, to show the queues' backlog, which the middleware reports cannot show.

## Goals

- A new **Redis** section (nav entry, `/redis`), shown only when Redis is configured.
- Connect to Redis **standalone** or through **Sentinel**, on the right logical database.
- **Read**: server facts, every taskiq queue (streams and lists) with its backlog, and a per-stream detail page.
- **Clean**: a recurring job that trims acknowledged entries from streams, plus a "Clean now" button.
- **Unit tests** and **end-to-end tests** against real Redis / Sentinel / a real taskiq worker in Docker.

## Non-goals (for now)

- Redis Cluster (planned later: keep the connection code behind a small interface so it can be added).
- Any write action on messages: no claiming, deleting or re-queueing pending messages. The page is read-only apart
  from "Clean now".
- Reading or decoding result-backend keys or schedules (they are pickled by default). Result keys are plain strings
  with their own expiry (`result_ex_time`), so they are not part of the forever-growing problem (decided: streams only).
- Authentication or TLS to the sentinels themselves (decided: sentinels have no auth). Credentials and TLS in
  `REDIS_URL` apply to the master only; the README says so.
- Redis older than 7.0 (our call): the page shows a banner "Redis 7.0 or later is required" and the cleanup does
  not run. 7.0 is where every command used here exists (`XTRIM MINID` and `XPENDING IDLE` are 6.2, group lag is 7.0).
- Pub/sub queues (nothing is stored in Redis, so there is nothing to show).

## Anti-goals

- **Deleting a message that was not acknowledged**, ever, on any Redis version. This is the one thing that would make
  the feature worse than `maxlen`.
- Blocking Redis for long (a huge first trim, or a full keyspace scan on every page refresh) and slowing the brokers.
- Making the app fail to start, or making `/healthz` fail, because Redis is down. Redis is an optional add-on.

## Connection

Environment variables, the same names as taskiq-redis' test setup without the `TEST_` prefix (decided):

| Variable | Example | Meaning |
|---|---|---|
| `REDIS_URL` | `redis://:pass@host:6379/3`, `rediss://…` | standalone: host, port, user/password, TLS, database (path) |
| `REDIS_SENTINELS` | `s1:26379,s2:26379` | comma-separated `host:port`; when set, connect through Sentinel |
| `REDIS_SENTINEL_MASTER_NAME` | `mymaster` | master name; required when `REDIS_SENTINELS` is set |
| `REDIS_DATABASE` | `3` | logical database; works in **both** modes (decided) |

Rules:

- The section is enabled when `REDIS_URL` or `REDIS_SENTINELS` is set. Neither set: no Redis code runs, no nav entry,
  the app behaves exactly as today.
- Database: `REDIS_DATABASE` if set, otherwise the `/N` path of `REDIS_URL`, otherwise `0`. In Sentinel mode the
  database (and the URL's username/password/TLS) apply to the **master** connection resolved through the sentinels;
  the URL's host is ignored. (Database rule decided; ignoring the host in Sentinel mode is our call.)
- Invalid values (bad URL, non-integer or negative `REDIS_DATABASE`, a malformed sentinel entry, sentinels without a
  master name) stop the app at startup with a clear message, like the other config errors, joined with them.
- Redis being *unreachable* is not a startup error: the Redis page shows a banner with the error, the cleanup job logs
  and retries on its next tick.
- The Redis version is read from `INFO server` at connect and after every reconnect (a Sentinel failover can land on a
  node with another version).

## Discovery and reading

Queues are found by scanning (decided): `SCAN … TYPE stream` and `SCAN … TYPE list`. No queue list in config. Lists
written by taskiq-redis' schedule source (`schedule:cron`, `schedule:time:*`, …) will appear too; they are shown as
any other list (our call: read-only, harmless, no special-casing).

Overview page (`/redis`), as in the approved mockup (`redis-page-v2.html` in the brainstorm session):

- Server strip: version, mode (standalone / sentinel + master name), database, used memory vs `maxmemory`, connected
  clients, uptime (from `INFO`).
- Queues table, one row per stream or list: key, type, length, pending count, age of the oldest pending message,
  number of groups, memory (`MEMORY USAGE`). Pending/groups are "—" for lists. Values over a warning threshold are
  amber (our call: the threshold reuses the stuck-task "queued for too long" setting if set).
- Cleanup box: interval, keep-age, the mode actually used last time (`XTRIM ACKED` or "safe cut-off (Redis < 8.2)"),
  last run time, streams processed, entries removed, duration, and any error. A **Clean now** button (decided) at the
  top right.

Stream page (`/redis/streams/{key}`):

- Length, first and last entry age (from the entry IDs' millisecond part), "trimmable now" (entries the next run
  would remove).
- Per consumer group: last-delivered ID, lag, pending; per consumer: pending, idle.
- The oldest pending messages (`XPENDING … IDLE`-style extended form, a bounded number, e.g. 20): entry ID, task name,
  consumer, idle, delivery count. The task name/id come from the entry's `data` field (the taskiq `BrokerMessage`
  JSON); the name links to `/tasks/{id}` when the admin knows that task id.
- A stream with no consumer group shows "never trimmed".

Our calls: the page refreshes by itself every few seconds while visible (htmx polling, the existing pages use SSE but
Redis changes are not pushed to us). The scanned key list is cached briefly (≈30 s) so refreshes don't rescan the
keyspace; per-key stats are read fresh.

## Cleanup

Recurring job, **off by default**, configured on the Settings page (and `PUT /api/settings`), like the retention TTL:

- `redis_trim_interval_minutes`: how often it runs (nil = off).
- `redis_keep_acked_minutes`: acknowledged entries younger than this are kept (so recent ones can be inspected);
  `0` means trim acknowledged entries right away. (Our call: both runtime settings, not env vars.)

Each run goes over **every** stream in the database that has at least one consumer group, taskiq's or not (decided:
no filtering by pattern or content). A stream with no group is never trimmed: nothing can be proven acknowledged.
The README must warn that every acknowledged entry in the database is subject to trimming. The age cut-off ID is `now - keep_acked` in milliseconds, `<ms>-0`.

- **Redis ≥ 8.2.0**: `XTRIM <key> MINID <age-cutoff> ACKED`. Redis only removes entries acknowledged by every group.
- **Redis < 8.2.0** (decided: stop at the oldest unacknowledged message): the app computes
  `cutoff = min(age-cutoff, oldest pending ID of every group, last-delivered ID + 1 of every group)` from
  `XINFO GROUPS` and `XPENDING` (summary form), then `XTRIM <key> MINID <cutoff>`. Everything strictly below that
  cutoff has been delivered to every group and is pending in none, so it is acknowledged.
  Races: a group created, or a message delivered, between the read and the trim cannot make the cutoff unsafe for
  entries below the old minimums except for a **new group**; the builder must handle that (re-check groups right
  before trimming, or accept and document it).
- Runs never overlap (same shape as `maintenance.Purger`: `Run` loop with a ticker and a `Trigger` channel).
- "Clean now" triggers one run with the current keep-age even when the scheduled job is off; while a run is in
  progress the button is disabled and shows "Running…"; at the end the box updates and a toast gives the count.
  It is a state-changing POST, so it goes through the existing cross-site request check.
- Each run logs, per stream, the number of entries removed; the last run's summary is kept in memory for the page
  (lost on restart; our call).
- Must not block Redis for long: a first run on a 1M-entry stream must be split into bounded steps with a pause in
  between (e.g. advance `MINID` in steps, or approximate trimming with `LIMIT` if it combines with `ACKED`). How is
  the builder's call; the e2e suite must cover a large stream.

## Tests

- Unit (default `go test ./...`, no Redis): config parsing (URL, `REDIS_DATABASE` precedence, sentinel list, errors),
  version comparison, the < 8.2 cutoff computation, stream-ID arithmetic, task-message parsing, page rendering with a
  fake Redis reader.
- End-to-end (decided: Docker), behind a build tag `-tags e2e`, `make e2e`, `docker-compose.e2e.yml`:
  - Redis 8.2+ and Redis 7.0, both cleanup paths: acknowledged entries go, pending and undelivered stay; a stream
    with no group is untouched. A Redis 6.2 target checks the "7.0 required" banner.
  - Sentinel (master + replica + sentinel), on a **non-zero** `REDIS_DATABASE`: the admin reads and trims that
    database and leaves db 0 untouched; also a failover.
  - A small Python taskiq worker using `RedisStreamBroker` (with the admin middleware) producing real acked and
    unacked messages, so the message parsing and the task links are checked against the real format.
- CI: a separate `e2e` job running `make e2e`.

## Left to the builder

The Go Redis client (go-redis v9 is the obvious choice: Sentinel failover client with `DB`, raw `Do` for `XTRIM …
ACKED`), package layout (suggested: `internal/redis` for connection + reading + cleaning, wired from `main.go` and
`web` like the purger), template and CSS details matching the existing pages, README section and env table update.
