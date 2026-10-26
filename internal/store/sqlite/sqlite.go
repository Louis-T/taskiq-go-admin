// Package sqlite implements store.Store on SQLite (pure Go driver, no cgo).
//
// Writes go through a single connection: SQLite has one writer at a time
// anyway, and serializing in the pool avoids SQLITE_BUSY storms. Reads use a
// separate pool and, thanks to WAL mode, never block (nor are blocked by) the
// writer.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/louis-t/taskiq-go-admin/internal/store"
	"github.com/louis-t/taskiq-go-admin/internal/task"
)

// Store is the SQLite implementation of store.Store.
type Store struct {
	path   string
	writer *sql.DB
	reader *sql.DB
}

var _ store.Store = (*Store)(nil)

// Open opens (creating if needed) the database at path.
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" || path == ":memory:" || strings.HasPrefix(path, "file:") {
		return nil, errors.New("sqlite: a plain file path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("sqlite: create directory: %w", err)
	}

	pragmas := "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=journal_size_limit(6144000)"
	writer, err := sql.Open("sqlite", path+pragmas+"&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	writer.SetMaxOpenConns(1)
	writer.SetMaxIdleConns(1)
	writer.SetConnMaxLifetime(0)

	reader, err := sql.Open("sqlite", path+pragmas)
	if err != nil {
		writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(8)
	reader.SetMaxIdleConns(8)

	s := &Store{path: path, writer: writer, reader: reader}
	if err := createSchema(ctx, writer); err != nil {
		s.Close()
		return nil, fmt.Errorf("sqlite: create schema: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error {
	_, _ = s.writer.Exec(`PRAGMA optimize`)
	return errors.Join(s.reader.Close(), s.writer.Close())
}

const taskColumns = `id, name, state, error, worker, execution_time, queued_at, started_at, finished_at, args, kwargs, labels, return_value, attempt`

func (s *Store) ApplyEvents(ctx context.Context, events []task.Event) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, e := range events {
		cur, err := getTask(ctx, tx, e.TaskID)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		next := task.Apply(cur, e)
		if err := upsertTask(ctx, tx, &next); err != nil {
			return fmt.Errorf("upsert task %q: %w", e.TaskID, err)
		}
	}
	return tx.Commit()
}

func upsertTask(ctx context.Context, tx *sql.Tx, t *task.Task) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO taskiq_admin_tasks (`+taskColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (id) DO UPDATE SET
			name = excluded.name, state = excluded.state, error = excluded.error,
			worker = excluded.worker, execution_time = excluded.execution_time,
			queued_at = excluded.queued_at, started_at = excluded.started_at,
			finished_at = excluded.finished_at, args = excluded.args,
			kwargs = excluded.kwargs, labels = excluded.labels,
			return_value = excluded.return_value, attempt = excluded.attempt`,
		t.ID, t.Name, string(t.State), t.Error, t.Worker, t.ExecutionTime,
		toMs(t.QueuedAt), toMsPtr(t.StartedAt), toMsPtr(t.FinishedAt),
		jsonArg(t.Args), jsonArg(t.Kwargs), jsonArg(t.Labels), jsonArg(t.ReturnValue),
		t.Attempt,
	)
	return err
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getTask(ctx context.Context, q queryer, id string) (*task.Task, error) {
	row := q.QueryRowContext(ctx, `SELECT `+taskColumns+` FROM taskiq_admin_tasks WHERE id = ?`, id)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return t, err
}

func (s *Store) GetTask(ctx context.Context, id string) (*task.Task, error) {
	return getTask(ctx, s.reader, id)
}

type scanner interface{ Scan(dest ...any) error }

func scanTask(sc scanner) (*task.Task, error) {
	var (
		t                         task.Task
		state                     string
		queuedAt                  int64
		startedAt, finishedAt     sql.NullInt64
		args, kwargs, labels, ret sql.NullString
	)
	err := sc.Scan(&t.ID, &t.Name, &state, &t.Error, &t.Worker, &t.ExecutionTime,
		&queuedAt, &startedAt, &finishedAt, &args, &kwargs, &labels, &ret, &t.Attempt)
	if err != nil {
		return nil, err
	}
	t.State = task.State(state)
	t.QueuedAt = fromMs(queuedAt)
	t.StartedAt = fromMsNull(startedAt)
	t.FinishedAt = fromMsNull(finishedAt)
	t.Args = jsonCol(args)
	t.Kwargs = jsonCol(kwargs)
	t.Labels = jsonCol(labels)
	t.ReturnValue = jsonCol(ret)
	return &t, nil
}

// where renders the filter as a SQL condition (without the WHERE keyword).
func where(f store.Filter) (string, []any) {
	var (
		conds []string
		args  []any
	)
	if f.State != "" {
		conds = append(conds, `state = ?`)
		args = append(args, string(f.State))
	}
	if name := strings.TrimSpace(f.Name); name != "" {
		if len([]rune(name)) < 3 {
			// The trigram tokenizer cannot match terms shorter than 3 characters.
			conds = append(conds, `lower(name) LIKE ? ESCAPE '\'`)
			args = append(args, likePattern(name))
		} else {
			conds = append(conds, `rowid IN (SELECT rowid FROM taskiq_admin_tasks_fts WHERE name MATCH ?)`)
			args = append(args, `"`+strings.ReplaceAll(name, `"`, `""`)+`"`)
		}
	}
	if f.TaskName != "" {
		conds = append(conds, `name = ?`)
		args = append(args, f.TaskName)
	}
	if f.Stale != nil {
		var ors []string
		if !f.Stale.RunningStartedBefore.IsZero() {
			ors = append(ors, `(state = 'running' AND started_at < ?)`)
			args = append(args, toMs(f.Stale.RunningStartedBefore))
		}
		if !f.Stale.QueuedBefore.IsZero() {
			ors = append(ors, `(state = 'queued' AND queued_at < ?)`)
			args = append(args, toMs(f.Stale.QueuedBefore))
		}
		if len(ors) == 0 {
			ors = []string{"0 = 1"}
		}
		conds = append(conds, "("+strings.Join(ors, " OR ")+")")
	}
	if q := strings.TrimSpace(f.ArgsSearch); q != "" {
		conds = append(conds, `(lower(coalesce(args, '')) LIKE ? ESCAPE '\' OR lower(coalesce(kwargs, '')) LIKE ? ESCAPE '\')`)
		p := likePattern(q)
		args = append(args, p, p)
	}
	if !f.From.IsZero() {
		conds = append(conds, `queued_at >= ?`)
		args = append(args, toMs(f.From))
	}
	if !f.To.IsZero() {
		// Round the exclusive bound up: a task stored at the same millisecond
		// as an unrounded "now" must be included.
		conds = append(conds, `queued_at < ?`)
		args = append(args, toMs(f.To.Add(time.Millisecond-time.Nanosecond)))
	}
	if len(conds) == 0 {
		return "1 = 1", nil
	}
	return strings.Join(conds, " AND "), args
}

func likePattern(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + r.Replace(strings.ToLower(s)) + "%"
}

func (s *Store) ListTasks(ctx context.Context, q store.ListQuery) (*store.ListResult, error) {
	if q.Limit <= 0 || q.Limit > store.MaxListLimit {
		return nil, fmt.Errorf("limit must be between 1 and %d", store.MaxListLimit)
	}
	cond, args := where(q.Filter)
	isNext := q.Direction != store.Prev
	if q.Cursor != nil {
		op := "<"
		if !isNext {
			op = ">"
		}
		cond += ` AND (queued_at, id) ` + op + ` (?, ?)`
		args = append(args, toMs(q.Cursor.QueuedAt), q.Cursor.ID)
	}
	order := "DESC"
	if !isNext {
		order = "ASC"
	}
	args = append(args, q.Limit+1)
	rows, err := s.reader.QueryContext(ctx, `SELECT `+taskColumns+` FROM taskiq_admin_tasks WHERE `+cond+
		` ORDER BY queued_at `+order+`, id `+order+` LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []task.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return store.Paginate(tasks, q), nil
}

func (s *Store) Histogram(ctx context.Context, q store.HistogramQuery) ([]store.HistogramRow, error) {
	bucket := q.Bucket.Milliseconds()
	if bucket <= 0 {
		return nil, errors.New("histogram bucket must be positive")
	}
	cond, args := where(q.Filter)
	off := q.Offset.Milliseconds()
	rows, err := s.reader.QueryContext(ctx, `SELECT ((queued_at + ?) / ?) * ? - ? AS bucket, state, COUNT(*)
		FROM taskiq_admin_tasks WHERE `+cond+` GROUP BY bucket, state ORDER BY bucket`,
		append([]any{off, bucket, bucket, off}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []store.HistogramRow
	for rows.Next() {
		var (
			start int64
			state string
			r     store.HistogramRow
		)
		if err := rows.Scan(&start, &state, &r.Count); err != nil {
			return nil, err
		}
		r.Start = fromMs(start)
		r.State = task.State(state)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Count(ctx context.Context, f store.Filter) (int64, error) {
	cond, args := where(f)
	var n int64
	err := s.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM taskiq_admin_tasks WHERE `+cond, args...).Scan(&n)
	return n, err
}

const typeSummaryColumns = `name, COUNT(*),
	SUM(state = 'queued'), SUM(state = 'running'), SUM(state = 'success'),
	SUM(state = 'failure'), SUM(state = 'abandoned'),
	AVG(execution_time), MAX(execution_time), MAX(queued_at)`

func scanTypeSummary(sc scanner) (store.TypeSummary, error) {
	var (
		t                                  store.TypeSummary
		queued, running, ok, failed, aband int64
		lastQueued                         int64
	)
	err := sc.Scan(&t.Name, &t.Total, &queued, &running, &ok, &failed, &aband, &t.AvgExec, &t.MaxExec, &lastQueued)
	t.Counts = map[task.State]int64{task.Queued: queued, task.Running: running, task.Success: ok, task.Failure: failed, task.Abandoned: aband}
	t.LastQueuedAt = fromMs(lastQueued)
	return t, err
}

func (s *Store) TaskTypes(ctx context.Context, from, to time.Time) ([]store.TypeSummary, error) {
	cond, args := where(store.Filter{From: from, To: to})
	rows, err := s.reader.QueryContext(ctx, `SELECT `+typeSummaryColumns+` FROM taskiq_admin_tasks
		WHERE `+cond+` AND name != '' GROUP BY name ORDER BY COUNT(*) DESC, name`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.TypeSummary
	for rows.Next() {
		t, err := scanTypeSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) TypeStats(ctx context.Context, name string, from, to time.Time) (*store.TypeStats, error) {
	cond, args := where(store.Filter{TaskName: name, From: from, To: to})
	sum, err := scanTypeSummary(s.reader.QueryRowContext(ctx,
		`SELECT `+typeSummaryColumns+` FROM taskiq_admin_tasks WHERE `+cond+` GROUP BY name`, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return &store.TypeStats{TypeSummary: store.TypeSummary{Name: name, Counts: map[task.State]int64{}}}, nil
	}
	if err != nil {
		return nil, err
	}
	st := &store.TypeStats{TypeSummary: sum}

	// Percentiles: count the values, then read the one at the matching rank.
	percentiles := func(expr string) (p50, p95 *float64, err error) {
		c := cond + ` AND ` + expr + ` IS NOT NULL`
		var n int64
		if err := s.reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM taskiq_admin_tasks WHERE `+c, args...).Scan(&n); err != nil || n == 0 {
			return nil, nil, err
		}
		at := func(q float64) (*float64, error) {
			var v float64
			err := s.reader.QueryRowContext(ctx, `SELECT `+expr+` FROM taskiq_admin_tasks WHERE `+c+
				` ORDER BY `+expr+` LIMIT 1 OFFSET ?`, append(args, int64(q*float64(n-1)))...).Scan(&v)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil // rows purged since the count
			}
			if err != nil {
				return nil, err
			}
			return &v, nil
		}
		if p50, err = at(0.50); err != nil {
			return nil, nil, err
		}
		p95, err = at(0.95)
		return p50, p95, err
	}
	if st.P50Exec, st.P95Exec, err = percentiles(`execution_time`); err != nil {
		return nil, err
	}
	w50, w95, err := percentiles(`(started_at - queued_at)`)
	if err != nil {
		return nil, err
	}
	ms := func(v *float64) *time.Duration {
		if v == nil {
			return nil
		}
		d := time.Duration(max(*v, 0)) * time.Millisecond
		return &d
	}
	st.P50Wait, st.P95Wait = ms(w50), ms(w95)
	return st, nil
}

func (s *Store) DeleteQueuedBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	res, err := s.writer.ExecContext(ctx, `DELETE FROM taskiq_admin_tasks WHERE rowid IN (
		SELECT rowid FROM taskiq_admin_tasks WHERE queued_at < ? ORDER BY queued_at LIMIT ?)`,
		toMs(cutoff), limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) AbandonRunning(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	res, err := s.writer.ExecContext(ctx, `UPDATE taskiq_admin_tasks SET state = 'abandoned' WHERE rowid IN (
		SELECT rowid FROM taskiq_admin_tasks WHERE state = 'running' AND (started_at IS NULL OR started_at < ?) LIMIT ?)`,
		toMs(cutoff), limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// settingFields maps setting keys to their field. Values are stored as
// text, NULL meaning disabled.
func settingFields(set *store.Settings) map[string]**int {
	return map[string]**int{
		"delete_old_ttl_minutes": &set.DeleteOldTTLMinutes,
		"stale_running_minutes":  &set.StaleRunningMinutes,
		"stale_queued_minutes":   &set.StaleQueuedMinutes,
	}
}

func (s *Store) GetSettings(ctx context.Context) (store.Settings, error) {
	var out store.Settings
	fields := settingFields(&out)
	rows, err := s.reader.QueryContext(ctx, `SELECT key, value FROM taskiq_admin_settings`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			key   string
			value sql.NullString
		)
		if err := rows.Scan(&key, &value); err != nil {
			return out, err
		}
		field, ok := fields[key]
		if !ok || !value.Valid || value.String == "" {
			continue
		}
		n, err := strconv.Atoi(value.String)
		if err != nil {
			return out, fmt.Errorf("invalid setting %s=%q: %w", key, value.String, err)
		}
		*field = &n
	}
	return out, rows.Err()
}

func (s *Store) SaveSettings(ctx context.Context, set store.Settings) error {
	if err := set.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, field := range settingFields(&set) {
		var value any
		if *field != nil {
			value = strconv.Itoa(**field)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO taskiq_admin_settings (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Backup snapshots the database with VACUUM INTO (consistent, and run on the
// read pool so ingestion keeps going) and streams the snapshot to w.
func (s *Store) Backup(ctx context.Context, w io.Writer) error {
	f, err := os.CreateTemp(filepath.Dir(s.path), ".backup-*.db")
	if err != nil {
		return err
	}
	tmp := f.Name()
	f.Close()
	os.Remove(tmp) // VACUUM INTO refuses to overwrite an existing file
	defer os.Remove(tmp)

	if _, err := s.reader.ExecContext(ctx, `VACUUM INTO ?`, tmp); err != nil {
		return fmt.Errorf("vacuum into: %w", err)
	}
	src, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer src.Close()
	_, err = io.Copy(w, src)
	return err
}

func toMs(t time.Time) int64 { return t.UnixMilli() }

func toMsPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixMilli()
}

func fromMs(ms int64) time.Time { return time.UnixMilli(ms).UTC() }

func fromMsNull(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := fromMs(v.Int64)
	return &t
}

func jsonArg(raw json.RawMessage) any {
	if raw == nil {
		return nil
	}
	return string(raw)
}

func jsonCol(v sql.NullString) json.RawMessage {
	if !v.Valid {
		return nil
	}
	if !json.Valid([]byte(v.String)) {
		// Never hand invalid JSON to encoders; expose it as a JSON string.
		b, _ := json.Marshal(v.String)
		return b
	}
	return json.RawMessage(v.String)
}
