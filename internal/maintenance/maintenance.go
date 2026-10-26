// Package maintenance runs the background housekeeping jobs: the TTL purge
// and the startup "abandoned" sweep.
//
// Both jobs touch potentially millions of rows. A single DELETE/UPDATE would
// hold the database write lock for its whole duration and stall ingestion
// (the middleware's requests would time out), so they work in small batches,
// each one its own short transaction, with a pause in between that lets the
// queued writes go through.
package maintenance

import (
	"context"
	"log/slog"
	"time"

	"github.com/louis-t/taskiq-go-admin/internal/store"
)

// BatchConfig controls how bulk jobs are chunked.
type BatchConfig struct {
	Size  int           // rows per transaction
	Pause time.Duration // sleep between two batches
}

// RunBatches calls step until it affects fewer than cfg.Size rows, sleeping
// cfg.Pause between calls. It returns the total number of affected rows.
func RunBatches(ctx context.Context, cfg BatchConfig, step func(ctx context.Context, limit int) (int64, error)) (int64, error) {
	var total int64
	for {
		n, err := step(ctx, cfg.Size)
		total += n
		if err != nil || n < int64(cfg.Size) {
			return total, err
		}
		if cfg.Pause > 0 {
			timer := time.NewTimer(cfg.Pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return total, ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// AbandonRunning marks the tasks that were running before startedBefore as
// abandoned: the admin panel was down, so it cannot know how they ended.
// A result arriving later still overrides this state.
func AbandonRunning(ctx context.Context, s store.Store, cfg BatchConfig, startedBefore time.Time, log *slog.Logger) int64 {
	n, err := RunBatches(ctx, cfg, func(ctx context.Context, limit int) (int64, error) {
		return s.AbandonRunning(ctx, startedBefore, limit)
	})
	if err != nil {
		log.Error("abandon running tasks", "updated", n, "err", err)
		return n
	}
	if n > 0 {
		log.Info("marked running tasks as abandoned", "count", n)
	}
	return n
}

// Purger deletes tasks older than the TTL configured in the settings.
type Purger struct {
	Store    store.Store
	Interval time.Duration
	Batch    BatchConfig
	Log      *slog.Logger
	Now      func() time.Time
	// OnChange, when set, is called after a run deleted something.
	OnChange func()

	trigger chan struct{}
}

func NewPurger(s store.Store, interval time.Duration, batch BatchConfig, log *slog.Logger) *Purger {
	return &Purger{Store: s, Interval: interval, Batch: batch, Log: log, Now: time.Now, trigger: make(chan struct{}, 1)}
}

// Trigger asks for a run as soon as possible (no-op if one is pending).
func (p *Purger) Trigger() {
	select {
	case p.trigger <- struct{}{}:
	default:
	}
}

// Run purges once at start, then every Interval or on Trigger, until ctx is
// done. Runs never overlap.
func (p *Purger) Run(ctx context.Context) {
	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		if _, err := p.RunOnce(ctx); err != nil && ctx.Err() == nil {
			p.Log.Error("purge old tasks", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.trigger:
		}
	}
}

// RunOnce deletes, batch by batch, every task queued before now - TTL.
func (p *Purger) RunOnce(ctx context.Context) (int64, error) {
	settings, err := p.Store.GetSettings(ctx)
	if err != nil {
		return 0, err
	}
	if settings.DeleteOldTTLMinutes == nil {
		return 0, nil
	}
	ttl := time.Duration(*settings.DeleteOldTTLMinutes) * time.Minute
	cutoff := p.Now().Add(-ttl)
	start := time.Now()
	n, err := RunBatches(ctx, p.Batch, func(ctx context.Context, limit int) (int64, error) {
		return p.Store.DeleteQueuedBefore(ctx, cutoff, limit)
	})
	if n > 0 && p.OnChange != nil {
		p.OnChange()
	}
	if n > 0 {
		p.Log.Info("purged old tasks", "deleted", n, "ttl", ttl.String(), "took", time.Since(start).Round(time.Millisecond).String())
	}
	return n, err
}
