// Package sweep runs hopper's background maintenance loops. The reaper takes
// back expired leases, the promoter makes due jobs leasable, and the janitor
// deletes finished rows past the retention window. Each underlying statement
// uses FOR UPDATE SKIP LOCKED, so loops are safe across broker replicas.
package sweep

import (
	"context"
	"log/slog"
	"time"

	"github.com/rpasco787/hopper/internal/store"
)

// Batch handles at most limit rows and reports how many it handled.
type Batch func(ctx context.Context, limit int) (int, error)

// Store is the narrow part of store.Store needed by the maintenance loops.
type Store interface {
	ReapExpiredLeases(ctx context.Context, limit int) (int, error)
	PromoteDue(ctx context.Context, limit int) (int, error)
	DeleteFinished(ctx context.Context, cutoff time.Time, limit int) (int, error)
}

var _ Store = (store.Store)(nil)

// Loop calls Batch on a ticker and drains full batches within each tick.
type Loop struct {
	Name      string
	Batch     Batch
	Interval  time.Duration
	BatchSize int
	Logger    *slog.Logger
}

func (l *Loop) name() string {
	if l.Name == "" {
		return "sweep"
	}
	return l.Name
}

func (l *Loop) interval() time.Duration {
	if l.Interval <= 0 {
		return time.Second
	}
	return l.Interval
}

func (l *Loop) batchSize() int {
	if l.BatchSize <= 0 {
		return 100
	}
	return l.BatchSize
}

func (l *Loop) logger() *slog.Logger {
	if l.Logger == nil {
		return slog.Default()
	}
	return l.Logger
}

// Run sweeps immediately and then on every tick until ctx is cancelled.
// Store errors are logged and retried on the next tick.
func (l *Loop) Run(ctx context.Context) error {
	ticker := time.NewTicker(l.interval())
	defer ticker.Stop()

	for {
		n, err := l.Once(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			l.logger().ErrorContext(ctx, "sweep failed", "loop", l.name(), "err", err, "handled", n)
		case n > 0:
			l.logger().InfoContext(ctx, "sweep handled rows", "loop", l.name(), "count", n)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Once drains batches until Batch returns fewer rows than the limit.
func (l *Loop) Once(ctx context.Context) (int, error) {
	batch := l.batchSize()
	total := 0
	for {
		n, err := l.Batch(ctx, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < batch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

// Reaper takes back expired leases once per second.
func Reaper(st Store) *Loop {
	return &Loop{Name: "reaper", Batch: st.ReapExpiredLeases, Interval: time.Second}
}

// Promoter makes due scheduled jobs leasable once per second.
func Promoter(st Store) *Loop {
	return &Loop{Name: "promoter", Batch: st.PromoteDue, Interval: time.Second}
}

// Janitor deletes completed and cancelled rows older than window.
func Janitor(st Store, window time.Duration) *Loop {
	return &Loop{
		Name: "janitor",
		Batch: func(ctx context.Context, limit int) (int, error) {
			return st.DeleteFinished(ctx, time.Now().Add(-window), limit)
		},
		Interval:  time.Minute,
		BatchSize: 1000,
	}
}

// All returns the three loops a broker runs.
func All(st Store, retention time.Duration) []*Loop {
	return []*Loop{Reaper(st), Promoter(st), Janitor(st, retention)}
}
