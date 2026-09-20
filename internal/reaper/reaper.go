// Package reaper turns expired leases back into deliverable jobs. It is the
// ticker half of the "Expiry" paragraph in docs/semantics.md §4; the SQL half
// is Store.ReapExpiredLeases. The broker runs one Reaper per process. Running
// one per broker replica is fine: the store uses SKIP LOCKED, so two reapers
// never touch the same row and the worst case is wasted ticks.
package reaper

import (
	"context"
	"log/slog"
	"time"
)

// Store is the one store method the reaper needs. Narrow on purpose so tests
// use a fake and so this package does not depend on store.Store growing.
type Store interface {
	ReapExpiredLeases(ctx context.Context, limit int) (int, error)
}

// Reaper sweeps expired leases on a fixed interval. Zero values take the
// defaults noted on each field.
type Reaper struct {
	Store Store
	// Interval between sweeps. Default 1s. Redelivery latency is at worst
	// lease duration + Interval + backoff, so keep this small.
	Interval time.Duration
	// BatchSize is the limit passed to each ReapExpiredLeases call. Default
	// 100. A sweep loops until a call returns fewer than this.
	BatchSize int
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

func (r *Reaper) interval() time.Duration {
	if r.Interval <= 0 {
		return time.Second
	}
	return r.Interval
}

func (r *Reaper) batchSize() int {
	if r.BatchSize <= 0 {
		return 100
	}
	return r.BatchSize
}

func (r *Reaper) logger() *slog.Logger {
	if r.Logger == nil {
		return slog.Default()
	}
	return r.Logger
}

// Run sweeps once immediately, then on every tick until ctx is cancelled, and
// returns ctx.Err(). A store error is logged and the next tick tries again; a
// Postgres blip must not take the reaper down with it.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval())
	defer ticker.Stop()

	for {
		n, err := r.Sweep(ctx)
		switch {
		case err != nil && ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			r.logger().ErrorContext(ctx, "reaper: sweep failed", "err", err, "reaped", n)
		case n > 0:
			r.logger().InfoContext(ctx, "reaper: took back expired leases", "count", n)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Sweep reaps until a batch comes back short and returns the total moved.
// Draining in one sweep, rather than one batch per tick, means a backlog left
// by a broker outage is cleared as fast as Postgres allows instead of at
// BatchSize per Interval.
func (r *Reaper) Sweep(ctx context.Context) (int, error) {
	batch := r.batchSize()
	total := 0
	for {
		n, err := r.Store.ReapExpiredLeases(ctx, batch)
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
