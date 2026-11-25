package sharded

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// downFor is how long a shard that failed to answer stays out of choices,
// unless its prober sees it answer first.
const downFor = time.Second

// health says which shards are worth choosing. It steers only choices (the
// unkeyed rotation, the shards a lease visits, the announcement target),
// never a call that must go to one shard (docs/design/sharding.md).
type health struct {
	until []atomic.Int64 // Unix nanoseconds a shard stays down until; 0 or past means up
}

func newHealth(n int) *health { return &health{until: make([]atomic.Int64, n)} }

func (h *health) down(i int, now time.Time) { h.until[i].Store(now.Add(downFor).UnixNano()) }
func (h *health) up(i int)                  { h.until[i].Store(0) }
func (h *health) isUp(i int, now time.Time) bool {
	return h.until[i].Load() <= now.UnixNano()
}

// order lists the shards to try, starting at start and wrapping: the ones
// that are up, or every shard when none is, since health can be wrong.
func (h *health) order(start int, now time.Time) []int {
	n := len(h.until)
	up := make([]int, 0, n)
	for k := range n {
		if i := (start + k) % n; h.isUp(i, now) {
			up = append(up, i)
		}
	}
	if len(up) > 0 {
		return up
	}
	all := make([]int, n)
	for k := range n {
		all[k] = (start + k) % n
	}
	return all
}

// Probe checks one shard; postgres.Prober.Probe is one.
type Probe func(context.Context) error

// Watch probes every shard each interval until ctx ends, putting a shard
// that answers back into choices at once and keeping one that fails out. It
// logs each change. Without Watch, a shard that failed comes back when downFor
// has passed, and the next real call is its probe.
func (s *Store) Watch(ctx context.Context, probes []Probe, interval, timeout time.Duration) {
	var wg sync.WaitGroup
	for i, probe := range probes {
		wg.Go(func() {
			wasUp := true
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				pctx, cancel := context.WithTimeout(ctx, timeout)
				err := probe(pctx)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err == nil {
					s.health.up(i)
					if !wasUp {
						s.logger.InfoContext(ctx, "shard answering again", "shard", i)
					}
				} else {
					s.health.down(i, time.Now())
					if wasUp {
						s.logger.WarnContext(ctx, "shard not answering; unkeyed enqueues, leases and announcements skip it",
							slog.Int("shard", i), slog.Any("err", err))
					}
				}
				wasUp = err == nil
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	wg.Wait()
}
