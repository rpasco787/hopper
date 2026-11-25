package sharded

import (
	"context"
	"errors"
	"time"

	"github.com/rpasco787/hopper/internal/store"
)

// Lease visits the healthy shards from a rotating start and asks each only for
// what is still missing, so the total never exceeds n (the credit invariant,
// docs/design/credit-anchored-freshness.md). Rows come back in shard order:
// priority and run_at order hold within a shard (docs/semantics.md §9).
//
// A shard that fails is skipped. The rows other shards returned are still
// returned, with no error, and the failure is an error only when no shard
// answered, so one shard down never ends a stream. Rows a failed shard may
// have leased are never sent, and its reaper takes them back as after any
// failed lease (§11.5).
func (s *Store) Lease(ctx context.Context, queues []string, n int, leaseDur time.Duration, workerID string) ([]store.Job, error) {
	if len(queues) == 0 || n <= 0 {
		return nil, nil
	}
	var (
		got      []store.Job
		first    error
		answered bool
	)
	for _, i := range s.health.order(s.rotate(), time.Now()) {
		if len(got) >= n {
			break
		}
		jobs, err := s.shards[i].Lease(ctx, queues, n-len(got), leaseDur, workerID)
		if err != nil {
			err = s.saw(i, err)
			if ctx.Err() != nil {
				return got, ctx.Err()
			}
			if !errors.Is(err, store.ErrUnavailable) {
				s.logger.WarnContext(ctx, "lease failed on one shard; serving the others", "err", err)
			}
			if first == nil {
				first = err
			}
			continue
		}
		answered = true
		got = append(got, jobs...)
	}
	if !answered && first != nil {
		return nil, first
	}
	if len(got) == 0 {
		return nil, nil
	}
	return got, nil
}
