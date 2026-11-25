package sharded

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rpasco787/hopper/internal/store"
)

// Enqueue sends a keyed job to its key's shard, so dedup works there as on one
// database, and an unkeyed one to the next healthy shard in rotation.
func (s *Store) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	if p.IdempotencyKey != nil {
		i := store.KeyShard(p.Queue, *p.IdempotencyKey, len(s.shards))
		job, inserted, err := s.shards[i].Enqueue(ctx, p)
		return job, inserted, s.saw(i, err)
	}
	return s.enqueueUnkeyed(ctx, p, s.rotate())
}

// enqueueUnkeyed tries the healthy shards from start. It moves on only after
// a failure that sent nothing (store.ErrNotSent). Any other failure is
// returned as it is: the insert may have committed, and a second try
// elsewhere would make a second job (docs/design/sharding.md, invariant 5).
func (s *Store) enqueueUnkeyed(ctx context.Context, p store.EnqueueParams, start int) (store.Job, bool, error) {
	var first error
	for _, i := range s.health.order(start, time.Now()) {
		job, inserted, err := s.shards[i].Enqueue(ctx, p)
		if err == nil {
			return job, inserted, nil
		}
		err = s.saw(i, err)
		if !errors.Is(err, store.ErrNotSent) || ctx.Err() != nil {
			return store.Job{}, false, err
		}
		if first == nil {
			first = err
		}
	}
	return store.Job{}, false, first
}

// EnqueueMany groups items by shard (keyed items by key, unkeyed ones round
// robin) and runs each group concurrently, each group in input order. A group
// stops at its first failure, and the other groups carry on, so what
// committed need not be a prefix (docs/semantics.md §8).
func (s *Store) EnqueueMany(ctx context.Context, ps []store.EnqueueParams) ([]store.EnqueueResult, error) {
	n := len(s.shards)
	out := make([]store.EnqueueResult, len(ps))
	errs := make([]error, len(ps))
	groups := make([][]int, n)
	start, unkeyed := s.rotate(), 0
	for k, p := range ps {
		var i int
		if p.IdempotencyKey != nil {
			i = store.KeyShard(p.Queue, *p.IdempotencyKey, n)
		} else {
			i = (start + unkeyed) % n
			unkeyed++
		}
		groups[i] = append(groups[i], k)
	}
	var wg sync.WaitGroup
	for i, ks := range groups {
		if len(ks) == 0 {
			continue
		}
		wg.Go(func() {
			for _, k := range ks {
				var (
					job      store.Job
					inserted bool
					err      error
				)
				if ps[k].IdempotencyKey != nil {
					job, inserted, err = s.shards[i].Enqueue(ctx, ps[k])
					err = s.saw(i, err)
				} else {
					job, inserted, err = s.enqueueUnkeyed(ctx, ps[k], i)
				}
				if err != nil {
					errs[k] = err
					return
				}
				out[k] = store.EnqueueResult{Job: job, Inserted: inserted, Done: true}
			}
		})
	}
	wg.Wait()
	for k, err := range errs {
		if err != nil {
			return out, fmt.Errorf("sharded: enqueue many: item %d: %w", k, err)
		}
	}
	return out, nil
}
