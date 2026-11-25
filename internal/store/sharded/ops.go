package sharded

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// each runs f on every shard at once and returns the first failure in shard
// order.
func (s *Store) each(f func(i int, sh Shard) error) error {
	errs := make([]error, len(s.shards))
	var wg sync.WaitGroup
	for i, sh := range s.shards {
		wg.Go(func() { errs[i] = s.saw(i, f(i, sh)) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func compareID(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) }

// ListJobs asks every shard for the page after the cursor and keeps the first
// limit rows of the merge. One cursor serves every shard because PostgreSQL
// orders uuid bytewise, which is one total order across shards: each shard's
// page holds every row of the global page that it owns. A shard that fails
// fails the call, since a page missing a shard would skip its rows
// (docs/design/sharding.md).
func (s *Store) ListJobs(ctx context.Context, queue string, states []store.State, limit int, after *uuid.UUID) ([]store.Job, error) {
	pages := make([][]store.Job, len(s.shards))
	if err := s.each(func(i int, sh Shard) (err error) {
		pages[i], err = sh.ListJobs(ctx, queue, states, limit, after)
		return err
	}); err != nil {
		return nil, err
	}
	all := slices.Concat(pages...)
	slices.SortFunc(all, func(a, b store.Job) int { return compareID(a.ID, b.ID) })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// ListDead merges by (FinishedAt, ID) for the reason ListJobs merges by ID.
func (s *Store) ListDead(ctx context.Context, queue string, limit int, after *store.DeadCursor) ([]store.Job, error) {
	pages := make([][]store.Job, len(s.shards))
	if err := s.each(func(i int, sh Shard) (err error) {
		pages[i], err = sh.ListDead(ctx, queue, limit, after)
		return err
	}); err != nil {
		return nil, err
	}
	all := slices.Concat(pages...)
	slices.SortFunc(all, func(a, b store.Job) int {
		if c := a.FinishedAt.Compare(*b.FinishedAt); c != 0 {
			return c
		}
		return compareID(a.ID, b.ID)
	})
	if len(all) > limit {
		all = all[:limit]
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all, nil
}

func (s *Store) Redrive(ctx context.Context, ids []uuid.UUID) (int, error) {
	return s.byShard(ids, func(sh Shard, part []uuid.UUID) (int, error) { return sh.Redrive(ctx, part) })
}

func (s *Store) Purge(ctx context.Context, ids []uuid.UUID) (int, error) {
	return s.byShard(ids, func(sh Shard, part []uuid.UUID) (int, error) { return sh.Purge(ctx, part) })
}

// byShard runs f on each shard with its own ids, all at once, and returns the
// total and the first failure. An id that names no shard is ignored, as each
// shard ignores ids that are not dead.
func (s *Store) byShard(ids []uuid.UUID, f func(Shard, []uuid.UUID) (int, error)) (int, error) {
	groups := make([][]uuid.UUID, len(s.shards))
	for _, id := range ids {
		if i, err := s.ShardOf(id); err == nil {
			groups[i] = append(groups[i], id)
		}
	}
	var total atomic.Int64
	err := s.each(func(i int, sh Shard) error {
		if len(groups[i]) == 0 {
			return nil
		}
		n, err := f(sh, groups[i])
		total.Add(int64(n))
		return err
	})
	return int(total.Load()), err
}

// PromoteDue, ReapExpiredLeases and DeleteFinished visit each shard in turn
// with what is left of limit, so "up to limit" still holds. cmd/broker runs
// each sweep per shard instead, so one shard's outage never holds up
// another's; these make the sharded store a whole store.Store.

func (s *Store) PromoteDue(ctx context.Context, limit int) (int, error) {
	return s.sweep(limit, func(sh Shard, n int) (int, error) { return sh.PromoteDue(ctx, n) })
}

func (s *Store) ReapExpiredLeases(ctx context.Context, limit int) (int, error) {
	return s.sweep(limit, func(sh Shard, n int) (int, error) { return sh.ReapExpiredLeases(ctx, n) })
}

func (s *Store) DeleteFinished(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	return s.sweep(limit, func(sh Shard, n int) (int, error) { return sh.DeleteFinished(ctx, cutoff, n) })
}

func (s *Store) sweep(limit int, f func(Shard, int) (int, error)) (int, error) {
	total := 0
	var errs []error
	for i, sh := range s.shards {
		if total >= limit {
			break
		}
		n, err := f(sh, limit-total)
		total += n
		if err != nil {
			errs = append(errs, s.saw(i, err))
		}
	}
	return total, errors.Join(errs...)
}

// AckMany and ExtendLeaseMany split their inputs by shard and run each part
// at once. The broker already keeps one batcher per shard, so in practice
// each call has one part. An item whose ID names no shard is ErrNotApplied,
// and the broker's one-row call then names it NotFound.
func (s *Store) AckMany(ctx context.Context, refs []store.LeaseRef) []error {
	return confirmMany(s, refs, func(r store.LeaseRef) uuid.UUID { return r.ID },
		func(sh Shard, part []store.LeaseRef) []error { return sh.AckMany(ctx, part) })
}

func (s *Store) ExtendLeaseMany(ctx context.Context, exts []store.Extension) []error {
	return confirmMany(s, exts, func(e store.Extension) uuid.UUID { return e.ID },
		func(sh Shard, part []store.Extension) []error { return sh.ExtendLeaseMany(ctx, part) })
}

func confirmMany[T any](s *Store, items []T, id func(T) uuid.UUID, run func(Shard, []T) []error) []error {
	out := make([]error, len(items))
	groups := make([][]int, len(s.shards))
	for k, it := range items {
		i, err := s.ShardOf(id(it))
		if err != nil {
			out[k] = store.ErrNotApplied
			continue
		}
		groups[i] = append(groups[i], k)
	}
	var wg sync.WaitGroup
	for i, ks := range groups {
		if len(ks) == 0 {
			continue
		}
		wg.Go(func() {
			part := make([]T, len(ks))
			for j, k := range ks {
				part[j] = items[k]
			}
			errs := run(s.shards[i], part)
			for j, k := range ks {
				var err error
				if j < len(errs) {
					err = errs[j]
				} else {
					err = fmt.Errorf("sharded: shard %d returned %d results for %d items", i, len(errs), len(ks))
				}
				out[k] = s.saw(i, err)
			}
		})
	}
	wg.Wait()
	return out
}

// Announce sends on one healthy shard, moving on if it fails. Every broker
// listens on every shard (Listeners), so any one reaches all of them, and
// rotating spreads NOTIFY's server-wide commit lock across shards.
func (s *Store) Announce(ctx context.Context, queues []string) error {
	if len(queues) == 0 {
		return nil
	}
	var first error
	for _, i := range s.health.order(s.announcements.start(len(s.shards)), time.Now()) {
		err := s.saw(i, s.shards[i].Announce(ctx, queues))
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if first == nil {
			first = err
		}
	}
	return first
}
