package sharded

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// One cursor is enough: IDs are one total order across shards, and each
// shard's page holds every row of the global page that it owns.
func TestListJobsPagesAcrossShardsInIDOrder(t *testing.T) {
	s, pgs := newSharded(t)
	fill(t, pgs, 30)
	var seen []uuid.UUID
	var after *uuid.UUID
	for {
		page, err := s.ListJobs(ctxT(t), "q", nil, 7, after)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), 7)
		if len(page) == 0 {
			break
		}
		for _, j := range page {
			seen = append(seen, j.ID)
		}
		last := page[len(page)-1].ID
		after = &last
	}
	require.Len(t, seen, 90)
	for k := 1; k < len(seen); k++ {
		require.Negative(t, bytes.Compare(seen[k-1][:], seen[k][:]))
	}
}

func TestListJobsFailsWhenAShardIsDown(t *testing.T) {
	s, _ := withDead(t, 2)
	_, err := s.ListJobs(ctxT(t), "q", nil, 10, nil)
	require.ErrorIs(t, err, store.ErrUnavailable, "a page with a hole in it would skip rows")
}

// kill makes one dead job on shard i: one attempt, leased and nacked.
func kill(t *testing.T, s *Store, i int) uuid.UUID {
	t.Helper()
	key := keyFor("d", i) + uuid.NewString()[:4]
	for store.KeyShard("d", key, testShards) != i {
		key = keyFor("d", i) + uuid.NewString()[:4]
	}
	job, _, err := s.Enqueue(ctxT(t), store.EnqueueParams{Queue: "d", MaxAttempts: 1, IdempotencyKey: &key})
	require.NoError(t, err)
	for {
		got, err := s.Lease(ctxT(t), []string{"d"}, 10, time.Minute, "w")
		require.NoError(t, err)
		for _, j := range got {
			require.NoError(t, s.Nack(ctxT(t), j.ID, *j.LeaseID, "boom"))
		}
		if dead, _ := s.GetJob(ctxT(t), job.ID); dead.State == store.StateDead {
			return job.ID
		}
	}
}

func TestDeadLetterAcrossShards(t *testing.T) {
	s, _ := newSharded(t)
	var ids []uuid.UUID
	for i := range 6 {
		ids = append(ids, kill(t, s, i%testShards))
	}
	var listed []uuid.UUID
	var after *store.DeadCursor
	for {
		page, err := s.ListDead(ctxT(t), "d", 2, after)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		for k, j := range page {
			listed = append(listed, j.ID)
			if k > 0 {
				require.False(t, page[k].FinishedAt.Before(*page[k-1].FinishedAt))
			}
		}
		last := page[len(page)-1]
		after = &store.DeadCursor{FinishedAt: *last.FinishedAt, ID: last.ID}
	}
	require.ElementsMatch(t, ids, listed)

	foreign := store.StampShard(uuid.Must(uuid.NewV7()), 9)
	n, err := s.Redrive(ctxT(t), append(ids[:3:3], foreign))
	require.NoError(t, err)
	require.Equal(t, 3, n, "a foreign id is ignored, as a live one is")
	n, err = s.Purge(ctxT(t), ids[3:])
	require.NoError(t, err)
	require.Equal(t, 3, n)
}

func TestBatchConfirmationsSplitByShard(t *testing.T) {
	s, pgs := newSharded(t)
	fill(t, pgs, 4)
	got, err := s.Lease(ctxT(t), []string{"q"}, 12, time.Minute, "w")
	require.NoError(t, err)
	exts := make([]store.Extension, len(got))
	refs := make([]store.LeaseRef, len(got))
	for k, j := range got {
		refs[k] = store.LeaseRef{ID: j.ID, LeaseID: *j.LeaseID}
		exts[k] = store.Extension{LeaseRef: refs[k], Duration: time.Minute}
	}
	refs = append(refs, store.LeaseRef{ID: store.StampShard(uuid.Must(uuid.NewV7()), 8), LeaseID: uuid.New()})
	for k, err := range s.ExtendLeaseMany(ctxT(t), exts) {
		require.NoError(t, err, "extension %d", k)
	}
	errs := s.AckMany(ctxT(t), refs)
	require.Len(t, errs, len(refs))
	for k := range got {
		require.NoError(t, errs[k])
	}
	require.ErrorIs(t, errs[len(errs)-1], store.ErrNotApplied, "the one-row Ack then names it NotFound")
}

func TestSweepsVisitEveryShard(t *testing.T) {
	s, pgs := newSharded(t)
	for _, p := range pgs {
		_, _, err := p.Enqueue(ctxT(t), store.EnqueueParams{Queue: "q", RunAt: time.Now().Add(-time.Second)})
		require.NoError(t, err)
	}
	// Enqueue chose 'available' for a past run_at, so put the rows back to scheduled.
	for _, p := range pools {
		_, err := p.Exec(context.Background(), "update jobs set state = 'scheduled'")
		require.NoError(t, err)
	}
	n, err := s.PromoteDue(ctxT(t), 100)
	require.NoError(t, err)
	require.Equal(t, testShards, n)
	n, err = s.PromoteDue(ctxT(t), 0)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestAnnounceGoesAroundADownShard(t *testing.T) {
	s, _ := withDead(t, 0)
	for range 5 {
		require.NoError(t, s.Announce(ctxT(t), []string{"q"}))
	}
	require.Len(t, s.Listeners(), testShards)
}
