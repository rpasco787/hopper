package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

type observerEvent struct {
	kind       string
	queue      string
	dead       bool
	jobs       []store.Job
	leasedAt   time.Time
	finishedAt time.Time
}

type recordingObserver struct {
	mu     sync.Mutex
	events []observerEvent
}

func (r *recordingObserver) add(e observerEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recordingObserver) Enqueued(queue string) {
	r.add(observerEvent{kind: "enqueued", queue: queue})
}

func (r *recordingObserver) Leased(jobs []store.Job) {
	r.add(observerEvent{kind: "leased", jobs: append([]store.Job(nil), jobs...)})
}

func (r *recordingObserver) Completed(queue string, leasedAt, finishedAt time.Time) {
	r.add(observerEvent{kind: "completed", queue: queue, leasedAt: leasedAt, finishedAt: finishedAt})
}

func (r *recordingObserver) Nacked(queue string, dead bool) {
	r.add(observerEvent{kind: "nacked", queue: queue, dead: dead})
}

func (r *recordingObserver) LeaseExpired(queue string, dead bool) {
	r.add(observerEvent{kind: "expired", queue: queue, dead: dead})
}

func (r *recordingObserver) snapshot() []observerEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observerEvent(nil), r.events...)
}

func (r *recordingObserver) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

func TestObserverSeesInsertNotDedup(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	key := "same"

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	_, _, err = s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	_, _, err = s.Enqueue(ctx, store.EnqueueParams{Queue: "r"})
	require.NoError(t, err)

	require.Equal(t, []observerEvent{
		{kind: "enqueued", queue: "q"},
		{kind: "enqueued", queue: "r"},
	}, rec.snapshot())
}

func TestObserverSeesLeasedRows(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	enqueued := enqueueN(t, ctx, s, "q", 3, 0)
	rec.reset()

	got, err := s.Lease(ctx, []string{"q"}, 3, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, got, 3)
	events := rec.snapshot()
	require.Len(t, events, 1)
	require.Equal(t, "leased", events[0].kind)
	require.Len(t, events[0].jobs, 3)
	wantRunAt := make(map[uuid.UUID]int64, len(enqueued))
	for _, j := range enqueued {
		wantRunAt[j.ID] = j.RunAt.UnixMicro()
	}
	for _, j := range events[0].jobs {
		require.Equal(t, int32(1), j.Attempts)
		require.NotNil(t, j.LeasedAt)
		require.Equal(t, wantRunAt[j.ID], j.RunAt.UnixMicro())
	}
}

func TestObserverSeesCompletionOnce(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	j := leaseOne(t, ctx, s, "q", "w")
	rec.reset()

	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	events := rec.snapshot()
	require.Len(t, events, 1)
	require.Equal(t, "completed", events[0].kind)
	require.Equal(t, "q", events[0].queue)
	require.Equal(t, j.LeasedAt.UnixMicro(), events[0].leasedAt.UnixMicro())
	stored := readJob(t, j.ID)
	require.Equal(t, stored.FinishedAt.UnixMicro(), events[0].finishedAt.UnixMicro())

	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
	require.Len(t, rec.snapshot(), 1)
	require.ErrorIs(t, s.Ack(ctx, j.ID, uuid.New()), store.ErrLeaseLost)
	require.Len(t, rec.snapshot(), 1)
}

func TestObserverSeesNackOutcome(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond}).WithObserver(rec)
	j := leaseOne(t, ctx, s, "q", "w")
	_, err := pool.Exec(ctx, `update jobs set max_attempts = 2 where id = $1`, j.ID)
	require.NoError(t, err)
	rec.reset()

	require.NoError(t, s.Nack(ctx, j.ID, *j.LeaseID, "first"))
	require.Equal(t, []observerEvent{{kind: "nacked", queue: "q"}}, rec.snapshot())
	time.Sleep(2 * time.Millisecond)
	_, err = s.PromoteDue(ctx, 1)
	require.NoError(t, err)
	leased, err := s.Lease(ctx, []string{"q"}, 1, time.Minute, "w")
	require.NoError(t, err)
	require.Len(t, leased, 1)
	rec.reset()

	require.NoError(t, s.Nack(ctx, leased[0].ID, *leased[0].LeaseID, "second"))
	require.Equal(t, []observerEvent{{kind: "nacked", queue: "q", dead: true}}, rec.snapshot())
	require.NoError(t, s.Nack(ctx, leased[0].ID, *leased[0].LeaseID, "retry"))
	require.Len(t, rec.snapshot(), 1)
}

func TestObserverSeesReapOutcome(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "retry", MaxAttempts: 2})
	require.NoError(t, err)
	_, _, err = s.Enqueue(ctx, store.EnqueueParams{Queue: "dead", MaxAttempts: 1})
	require.NoError(t, err)
	jobs, err := s.Lease(ctx, []string{"retry", "dead"}, 2, time.Millisecond, "w")
	require.NoError(t, err)
	for _, j := range jobs {
		expireLease(t, j.ID)
	}
	rec.reset()

	n, err := s.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	events := rec.snapshot()
	require.ElementsMatch(t, []observerEvent{
		{kind: "expired", queue: "retry"},
		{kind: "expired", queue: "dead", dead: true},
	}, events)
}

func TestObserverCalledBeforeSuccessfulReturn(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)

	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "ack"})
	require.NoError(t, err)
	require.Len(t, rec.snapshot(), 1)
	leased, err := s.Lease(ctx, []string{"ack"}, 1, time.Millisecond, "w")
	require.NoError(t, err)
	require.Len(t, rec.snapshot(), 2)
	require.NoError(t, s.Ack(ctx, j.ID, *leased[0].LeaseID))
	require.Len(t, rec.snapshot(), 3)

	nack := leaseOne(t, ctx, s, "nack", "w")
	before := len(rec.snapshot())
	require.NoError(t, s.Nack(ctx, nack.ID, *nack.LeaseID, "x"))
	require.Len(t, rec.snapshot(), before+1)

	reap := leaseOne(t, ctx, s, "reap", "w")
	expireLease(t, reap.ID)
	before = len(rec.snapshot())
	_, err = s.ReapExpiredLeases(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rec.snapshot(), before+1)
}

func TestObserverSilentOnStoreFailure(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	j := leaseOne(t, ctx, s, "q", "w")
	rec.reset()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, _, err := s.Enqueue(cancelled, store.EnqueueParams{Queue: "q"})
	require.Error(t, err)
	_, err = s.Lease(cancelled, []string{"q"}, 1, time.Second, "w")
	require.Error(t, err)
	require.Error(t, s.Ack(cancelled, j.ID, *j.LeaseID))
	require.Error(t, s.Nack(cancelled, j.ID, *j.LeaseID, "x"))
	_, err = s.ReapExpiredLeases(cancelled, 1)
	require.Error(t, err)
	require.ErrorIs(t, s.Nack(ctx, uuid.New(), *j.LeaseID, "x"), store.ErrNotFound)
	_, _, err = s.Enqueue(ctx, store.EnqueueParams{})
	require.Error(t, err)
	require.Empty(t, rec.snapshot())
}

func TestObserverCountsOnceAcrossALostResponse(t *testing.T) {
	ctx := ctxT(t)
	rec := &recordingObserver{}
	s := newStore(t).WithObserver(rec)
	ack := leaseOne(t, ctx, s, "ack", "w")
	nack := leaseOne(t, ctx, s, "nack", "w")
	key := "key"
	rec.reset()

	require.NoError(t, s.Ack(ctx, ack.ID, *ack.LeaseID))
	require.NoError(t, s.Ack(ctx, ack.ID, *ack.LeaseID))
	require.NoError(t, s.Nack(ctx, nack.ID, *nack.LeaseID, "x"))
	require.NoError(t, s.Nack(ctx, nack.ID, *nack.LeaseID, "x"))
	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "enqueue", IdempotencyKey: &key})
	require.NoError(t, err)
	_, _, err = s.Enqueue(ctx, store.EnqueueParams{Queue: "enqueue", IdempotencyKey: &key})
	require.NoError(t, err)

	events := rec.snapshot()
	require.Len(t, events, 3)
	require.Equal(t, []string{"completed", "nacked", "enqueued"}, []string{events[0].kind, events[1].kind, events[2].kind})
}

func TestDefaultObserverIsNop(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t).WithObserver(nil)
	j := leaseOne(t, ctx, s, "q", "w")
	require.NoError(t, s.Ack(ctx, j.ID, *j.LeaseID))
}
