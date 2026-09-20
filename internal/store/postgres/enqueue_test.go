package postgres

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

func TestEnqueueDefaults(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	before := time.Now()
	j, inserted, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:   "email",
		Payload: json.RawMessage(`{"to":"a@b.c"}`),
	})
	require.NoError(t, err)
	require.True(t, inserted, "a job with no idempotency key always inserts")

	require.NotEqual(t, uuid.Nil, j.ID)
	require.Equal(t, uuid.Version(7), j.ID.Version(), "ids must be UUIDv7 so the pkey appends in time order")

	require.Equal(t, "email", j.Queue)
	require.JSONEq(t, `{"to":"a@b.c"}`, string(j.Payload))
	require.Equal(t, store.StateAvailable, j.State, "a due job is leasable immediately")
	require.Equal(t, int16(0), j.Priority)
	require.Equal(t, int32(0), j.Attempts)
	require.Equal(t, int32(5), j.MaxAttempts)
	require.Nil(t, j.LeaseID)
	require.Nil(t, j.LeasedBy)
	require.Nil(t, j.LeasedAt)
	require.Nil(t, j.LeaseExpires)
	require.Nil(t, j.IdempotencyKey)
	require.Nil(t, j.LastError)
	require.Nil(t, j.FinishedAt)
	require.WithinDuration(t, before, j.RunAt, 2*time.Second, "zero RunAt defaults to now")
	require.WithinDuration(t, before, j.CreatedAt, 2*time.Second)
	require.WithinDuration(t, before, j.UpdatedAt, 2*time.Second)
}

func TestEnqueueFutureRunAtIsScheduled(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	runAt := time.Now().Add(time.Hour)
	j, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "email", RunAt: runAt})
	require.NoError(t, err)

	require.Equal(t, store.StateScheduled, j.State, "a future job is not leasable; the promoter moves it later")
	// The DB stores microseconds and returns its own zone, so compare instants
	// with a tolerance rather than Equal.
	require.WithinDuration(t, runAt, j.RunAt, time.Millisecond)
}

func TestEnqueueOptionalFieldsRoundTrip(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "order-42"
	j, _, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "orders",
		Payload:        nil,
		Priority:       7,
		MaxAttempts:    2,
		IdempotencyKey: &key,
	})
	require.NoError(t, err)

	require.JSONEq(t, `{}`, string(j.Payload), "empty payload is stored as {}")
	require.Equal(t, int16(7), j.Priority)
	require.Equal(t, int32(2), j.MaxAttempts)
	require.NotNil(t, j.IdempotencyKey)
	require.Equal(t, key, *j.IdempotencyKey)

	// And the row really is what RETURNING said it is.
	got := readJob(t, j.ID)
	require.Equal(t, j.ID, got.ID)
	require.Equal(t, key, *got.IdempotencyKey)
	require.JSONEq(t, `{}`, string(got.Payload))
}

func TestEnqueueEmptyQueueRejected(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	_, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: ""})
	require.Error(t, err)

	var n int
	require.NoError(t, pool.QueryRow(ctx, "select count(*) from jobs").Scan(&n))
	require.Equal(t, 0, n, "validation must fail before anything is written")
}

// TestEnqueueDedupsOnIdempotencyKey is docs/semantics.md §8: the first writer
// wins, the duplicate is handed the original row, and it is not told that its
// payload, priority or run_at were ignored — only that nothing was inserted.
func TestEnqueueDedupsOnIdempotencyKey(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "order-7"
	first, inserted, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "orders",
		Payload:        json.RawMessage(`{"n":1}`),
		Priority:       3,
		MaxAttempts:    9,
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	// Same (queue, key), everything else deliberately different.
	second, inserted, err := s.Enqueue(ctx, store.EnqueueParams{
		Queue:          "orders",
		Payload:        json.RawMessage(`{"n":2}`),
		Priority:       10,
		MaxAttempts:    1,
		RunAt:          time.Now().Add(time.Hour),
		IdempotencyKey: &key,
	})
	require.NoError(t, err)
	require.False(t, inserted, "a duplicate must not insert")
	require.Equal(t, first.ID, second.ID)
	require.JSONEq(t, `{"n":1}`, string(second.Payload))
	require.Equal(t, int16(3), second.Priority)
	require.Equal(t, int32(9), second.MaxAttempts)
	require.Equal(t, store.StateAvailable, second.State, "the duplicate's future RunAt is ignored")
	require.Equal(t, first.UpdatedAt.UnixMicro(), second.UpdatedAt.UnixMicro(),
		"the existing row is read, never rewritten")

	var n int
	require.NoError(t, pool.QueryRow(ctx, `select count(*) from jobs`).Scan(&n))
	require.Equal(t, 1, n)
}

func TestEnqueueKeysAreScopedPerQueue(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "shared"
	a, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q1", IdempotencyKey: &key})
	require.NoError(t, err)
	require.True(t, inserted)

	b, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q2", IdempotencyKey: &key})
	require.NoError(t, err)
	require.True(t, inserted, "the same key in another queue is another job")
	require.NotEqual(t, a.ID, b.ID)
}

// TestEnqueueWithoutKeyNeverDedups pins why the conflict target repeats the
// index predicate: rows with a null key are not in jobs_idem_uidx at all, so
// ON CONFLICT can never fire for them however identical they are.
func TestEnqueueWithoutKeyNeverDedups(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	p := store.EnqueueParams{Queue: "q", Payload: json.RawMessage(`{"same":true}`)}
	a, inserted, err := s.Enqueue(ctx, p)
	require.NoError(t, err)
	require.True(t, inserted)
	b, inserted, err := s.Enqueue(ctx, p)
	require.NoError(t, err)
	require.True(t, inserted)
	require.NotEqual(t, a.ID, b.ID)
}

// TestEnqueueDedupsAgainstFinishedRows is the surprising sentence in
// docs/semantics.md §8: protection lasts as long as the row exists, not as
// long as the job is runnable. A key on a completed job still dedups.
func TestEnqueueDedupsAgainstFinishedRows(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	key := "invoice-1"
	first, _, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	leased, err := s.Lease(ctx, []string{"q"}, 1, time.Second, "w")
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.NoError(t, s.Ack(ctx, leased[0].ID, *leased[0].LeaseID))

	again, inserted, err := s.Enqueue(ctx, store.EnqueueParams{Queue: "q", IdempotencyKey: &key})
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, first.ID, again.ID)
	require.Equal(t, store.StateCompleted, again.State,
		"the caller gets the finished job back, not a fresh one")
}

// TestIdempotencyStorm is the docs/semantics.md §13 row and the session's
// "done when": 1000 concurrent Enqueue calls over 10 distinct keys leave
// exactly 10 rows, every caller for a key is handed the same id, and exactly
// one caller per key reports having inserted.
//
// Unlike TestLeaseDisjoint this does not need a connection per goroutine. The
// claim is about the unique index arbitrating writes, not about row locks, so
// goroutines queueing on the 60-connection pool does not weaken it — it just
// means the storm arrives in waves, which is if anything harder.
func TestIdempotencyStorm(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	const (
		keys    = 10
		perKey  = 100
		callers = keys * perKey
	)
	keyFor := func(i int) string { return fmt.Sprintf("key-%d", i%keys) }

	ids := make([]uuid.UUID, callers)
	inserts := make([]bool, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := keyFor(i)
			<-start // so they genuinely collide rather than trickling in
			j, inserted, err := s.Enqueue(ctx, store.EnqueueParams{
				Queue:          "storm",
				Payload:        json.RawMessage(fmt.Sprintf(`{"caller":%d}`, i)),
				IdempotencyKey: &key,
			})
			ids[i], inserts[i], errs[i] = j.ID, inserted, err
		}(i)
	}
	close(start)
	wg.Wait()

	byKey := make(map[string]uuid.UUID, keys)
	insertCount := 0
	for i := range callers {
		require.NoErrorf(t, errs[i], "caller %d", i)
		if inserts[i] {
			insertCount++
		}
		key := keyFor(i)
		if prev, ok := byKey[key]; ok {
			require.Equalf(t, prev, ids[i], "every caller for %s must get the same id", key)
		} else {
			byKey[key] = ids[i]
		}
	}
	require.Len(t, byKey, keys)
	require.Equal(t, keys, insertCount, "exactly one caller per key may insert")

	var n int
	require.NoError(t, pool.QueryRow(ctx, `select count(*) from jobs`).Scan(&n))
	require.Equal(t, keys, n, "1000 enqueues, 10 keys, 10 rows")
}
