package broker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// batchingStore is a fakeStore that can also confirm in batches. A method a
// test leaves nil panics if called, as in fakeStore.
type batchingStore struct {
	*fakeStore
	ackManyFn    func(context.Context, []store.LeaseRef) []error
	extendManyFn func(context.Context, []store.Extension) []error
}

func (b *batchingStore) AckMany(ctx context.Context, refs []store.LeaseRef) []error {
	return b.ackManyFn(ctx, refs)
}
func (b *batchingStore) ExtendLeaseMany(ctx context.Context, exts []store.Extension) []error {
	return b.extendManyFn(ctx, exts)
}

func notApplied(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = store.ErrNotApplied
	}
	return errs
}

func TestAcksAndHeartbeatsGoThroughTheBatchers(t *testing.T) {
	id, lease := uuid.New(), uuid.New()
	var mu sync.Mutex
	var acked []store.LeaseRef
	var extended []store.Extension
	st := &batchingStore{
		fakeStore: &fakeStore{}, // a direct Ack or ExtendLease would panic
		ackManyFn: func(_ context.Context, refs []store.LeaseRef) []error {
			mu.Lock()
			defer mu.Unlock()
			acked = append(acked, refs...)
			return make([]error, len(refs))
		},
		extendManyFn: func(_ context.Context, exts []store.Extension) []error {
			mu.Lock()
			defer mu.Unlock()
			extended = append(extended, exts...)
			return make([]error, len(exts))
		},
	}
	rpc := newTestClient(t, NewService(st, Config{ConfirmBatch: 64}))

	_, err := rpc.Ack(t.Context(), &hopperv1.AckRequest{Id: id.String(), LeaseId: lease.String()})
	require.NoError(t, err)
	_, err = rpc.Heartbeat(t.Context(), &hopperv1.HeartbeatRequest{
		Id: id.String(), LeaseId: lease.String(), LeaseDuration: durationpb.New(time.Minute)})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	ref := store.LeaseRef{ID: id, LeaseID: lease}
	require.Equal(t, []store.LeaseRef{ref}, acked)
	require.Equal(t, []store.Extension{{LeaseRef: ref, Duration: time.Minute}}, extended)
}

func TestABatchSlotErrorBecomesItsCallsStatus(t *testing.T) {
	st := &batchingStore{fakeStore: &fakeStore{},
		ackManyFn: func(context.Context, []store.LeaseRef) []error { return []error{store.ErrUnavailable} }}
	rpc := newTestClient(t, NewService(st, Config{ConfirmBatch: 64}))
	_, err := rpc.Ack(t.Context(), &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
	require.Equal(t, codes.Unavailable, status.Code(err))
}

// TestANotAppliedPairGetsTheOneRowAnswer: a pair the batch didn't apply is
// answered by the one-row call, exactly as with batching off.
func TestANotAppliedPairGetsTheOneRowAnswer(t *testing.T) {
	st := &batchingStore{
		fakeStore: &fakeStore{
			ackFn:    func(context.Context, uuid.UUID, uuid.UUID) error { return store.ErrLeaseLost },
			extendFn: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) error { return store.ErrCancelled },
		},
		ackManyFn:    func(_ context.Context, refs []store.LeaseRef) []error { return notApplied(len(refs)) },
		extendManyFn: func(_ context.Context, exts []store.Extension) []error { return notApplied(len(exts)) },
	}
	rpc := newTestClient(t, NewService(st, Config{ConfirmBatch: 64}))
	_, err := rpc.Ack(t.Context(), &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
	require.Equal(t, codes.Aborted, status.Code(err))
	_, err = rpc.Heartbeat(t.Context(), &hopperv1.HeartbeatRequest{
		Id: uuid.NewString(), LeaseId: uuid.NewString(), LeaseDuration: durationpb.New(time.Minute)})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestALockedRowDelaysOnlyItsOwnCaller: a free pair and a locked pair share
// one batch. The free caller is answered as soon as the batch returns, while
// the locked caller still waits on its own one-row Ack.
func TestALockedRowDelaysOnlyItsOwnCaller(t *testing.T) {
	warm, free, locked := uuid.New(), uuid.New(), uuid.New()
	hold, unlock := make(chan struct{}), make(chan struct{})
	batches := make(chan []store.LeaseRef, 4)
	st := &batchingStore{
		fakeStore: &fakeStore{ackFn: func(ctx context.Context, id, _ uuid.UUID) error {
			if id != locked {
				return errors.New("only the locked pair falls back")
			}
			select {
			case <-unlock:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
		ackManyFn: func(_ context.Context, refs []store.LeaseRef) []error {
			batches <- refs
			if refs[0].ID == warm {
				<-hold
			}
			errs := make([]error, len(refs))
			for i, r := range refs {
				if r.ID == locked {
					errs[i] = store.ErrNotApplied
				}
			}
			return errs
		},
	}
	svc := NewService(st, Config{ConfirmBatch: 64, ConfirmFlights: 1})
	rpc := newTestClient(t, svc)
	ack := func(id uuid.UUID) <-chan error {
		out := make(chan error, 1)
		go func() {
			_, err := rpc.Ack(t.Context(), &hopperv1.AckRequest{Id: id.String(), LeaseId: uuid.NewString()})
			out <- err
		}()
		return out
	}

	first := ack(warm)
	<-batches // the only flight is busy, so the next two queue together
	freeDone, lockedDone := ack(free), ack(locked)
	require.Eventually(t, func() bool { return svc.acks[0].queued() == 2 }, time.Second, time.Millisecond)
	close(hold)
	require.NoError(t, <-first)
	require.Len(t, <-batches, 2, "the free and the locked pair share a batch")
	select {
	case err := <-freeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the free caller waited for the locked row")
	}
	select {
	case err := <-lockedDone:
		t.Fatalf("the locked caller returned before its row was released: %v", err)
	default:
	}
	close(unlock)
	require.NoError(t, <-lockedDone)
}

func TestABatchOfOneConfirmsDirectly(t *testing.T) {
	var direct atomic.Int32
	st := &batchingStore{fakeStore: &fakeStore{
		ackFn: func(context.Context, uuid.UUID, uuid.UUID) error { direct.Add(1); return nil },
	}} // ackManyFn nil: batching would panic
	for _, cfg := range []Config{{ConfirmBatch: 1}} { // explicitly off
		rpc := newTestClient(t, NewService(st, cfg))
		_, err := rpc.Ack(t.Context(), &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, direct.Load())
}

// TestBatchingIsAnnouncedOrRefusedOutLoud: a broker says when it batches,
// and warns when it was asked to but its store can't.
func TestBatchingIsAnnouncedOrRefusedOutLoud(t *testing.T) {
	var on bytes.Buffer
	svc := NewService(&batchingStore{fakeStore: &fakeStore{}},
		Config{ConfirmBatch: 64, Logger: slog.New(slog.NewTextHandler(&on, nil))})
	require.NotNil(t, svc.acks)
	require.Contains(t, on.String(), "confirmation batching on")

	var off bytes.Buffer
	svc = NewService(&fakeStore{}, Config{ConfirmBatch: 64, Logger: slog.New(slog.NewTextHandler(&off, nil))})
	require.Nil(t, svc.acks)
	require.Contains(t, off.String(), "level=WARN")
	require.Contains(t, off.String(), "the store cannot batch")
}

// TestShutdownWaitsForAnAbandonedConfirmationStatement: a caller's deadline
// passes mid-statement, so the batcher cancels the statement. Shutdown
// returns only once that statement has ended, so the pool can close behind it.
func TestShutdownWaitsForAnAbandonedConfirmationStatement(t *testing.T) {
	var cancelled atomic.Bool
	finished := make(chan struct{})
	st := &batchingStore{fakeStore: &fakeStore{},
		ackManyFn: func(ctx context.Context, refs []store.LeaseRef) []error {
			defer close(finished)
			<-ctx.Done()
			cancelled.Store(errors.Is(ctx.Err(), context.Canceled))
			time.Sleep(200 * time.Millisecond) // a cancelled statement takes a moment to unwind
			return slices.Repeat([]error{ctx.Err()}, len(refs))
		}}
	srv, conn := startServer(t, st, Config{ConfirmBatch: 64})
	rpc := hopperv1.NewHopperServiceClient(conn)
	start := time.Now()
	callCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := rpc.Ack(callCtx, &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	shutdownCtx, cancelShutdown := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelShutdown()
	require.NoError(t, srv.Shutdown(shutdownCtx))
	select {
	case <-finished:
	default:
		t.Fatal("Shutdown returned while a confirmation statement was still running")
	}
	require.True(t, cancelled.Load(), "cancelled when its only caller left, not timed out at the cap")
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestConfirmationsBatchByDefault(t *testing.T) {
	svc := NewService(&batchingStore{fakeStore: &fakeStore{}}, Config{})
	require.NotNil(t, svc.acks)
	require.NotNil(t, svc.extends)
}
