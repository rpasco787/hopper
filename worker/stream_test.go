package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// spy is a delivery runner the test drives. Task 3 wires dispatch and capacity;
// what a delivery actually does with its token arrives in Tasks 4 and 5, so
// until then the test says how long a delivery lasts.
type spy struct {
	deliveries chan *delivery
	hold       *gate // nil runs to completion immediately
	wg         sync.WaitGroup
	dispatched atomic.Int64
}

// newSpy registers its own cleanup first, so a test that fails while a
// delivery is held still releases it and joins the goroutine.
func newSpy(t *testing.T, block bool) *spy {
	t.Helper()
	sp := &spy{deliveries: make(chan *delivery, 64)}
	if block {
		sp.hold = newGate(t)
	}
	t.Cleanup(func() {
		if sp.hold != nil {
			sp.hold.release()
		}
		joined := make(chan struct{})
		go func() { defer close(joined); sp.wg.Wait() }()
		select {
		case <-joined:
		case <-time.After(barrier):
			t.Error("a held delivery never returned")
		}
	})
	return sp
}

func (sp *spy) run(_ *lifecycle, _ *work, _ hopperv1.HopperServiceClient, d *delivery) {
	sp.wg.Add(1)
	defer sp.wg.Done()
	sp.dispatched.Add(1)
	sp.deliveries <- d
	if sp.hold == nil {
		return
	}
	// Deliberately uncooperative: this delivery ends when the test says so,
	// not when the SDK asks. That is what makes abandonment observable.
	<-sp.hold.ch
}

func (sp *spy) await(t *testing.T, ctx context.Context) *delivery {
	t.Helper()
	select {
	case d := <-sp.deliveries:
		return d
	case <-ctx.Done():
		t.Fatalf("no delivery reached the runner: %v", ctx.Err())
		return nil
	}
}

func (sp *spy) count() int { return len(sp.deliveries) }

func newTestWorker(t *testing.T, target string, opts Options, runner deliveryRunner, queues ...string) *Worker {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quietLogger()
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 2 * time.Second
	}
	w := New(target, opts)
	for _, q := range queues {
		w.Handle(q, noop)
	}
	w.runner = runner
	return w
}

func TestSubscriptionAdvertisesSortedQueuesAndAllCredit(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{
		ID: "worker-7", Concurrency: 3, LeaseDuration: 7 * time.Second,
	}, sp.run, "zeta", "alpha", "mu")
	r := runWorker(t, w)

	s := f.awaitStream(t, ctx)
	require.Equal(t, []string{"alpha", "mu", "zeta"}, s.sub.GetQueues())
	require.Equal(t, "worker-7", s.sub.GetWorkerId())
	require.Equal(t, uint32(3), s.sub.GetCredit(), "a fresh worker advertises its whole capacity")
	require.Equal(t, 7*time.Second, s.sub.GetLeaseDuration().AsDuration())

	require.NoError(t, r.stopAndWait(t))
	require.NoError(t, s.awaitEOF(ctx), "a graceful stop half-closes the stream")
}

func TestZeroLeaseDurationLetsTheBrokerChoose(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)

	s := f.awaitStream(t, ctx)
	require.Nil(t, s.sub.LeaseDuration, "an absent duration is how the wire says the broker chooses")
	require.NoError(t, r.stopAndWait(t))
}

func TestCapacitySaturatesUntilDeliveriesFinish(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, true)
	w := newTestWorker(t, f.addr, Options{Concurrency: 2}, sp.run, "q")
	r := runWorker(t, w)

	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	sp.await(t, ctx)

	require.Equal(t, uint64(0), s.creditTotal(), "both tokens are held by deliveries")
	require.Empty(t, s.creditMessages(), "a saturated worker advertises nothing")

	sp.hold.release()
	require.NoError(t, s.awaitCredit(ctx, 2), "finished deliveries return their tokens")
	require.NoError(t, r.stopAndWait(t))
}

func TestMalformedDeliveryReturnsItsTokenAndKeepsTheStream(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)

	broken := newFixture("q").proto()
	broken.State = hopperv1.JobState_JOB_STATE_AVAILABLE
	require.NoError(t, s.deliver(ctx, broken))

	// The token comes back, which is only observable because the writer
	// re-advertises it. Nothing was executed and nothing was mutated: the
	// identifiers in a malformed message are not trustworthy enough to nack.
	require.NoError(t, s.awaitCredit(ctx, 1))
	require.Zero(t, sp.count())
	require.Empty(t, f.ackRequests())
	require.Empty(t, f.nackRequests())

	// The stream survives, so a good delivery still runs.
	good := newFixture("q")
	require.NoError(t, s.deliver(ctx, good.proto()))
	require.Equal(t, good.ID, sp.await(t, ctx).job.ID)
	require.NoError(t, r.stopAndWait(t))
}

func TestUnknownQueueIsDispatchedWithoutAHandler(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "known")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)

	require.NoError(t, s.deliver(ctx, newFixture("unknown").proto()))
	d := sp.await(t, ctx)
	require.Equal(t, "unknown", d.job.Queue)
	require.Nil(t, d.handler, "an unregistered queue is nacked unstarted, not dropped silently")

	require.NoError(t, r.stopAndWait(t))
}

func TestRegisteredQueueArrivesWithItsHandler(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)

	fx := newFixture("q")
	fx.Payload = []byte(`{"ms":5}`)
	fx.Attempt, fx.MaxAttempts, fx.Grant = 2, 9, 12*time.Second
	require.NoError(t, s.deliver(ctx, fx.proto()))

	d := sp.await(t, ctx)
	require.NotNil(t, d.handler)
	require.Equal(t, fx.ID, d.job.ID)
	require.Equal(t, fx.LeaseID, d.leaseID)
	require.Equal(t, int32(2), d.job.Attempt)
	require.Equal(t, int32(9), d.job.MaxAttempts)
	require.Equal(t, 12*time.Second, d.grant)
	require.JSONEq(t, `{"ms":5}`, string(d.job.Payload))
	require.NoError(t, r.stopAndWait(t))
}

func TestOverDeliveryEndsTheStream(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, true)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)
	s := f.awaitStream(t, ctx)

	// Spend the only credit on a delivery that holds its token, then send one
	// more anyway. A real broker never does this; if one did, manufacturing a
	// token to run the extra job would breach the limit the caller configured.
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	require.NoError(t, s.send(newFixture("q").proto()))

	// Wait for the worker to end the stream before releasing the held
	// delivery. Releasing first would return its token, the writer would
	// advertise fresh credit, and the excess job would become a legitimate
	// delivery — the test would then prove nothing and hang.
	require.NoError(t, s.awaitEnd(ctx))
	sp.hold.release()
	err := r.wait(t)
	require.ErrorIs(t, err, errOverDelivery)
	require.NotErrorIs(t, err, ErrAbandoned)
	require.Equal(t, int64(1), sp.dispatched.Load(), "the excess job was never dispatched")
}

func TestPermanentSubscriptionErrorStopsTheWorker(t *testing.T) {
	f := startFake(t)
	sp := newSpy(t, false)
	f.set(func() {
		f.onSubscribe = func(context.Context, *hopperv1.Subscription) error {
			return status.Error(codes.InvalidArgument, "queues is required")
		}
	})
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	r := runWorker(t, w)

	err := r.wait(t)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.NotErrorIs(t, err, ErrAbandoned, "nothing was in flight to abandon")
}

func TestBlockedStreamOpenStopsAtTheDeadline(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	sp := newSpy(t, false)
	g := newGate(t)
	entered := make(chan struct{}, 1)
	ctl.set(func() {
		ctl.onOpen = func(ctx context.Context) error {
			entered <- struct{}{}
			return g.wait(ctx)
		}
	})

	const budget = 300 * time.Millisecond
	w := newTestWorker(t, f.addr, Options{
		Concurrency: 1, ShutdownTimeout: budget, DialOptions: []grpc.DialOption{ctl.dialOption()},
	}, sp.run, "q")
	r := runWorker(t, w)

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the stream open never started")
	}

	start := time.Now()
	r.stop()
	require.NoError(t, r.wait(t), "a shutdown during establishment leaves nothing behind")
	require.Less(t, time.Since(start), 2*budget,
		"establishment shares the one shutdown deadline; it does not get a budget of its own")
}

func TestBlockedSubscriptionSendStopsAtTheDeadline(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	sp := newSpy(t, false)
	g := newGate(t)
	entered := make(chan struct{}, 1)
	ctl.set(func() {
		ctl.onSend = func(ctx context.Context, _ any, _ func() error) error {
			entered <- struct{}{}
			return g.wait(ctx)
		}
	})

	const budget = 300 * time.Millisecond
	w := newTestWorker(t, f.addr, Options{
		Concurrency: 1, ShutdownTimeout: budget, DialOptions: []grpc.DialOption{ctl.dialOption()},
	}, sp.run, "q")
	r := runWorker(t, w)

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the subscription was never sent")
	}
	start := time.Now()
	r.stop()
	require.NoError(t, r.wait(t))
	require.Less(t, time.Since(start), 2*budget)
}

func TestBlockedCreditSendStopsAtTheDeadline(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	sp := newSpy(t, false)
	g := newGate(t)

	var sends atomic.Int64
	entered := make(chan struct{}, 1)
	ctl.set(func() {
		ctl.onSend = func(ctx context.Context, _ any, forward func() error) error {
			if sends.Add(1) == 1 {
				return forward() // let the subscription through
			}
			select {
			case entered <- struct{}{}:
			default:
			}
			return g.wait(ctx)
		}
	})

	const budget = 300 * time.Millisecond
	w := newTestWorker(t, f.addr, Options{
		Concurrency: 1, ShutdownTimeout: budget, DialOptions: []grpc.DialOption{ctl.dialOption()},
	}, sp.run, "q")
	r := runWorker(t, w)

	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the freed token was never advertised")
	}

	start := time.Now()
	r.stop()
	require.NoError(t, r.wait(t))
	require.Less(t, time.Since(start), 2*budget)
}

func TestForcedShutdownUsesOneBudgetForTransportAndDeliveries(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sp := newSpy(t, true)

	// A broker that never answers the half-close, combined with a delivery
	// that never finishes. Two sequential budgets would take twice as long,
	// which is how a container gets killed mid-handler.
	f.set(func() {
		f.onLease = func(ctx context.Context, s *fakeStream) error {
			<-ctx.Done()
			return status.FromContextError(ctx.Err()).Err()
		}
	})

	const budget = 400 * time.Millisecond
	w := newTestWorker(t, f.addr, Options{Concurrency: 1, ShutdownTimeout: budget}, sp.run, "q")
	r := runWorker(t, w)

	s := f.awaitStream(t, ctx)
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	sp.await(t, ctx)

	start := time.Now()
	r.stop()
	err := r.wait(t)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 active deliveries")
	require.GreaterOrEqual(t, elapsed, budget, "the worker waits out its budget before abandoning")
	require.Less(t, elapsed, 2*budget, "there is no second timeout after stream teardown")
}

func TestReconcileReturnsUnspentCreditExactlyOnce(t *testing.T) {
	pool := newCapacity(4, quietLogger())
	s := &session{pool: pool, log: quietLogger()}

	require.Equal(t, 4, pool.takeAll())
	s.reserve(4)
	require.True(t, s.spend(), "one delivery arrived against the credit")

	s.reconcile()
	require.Equal(t, 3, len(pool.free), "the delivery's own token is returned by its runner, not by reconcile")

	s.reconcile()
	s.reconcile()
	require.Equal(t, 3, len(pool.free), "reconciling twice must not manufacture tokens")
}

func TestSpendRefusesCreditItDoesNotHave(t *testing.T) {
	s := &session{pool: newCapacity(1, quietLogger()), log: quietLogger()}
	s.reserve(1)
	require.True(t, s.spend())
	require.False(t, s.spend(), "a delivery past the credit is a protocol violation, not a token")
}

func TestParseDeliveryRejectsMalformedRows(t *testing.T) {
	valid := func() *hopperv1.Job { return newFixture("q").proto() }

	tests := []struct {
		name    string
		corrupt func(*hopperv1.Job)
		want    string
	}{
		{"bad job id", func(j *hopperv1.Job) { j.Id = "not-a-uuid" }, "job id"},
		{"empty queue", func(j *hopperv1.Job) { j.Queue = "" }, "no queue"},
		{"not leased", func(j *hopperv1.Job) { j.State = hopperv1.JobState_JOB_STATE_AVAILABLE }, "not leased"},
		{"absent lease id", func(j *hopperv1.Job) { j.LeaseId = nil }, "no lease id"},
		{"bad lease id", func(j *hopperv1.Job) { s := "nope"; j.LeaseId = &s }, "lease id"},
		{"nil lease id", func(j *hopperv1.Job) { s := uuid.Nil.String(); j.LeaseId = &s }, "nil lease id"},
		{"absent leased_at", func(j *hopperv1.Job) { j.LeasedAt = nil }, "leased_at is absent"},
		{"absent lease_expires", func(j *hopperv1.Job) { j.LeaseExpires = nil }, "lease_expires is absent"},
		{"invalid leased_at", func(j *hopperv1.Job) {
			j.LeasedAt = &timestamppb.Timestamp{Seconds: -62135596801}
		}, "leased_at is invalid"},
		{"expiry before lease", func(j *hopperv1.Job) {
			j.LeaseExpires = timestamppb.New(j.GetLeasedAt().AsTime().Add(-time.Second))
		}, "lease of -1s"},
		{"zero-length lease", func(j *hopperv1.Job) { j.LeaseExpires = j.GetLeasedAt() }, "lease of 0s"},
		{"absent created_at", func(j *hopperv1.Job) { j.CreatedAt = nil }, "created_at is absent"},
		{"no attempt", func(j *hopperv1.Job) { j.Attempts = 0 }, "attempt 0"},
		{"no max attempts", func(j *hopperv1.Job) { j.MaxAttempts = 0 }, "max attempts 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			j := valid()
			tc.corrupt(j)
			_, err := parseDelivery(j)
			require.ErrorContains(t, err, tc.want)
		})
	}

	_, err := parseDelivery(nil)
	require.ErrorContains(t, err, "no job")

	d, err := parseDelivery(valid())
	require.NoError(t, err)
	require.NotZero(t, d.receivedAt)
	require.Equal(t, 30*time.Second, d.grant)
}
