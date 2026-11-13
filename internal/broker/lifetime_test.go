package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

type countingGauge struct{ value, incs atomic.Int64 }

func (g *countingGauge) Inc() { g.value.Add(1); g.incs.Add(1) }
func (g *countingGauge) Dec() { g.value.Add(-1) }

func idleStore() *fakeStore {
	return &fakeStore{leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
		return nil, nil
	}}
}

func sendNothing(context.Context, *hopperv1.LeaseJobsResponse) error { return nil }

// runLease starts LeaseJobs on a fake stream and returns its result channel.
func runLease(svc *Service, stream *fakeLeaseStream) <-chan error {
	done := make(chan error, 1)
	go func() { done <- svc.LeaseJobs(stream) }()
	return done
}

func awaitEnd(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the lease stream did not end")
		return nil
	}
}

// TestLeaseStreamsGaugeCountsOpenStreams: the gauge rises once a subscription
// is accepted and falls when the stream ends.
func TestLeaseStreamsGaugeCountsOpenStreams(t *testing.T) {
	t.Parallel()
	g := &countingGauge{}
	svc := NewService(idleStore(), Config{PollInterval: time.Hour, LeaseStreams: g})
	stream := newFakeLeaseStream(t, sendNothing)
	stream.in <- subscribeMsg(1)
	done := runLease(svc, stream)

	require.Eventually(t, func() bool { return g.value.Load() == 1 }, 5*time.Second, time.Millisecond)
	close(stream.in) // a client half-close ends the stream cleanly
	require.NoError(t, awaitEnd(t, done))
	require.Zero(t, g.value.Load())
}

// TestRejectedSubscriptionIsNotCounted: a stream refused at subscription
// never served anything and must not appear in the gauge, even briefly.
func TestRejectedSubscriptionIsNotCounted(t *testing.T) {
	t.Parallel()
	g := &countingGauge{}
	svc := NewService(&fakeStore{}, Config{LeaseStreams: g})
	stream := newFakeLeaseStream(t, sendNothing)
	stream.in <- &hopperv1.LeaseJobsRequest{Msg: &hopperv1.LeaseJobsRequest_Subscribe{
		Subscribe: &hopperv1.Subscription{WorkerId: "w"}, // no queues
	}}
	err := awaitEnd(t, runLease(svc, stream))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, g.incs.Load())
}

func TestJitterStaysWithinTenPercent(t *testing.T) {
	t.Parallel()
	for range 1000 {
		d := jitter(time.Minute)
		require.GreaterOrEqual(t, d, 54*time.Second)
		require.LessOrEqual(t, d, 66*time.Second)
	}
}

// TestLeaseStreamAgesOut: an idle stream (parked after an empty lease) and a
// stream with no credit (waiting for some) both end with UNAVAILABLE once
// they reach their maximum age, so the worker reconnects.
func TestLeaseStreamAgesOut(t *testing.T) {
	t.Parallel()
	for _, credit := range []uint32{0, 1} {
		svc := NewService(idleStore(), Config{LeaseStreamMaxAge: 20 * time.Millisecond, PollInterval: time.Hour})
		stream := newFakeLeaseStream(t, sendNothing)
		stream.in <- subscribeMsg(credit)
		err := awaitEnd(t, runLease(svc, stream))
		require.Equal(t, codes.Unavailable, status.Code(err), "credit %d", credit)
		require.Contains(t, status.Convert(err).Message(), "maximum age", "credit %d", credit)
	}
}

// TestLeaseStreamWithoutMaxAgeStaysOpen: zero means never.
func TestLeaseStreamWithoutMaxAgeStaysOpen(t *testing.T) {
	t.Parallel()
	svc := NewService(idleStore(), Config{PollInterval: time.Hour})
	stream := newFakeLeaseStream(t, sendNothing)
	stream.in <- subscribeMsg(1)
	done := runLease(svc, stream)
	require.Never(t, func() bool { return len(done) > 0 }, 200*time.Millisecond, 10*time.Millisecond)
	close(stream.in)
	require.NoError(t, awaitEnd(t, done))
}

// TestLeaseStreamAgesOutOnlyBetweenBatches: the age timer fires while a batch
// is half sent. The stream must send the rest of the batch first; a leased row
// it never sent would sit leased until the reaper took it back.
func TestLeaseStreamAgesOutOnlyBetweenBatches(t *testing.T) {
	t.Parallel()
	const maxAge = 20 * time.Millisecond
	var leases atomic.Int32
	svc := NewService(&fakeStore{leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
		if leases.Add(1) > 1 {
			return nil, nil
		}
		return []store.Job{leasedJob("q"), leasedJob("q"), leasedJob("q")}, nil
	}}, Config{LeaseStreamMaxAge: maxAge, PollInterval: time.Hour})

	firstSend, release := make(chan struct{}), make(chan struct{})
	var sent atomic.Int32
	stream := newFakeLeaseStream(t, func(ctx context.Context, _ *hopperv1.LeaseJobsResponse) error {
		if sent.Add(1) == 1 {
			close(firstSend)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	stream.in <- subscribeMsg(3)
	done := runLease(svc, stream)

	waitFor(t, firstSend, "the first send")
	time.Sleep(5 * maxAge) // the timer has fired: it is at most 1.1 × maxAge
	close(release)

	err := awaitEnd(t, done)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "maximum age")
	require.Equal(t, int32(3), sent.Load(), "every leased row was sent before the stream ended")
}

type connCounter struct{ n atomic.Int32 }

func (c *connCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (c *connCounter) HandleRPC(context.Context, stats.RPCStats)                         {}
func (c *connCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (c *connCounter) HandleConn(_ context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnBegin); ok {
		c.n.Add(1)
	}
}

// TestMaxConnectionAgeRecyclesConnections: after its age a connection is sent
// GOAWAY, and the client's next call opens a new one.
func TestMaxConnectionAgeRecyclesConnections(t *testing.T) {
	t.Parallel()
	conns := &connCounter{}
	_, conn := startServer(t, &fakeStore{getJobFn: func(context.Context, uuid.UUID) (store.Job, error) {
		return store.Job{}, store.ErrNotFound
	}}, Config{MaxConnectionAge: 100 * time.Millisecond}, grpc.StatsHandler(conns))
	rpc := hopperv1.NewHopperServiceClient(conn)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = rpc.GetJob(ctx, &hopperv1.GetJobRequest{Id: uuid.NewString()})
		return conns.n.Load() >= 2
	}, 5*time.Second, 50*time.Millisecond, "the connection was never recycled")
}

// TestConnectionAgeNeverCutsAHeldBatch: GOAWAY arrives while the stream's
// lease call is stuck, here for a second. That is ten times the stream's own
// age and twenty times the connection's. With unlimited grace the connection
// stays up for the stream, which sends every leased row and then ends itself
// for age. A grace shorter than the hold would cut the transport with three
// rows leased and unsent, and they would wait for the reaper.
func TestConnectionAgeNeverCutsAHeldBatch(t *testing.T) {
	t.Parallel()
	var leases atomic.Int32
	_, conn := startServer(t, &fakeStore{leaseFn: func(ctx context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
		if leases.Add(1) > 1 {
			return nil, nil
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []store.Job{leasedJob("q"), leasedJob("q"), leasedJob("q")}, nil
	}}, Config{MaxConnectionAge: 50 * time.Millisecond, LeaseStreamMaxAge: 100 * time.Millisecond, PollInterval: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := hopperv1.NewHopperServiceClient(conn).LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(subscribeMsg(3)))
	for i := range 3 {
		_, err := stream.Recv()
		require.NoError(t, err, "row %d of the held batch", i+1)
	}
	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "maximum age")
}

// TestClientDeadlineCancelsTheStoreCall: a caller's 100ms deadline reaches the
// store as context cancellation, so a slow query is abandoned with its caller.
func TestClientDeadlineCancelsTheStoreCall(t *testing.T) {
	t.Parallel()
	cancelled := make(chan struct{})
	_, conn := startServer(t, &fakeStore{getJobFn: func(ctx context.Context, _ uuid.UUID) (store.Job, error) {
		<-ctx.Done()
		close(cancelled)
		return store.Job{}, ctx.Err()
	}}, Config{})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := hopperv1.NewHopperServiceClient(conn).GetJob(ctx, &hopperv1.GetJobRequest{Id: uuid.NewString()})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	waitFor(t, cancelled, "the store call to see the deadline")
	require.Less(t, time.Since(start), time.Second)
}
