package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
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
