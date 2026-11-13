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
