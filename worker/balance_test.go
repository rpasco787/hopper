package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
)

// twoBrokers resolves one target to both fakes, the way DNS resolves a
// headless Service or a shared Compose alias to every broker.
func twoBrokers(a, b *fake) (string, grpc.DialOption) {
	r := manual.NewBuilderWithScheme("hoppertest")
	r.InitialState(resolver.State{Endpoints: []resolver.Endpoint{
		{Addresses: []resolver.Address{{Addr: a.addr}}},
		{Addresses: []resolver.Address{{Addr: b.addr}}},
	}})
	return "hoppertest:///brokers", grpc.WithResolvers(r)
}

// TestReconnectingStreamsSpreadAcrossBrokers: both fakes end every lease
// stream at once, as a broker does when a stream reaches its maximum age, so
// the worker keeps reopening it. Through round robin the reopened streams
// reach both brokers; under pick_first they would all go back to the first.
func TestReconnectingStreamsSpreadAcrossBrokers(t *testing.T) {
	a, b := startFake(t), startFake(t)
	for _, f := range []*fake{a, b} {
		f.set(func() {
			f.onLease = func(context.Context, *fakeStream) error {
				return status.Error(codes.Unavailable, "lease stream reached its maximum age; reconnect")
			}
		})
	}
	target, resolve := twoBrokers(a, b)
	sp := newSpy(t, false)
	w := newTestWorker(t, target, Options{Concurrency: 1, DialOptions: []grpc.DialOption{resolve}}, sp.run, "q")
	r := runWorker(t, w)

	require.Eventually(t, func() bool {
		return len(a.subscriptions()) > 0 && len(b.subscriptions()) > 0
	}, barrier, 10*time.Millisecond, "every lease stream went to one broker")
	require.NoError(t, r.stopAndWait(t))
}

// TestRoutineAgeOutsDoNotGrowTheReconnectDelay: a broker ends every lease
// stream at its maximum age, and a worker on a quiet queue delivers nothing in
// between. A stream that stayed up that long shows the broker is healthy, so
// reconnecting must not climb the backoff toward its 5s cap; otherwise every
// routine age-out leaves an idle worker with no stream for seconds.
func TestRoutineAgeOutsDoNotGrowTheReconnectDelay(t *testing.T) {
	const lived = 60 * time.Millisecond
	f := startFake(t)
	var mu sync.Mutex
	var opened []time.Time
	f.set(func() {
		f.onLease = func(ctx context.Context, _ *fakeStream) error {
			mu.Lock()
			opened = append(opened, time.Now())
			mu.Unlock()
			select {
			case <-time.After(lived):
			case <-ctx.Done():
				return ctx.Err()
			}
			return status.Error(codes.Unavailable, "lease stream reached its maximum age; reconnect")
		}
	})
	sp := newSpy(t, false)
	w := newTestWorker(t, f.addr, Options{Concurrency: 1}, sp.run, "q")
	w.healthySession = lived / 2 // in production, backoffCap
	r := runWorker(t, w)

	const streams = 8
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(opened) >= streams
	}, barrier, 10*time.Millisecond, "the worker reopened its stream too slowly: the reconnect delay grew")
	require.NoError(t, r.stopAndWait(t))

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < streams; i++ {
		// lived, plus one un-grown delay (at most 125ms), plus scheduling slack.
		gap := opened[i].Sub(opened[i-1])
		require.Less(t, gap, lived+400*time.Millisecond, "reopen %d waited %s: the delay grew", i, gap)
	}
}
