package worker

import (
	"context"
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
