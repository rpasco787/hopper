package worker

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// streamControl is a client-side seam for the transport failures that matter
// to this SDK: a stream that will not open, a Send that fails, a Send that
// blocks, and a delivery that arrives before the Send that paid for it has
// returned.
//
// It is an interceptor rather than an attempt to fill real HTTP/2 flow-control
// windows. Filling a window works, but only sometimes and only at sizes that
// change with the library; a hook happens exactly when the test says it does.
type streamControl struct {
	mu      sync.Mutex
	onOpen  func(ctx context.Context) error
	onSend  func(ctx context.Context, m any, forward func() error) error
	onClose func(ctx context.Context, forward func() error) error
}

func (c *streamControl) dialOption() grpc.DialOption {
	return grpc.WithStreamInterceptor(c.intercept)
}

func (c *streamControl) set(apply func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	apply()
}

func (c *streamControl) hooks() (
	open func(context.Context) error,
	send func(context.Context, any, func() error) error,
	closeSend func(context.Context, func() error) error,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onOpen, c.onSend, c.onClose
}

func (c *streamControl) intercept(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if open, _, _ := c.hooks(); open != nil {
		if err := open(ctx); err != nil {
			return nil, err
		}
	}
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return &controlledStream{ClientStream: cs, ctl: c, ctx: ctx}, nil
}

type controlledStream struct {
	grpc.ClientStream
	ctl *streamControl
	ctx context.Context
}

func (s *controlledStream) SendMsg(m any) error {
	_, send, _ := s.ctl.hooks()
	if send == nil {
		return s.ClientStream.SendMsg(m)
	}
	return send(s.ctx, m, func() error { return s.ClientStream.SendMsg(m) })
}

func (s *controlledStream) CloseSend() error {
	_, _, closeSend := s.ctl.hooks()
	if closeSend == nil {
		return s.ClientStream.CloseSend()
	}
	return closeSend(s.ctx, func() error { return s.ClientStream.CloseSend() })
}

// gate is a one-shot release. Its cleanup is registered at construction, before
// anything can block on it, so a failing test releases whatever it stranded
// instead of hanging the package.
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate(t *testing.T) *gate {
	t.Helper()
	g := &gate{ch: make(chan struct{})}
	t.Cleanup(g.release)
	return g
}

func (g *gate) release() { g.once.Do(func() { close(g.ch) }) }

// wait blocks until the gate opens or ctx ends. Scripts that block must use
// this: a bare channel receive would ignore transport cancellation, and then
// the harness itself would be the thing that cannot be shut down.
func (g *gate) wait(ctx context.Context) error {
	select {
	case <-g.ch:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// subscribe opens a lease stream and sends the subscription.
func subscribe(t *testing.T, ctx context.Context, c hopperv1.HopperServiceClient,
	sub *hopperv1.Subscription) hopperv1.HopperService_LeaseJobsClient {
	t.Helper()
	stream, err := c.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: sub},
	}))
	return stream
}

func creditMsg(n uint32) *hopperv1.LeaseJobsRequest {
	return &hopperv1.LeaseJobsRequest{Msg: &hopperv1.LeaseJobsRequest_Credit{Credit: n}}
}

func TestFakeUnaryDefaultsSucceedAndRecordCopies(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	d := newFixture("q")
	_, err := c.Ack(ctx, &hopperv1.AckRequest{Id: d.ID.String(), LeaseId: d.LeaseID.String()})
	require.NoError(t, err)
	_, err = c.Nack(ctx, &hopperv1.NackRequest{Id: d.ID.String(), LeaseId: d.LeaseID.String(), Error: "boom"})
	require.NoError(t, err)
	_, err = c.Heartbeat(ctx, &hopperv1.HeartbeatRequest{Id: d.ID.String(), LeaseId: d.LeaseID.String()})
	require.NoError(t, err)

	started, finished := f.counts("Ack")
	require.Equal(t, 1, started)
	require.Equal(t, 1, finished)

	acks := f.ackRequests()
	require.Len(t, acks, 1)
	require.Equal(t, d.ID.String(), acks[0].GetId())
	acks[0].Id = "tampered"
	require.Equal(t, d.ID.String(), f.ackRequests()[0].GetId(), "snapshots are copies")

	require.Equal(t, "boom", f.nackRequests()[0].GetError())
	require.Equal(t, d.LeaseID.String(), f.heartbeatRequests()[0].GetLeaseId())
}

func TestFakeUnaryHookOverridesTheDefault(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	f.set(func() {
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			return status.Error(codes.Aborted, "lease lost")
		}
	})
	_, err := c.Ack(ctx, &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
	require.Equal(t, codes.Aborted, status.Code(err))

	started, finished := f.counts("Ack")
	require.Equal(t, 1, started)
	require.Equal(t, 1, finished, "a hook that returns still completes the call")
}

func TestHalfCloseProducesEOF(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	stream := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 2})
	s := f.awaitStream(t, ctx)
	require.Equal(t, uint64(2), s.creditTotal(), "the subscription's credit counts")

	require.NoError(t, stream.CloseSend())
	require.NoError(t, s.awaitEOF(ctx))

	// The default script treats EOF as a clean end, so the client sees the
	// stream finish rather than an error.
	_, err := stream.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestCreditAccountingIsPerStream(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	first := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 1})
	s1 := f.awaitStream(t, ctx)
	second := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 0})
	s2 := f.awaitStream(t, ctx)

	require.NoError(t, first.Send(creditMsg(3)))
	require.NoError(t, s1.awaitCredit(ctx, 4))
	require.Equal(t, uint64(0), s2.creditTotal(), "one stream's credit is not another's")

	require.NoError(t, second.Send(creditMsg(2)))
	require.NoError(t, s2.awaitCredit(ctx, 2))
	require.Equal(t, []uint32{3}, s1.creditMessages())
	require.Equal(t, []uint32{2}, s2.creditMessages())
}

func TestDeliverySpendsCreditAndOverDeliveryDoesNot(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	stream := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 1})
	s := f.awaitStream(t, ctx)

	d := newFixture("q")
	require.NoError(t, s.deliver(ctx, d.proto()))
	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, d.ID.String(), got.GetJob().GetId())
	require.Equal(t, uint64(0), s.creditTotal())

	require.ErrorContains(t, s.spend(1), "only 0 outstanding")

	// send bypasses credit, which is how a test manufactures over-delivery.
	require.NoError(t, s.send(newFixture("q").proto()))
	_, err = stream.Recv()
	require.NoError(t, err)
}

func TestNonresponsiveHalfCloseStillRespectsCancellation(t *testing.T) {
	outer := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	// A script that deliberately ignores the client's half-close, the way a
	// broker wedged on a database call would.
	observed := make(chan error, 1)
	f.set(func() {
		f.onLease = func(ctx context.Context, s *fakeStream) error {
			<-ctx.Done()
			observed <- context.Cause(ctx)
			return status.FromContextError(ctx.Err()).Err()
		}
	})

	ctx, cancel := context.WithCancel(outer)
	defer cancel()
	stream := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 1})
	s := f.awaitStream(t, ctx)

	require.NoError(t, stream.CloseSend())
	require.NoError(t, s.awaitEOF(outer), "the reader sees EOF even though the script ignores it")

	started, finished := f.counts("LeaseJobs")
	require.Equal(t, 1, started)
	require.Equal(t, 0, finished, "the handler is still stuck, which is the point")

	cancel()
	select {
	case err := <-observed:
		require.ErrorIs(t, err, context.Canceled)
	case <-outer.Done():
		t.Fatal("the stuck script never observed transport cancellation")
	}
}

func TestStreamControlFailsToOpen(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	c := f.dial(t, ctl.dialOption())

	ctl.set(func() {
		ctl.onOpen = func(context.Context) error { return status.Error(codes.Unavailable, "no route") }
	})
	_, err := c.LeaseJobs(ctx)
	require.Equal(t, codes.Unavailable, status.Code(err))

	started, _ := f.counts("LeaseJobs")
	require.Equal(t, 0, started, "the stream never reached the broker")
}

func TestStreamControlBlocksOpeningUntilCancelled(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	c := f.dial(t, ctl.dialOption())

	g := newGate(t)
	ctl.set(func() { ctl.onOpen = func(ctx context.Context) error { return g.wait(ctx) } })

	opening, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, err := c.LeaseJobs(opening)
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-ctx.Done():
		t.Fatal("a blocked stream open ignored cancellation")
	}
}

func TestStreamControlFailsASend(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	c := f.dial(t, ctl.dialOption())

	sendErr := errors.New("write failed")
	ctl.set(func() {
		ctl.onSend = func(context.Context, any, func() error) error { return sendErr }
	})
	stream, err := c.LeaseJobs(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, stream.Send(&hopperv1.LeaseJobsRequest{}), sendErr)

	started, _ := f.counts("LeaseJobs")
	require.Equal(t, 0, started, "a failed Send delivers nothing")
}

func TestStreamControlDeliversBeforeASendReturns(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	c := f.dial(t, ctl.dialOption())

	stream := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 0})
	s := f.awaitStream(t, ctx)

	// From here, a Send reaches the broker and only then blocks. That is the
	// ordering the SDK has to survive: the answer to a credit message can
	// arrive while the code that sent it is still inside Send.
	g := newGate(t)
	ctl.set(func() {
		ctl.onSend = func(ctx context.Context, _ any, forward func() error) error {
			if err := forward(); err != nil {
				return err
			}
			return g.wait(ctx)
		}
	})

	sent := make(chan error, 1)
	go func() { sent <- stream.Send(creditMsg(1)) }()

	require.NoError(t, s.awaitCredit(ctx, 1), "the credit arrived although Send has not returned")
	d := newFixture("q")
	require.NoError(t, s.deliver(ctx, d.proto()))

	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, d.ID.String(), got.GetJob().GetId())

	select {
	case err := <-sent:
		t.Fatalf("Send returned %v before the gate opened", err)
	default:
	}

	g.release()
	select {
	case err := <-sent:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Send never returned after the gate opened")
	}
}

func TestStreamControlBlocksCloseSend(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	ctl := &streamControl{}
	c := f.dial(t, ctl.dialOption())

	g := newGate(t)
	ctl.set(func() {
		ctl.onClose = func(ctx context.Context, forward func() error) error {
			if err := g.wait(ctx); err != nil {
				return err
			}
			return forward()
		}
	})

	stream := subscribe(t, ctx, c, &hopperv1.Subscription{Queues: []string{"q"}, WorkerId: "w", Credit: 1})
	s := f.awaitStream(t, ctx)

	closed := make(chan error, 1)
	go func() { closed <- stream.CloseSend() }()
	g.release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("CloseSend never ran")
	}
	require.NoError(t, s.awaitEOF(ctx))
}

func TestSubscriptionRejectionReachesTheClient(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	f.set(func() {
		f.onSubscribe = func(context.Context, *hopperv1.Subscription) error {
			return status.Error(codes.InvalidArgument, "queues is required")
		}
	})
	stream := subscribe(t, ctx, c, &hopperv1.Subscription{WorkerId: "w"})
	_, err := stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestNonSubscriptionFirstMessageIsRejected(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	c := f.dial(t)

	stream, err := c.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(creditMsg(1)))
	_, err = stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
