package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// This file is the SDK's test broker. It is a real gRPC server speaking the
// real generated service, so the SDK under test goes through actual HTTP/2
// framing, status codes and stream semantics — but every response is scripted
// by the test rather than by a database.
//
// Two rules shape it. Nothing in a server goroutine may call require or
// FailNow: FailNow is runtime.Goexit, which would kill that goroutine and turn
// a failed assertion into a hang. And every wait takes a context, so a script
// that goes wrong fails with a message naming what it was waiting for instead
// of running out the test binary's ten-minute clock.

// barrier is the default budget for a harness wait. It is long enough that a
// loaded machine does not trip it and short enough to beat `go test`'s timeout,
// so a stuck test reports which barrier it died on.
const barrier = 10 * time.Second

// waitCtx returns a context bounded by the default barrier.
func waitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), barrier)
	t.Cleanup(cancel)
	return ctx
}

// fake is a scriptable hopper broker.
type fake struct {
	hopperv1.UnimplementedHopperServiceServer

	addr string
	srv  *grpc.Server
	// wg counts RPC handlers so cleanup can prove they all returned. A handler
	// that outlives its test is a leak the next test would pay for.
	wg sync.WaitGroup

	// streams publishes each accepted lease stream to the test goroutine,
	// which then drives it. It is buffered well past what any test opens so a
	// server handler never blocks trying to announce itself.
	streams chan *fakeStream

	mu       sync.Mutex
	started  map[string]int
	finished map[string]int
	subs     []*hopperv1.Subscription
	acks     []*hopperv1.AckRequest
	nacks    []*hopperv1.NackRequest
	beats    []*hopperv1.HeartbeatRequest

	// Hooks are read under mu so a test can swap one mid-run — gating
	// heartbeats after the first, say — without racing the server.
	onLease     func(context.Context, *fakeStream) error
	onSubscribe func(context.Context, *hopperv1.Subscription) error
	onHeartbeat func(context.Context, *hopperv1.HeartbeatRequest) error
	onAck       func(context.Context, *hopperv1.AckRequest) error
	onNack      func(context.Context, *hopperv1.NackRequest) error
}

// startFake serves a fake broker on loopback and returns it. Cleanup stops the
// server, which cancels every handler context, and then proves the handlers
// actually returned.
func startFake(t *testing.T) *fake {
	t.Helper()

	f := &fake{
		streams:  make(chan *fakeStream, 32),
		started:  map[string]int{},
		finished: map[string]int{},
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f.addr = lis.Addr().String()
	f.srv = grpc.NewServer()
	hopperv1.RegisterHopperServiceServer(f.srv, f)

	served := make(chan struct{})
	go func() { defer close(served); _ = f.srv.Serve(lis) }()

	t.Cleanup(func() {
		f.srv.Stop()
		<-served
		joined := make(chan struct{})
		go func() { defer close(joined); f.wg.Wait() }()
		select {
		case <-joined:
		case <-time.After(barrier):
			t.Error("fake broker RPC handlers did not return after Stop")
		}
	})
	return f
}

// dial connects to the fake with the SDK's own defaults plus any extra options
// a test needs, such as a stream interceptor.
func (f *fake) dial(t *testing.T, opts ...grpc.DialOption) hopperv1.HopperServiceClient {
	t.Helper()
	base := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(f.addr, append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return hopperv1.NewHopperServiceClient(conn)
}

// enter records an arrival and returns the function that records completion.
// The two are separate because they answer different questions: whether the
// SDK sent a call at all, and whether it waited for the answer. A test that
// blocks a heartbeat needs to see the arrival while the completion is still
// outstanding.
func (f *fake) enter(method string) func() {
	f.wg.Add(1)
	f.mu.Lock()
	f.started[method]++
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		f.finished[method]++
		f.mu.Unlock()
		f.wg.Done()
	}
}

// counts returns how many calls to method arrived and how many completed.
func (f *fake) counts(method string) (started, finished int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started[method], f.finished[method]
}

// setHook installs a hook under the lock. Each setter takes the field by
// pointer so there is one implementation rather than four.
func (f *fake) set(apply func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	apply()
}

func (f *fake) Heartbeat(ctx context.Context, req *hopperv1.HeartbeatRequest) (*hopperv1.HeartbeatResponse, error) {
	defer f.enter("Heartbeat")()
	f.mu.Lock()
	f.beats = append(f.beats, proto.Clone(req).(*hopperv1.HeartbeatRequest))
	hook := f.onHeartbeat
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, req); err != nil {
			return nil, err
		}
	}
	return &hopperv1.HeartbeatResponse{}, nil
}

func (f *fake) Ack(ctx context.Context, req *hopperv1.AckRequest) (*hopperv1.AckResponse, error) {
	defer f.enter("Ack")()
	f.mu.Lock()
	f.acks = append(f.acks, proto.Clone(req).(*hopperv1.AckRequest))
	hook := f.onAck
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, req); err != nil {
			return nil, err
		}
	}
	return &hopperv1.AckResponse{}, nil
}

func (f *fake) Nack(ctx context.Context, req *hopperv1.NackRequest) (*hopperv1.NackResponse, error) {
	defer f.enter("Nack")()
	f.mu.Lock()
	f.nacks = append(f.nacks, proto.Clone(req).(*hopperv1.NackRequest))
	hook := f.onNack
	f.mu.Unlock()
	if hook != nil {
		if err := hook(ctx, req); err != nil {
			return nil, err
		}
	}
	return &hopperv1.NackResponse{}, nil
}

// Recorded requests, as copies. A caller that mutated one of these would be
// editing the evidence.
func (f *fake) ackRequests() []*hopperv1.AckRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneAll(f.acks)
}

func (f *fake) nackRequests() []*hopperv1.NackRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneAll(f.nacks)
}

func (f *fake) heartbeatRequests() []*hopperv1.HeartbeatRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneAll(f.beats)
}

func (f *fake) subscriptions() []*hopperv1.Subscription {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneAll(f.subs)
}

func cloneAll[T proto.Message](in []T) []T {
	out := make([]T, len(in))
	for i, m := range in {
		out[i] = proto.Clone(m).(T)
	}
	return out
}

// LeaseJobs accepts the subscription, starts a per-stream credit reader, and
// hands the stream to the test's script. The default script idles until the
// client half-closes or the transport is cancelled, which is what an unloaded
// broker looks like to a worker with nothing to do.
func (f *fake) LeaseJobs(srv hopperv1.HopperService_LeaseJobsServer) error {
	defer f.enter("LeaseJobs")()
	ctx := srv.Context()

	first, err := srv.Recv()
	if err != nil {
		return err
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return status.Error(codes.InvalidArgument, "the first message must be a subscription")
	}

	f.mu.Lock()
	f.subs = append(f.subs, proto.Clone(sub).(*hopperv1.Subscription))
	onSubscribe, onLease := f.onSubscribe, f.onLease
	f.mu.Unlock()

	if onSubscribe != nil {
		if err := onSubscribe(ctx, sub); err != nil {
			return err
		}
	}

	s := &fakeStream{
		srv:    srv,
		sub:    proto.Clone(sub).(*hopperv1.Subscription),
		credit: uint64(sub.GetCredit()),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	go s.read()

	select {
	case f.streams <- s:
	default:
		return status.Error(codes.ResourceExhausted, "the fake broker's stream buffer is full")
	}

	if onLease != nil {
		return onLease(ctx, s)
	}
	return s.idle(ctx)
}

// awaitStream returns the next lease stream the fake accepted.
func (f *fake) awaitStream(t *testing.T, ctx context.Context) *fakeStream {
	t.Helper()
	select {
	case s := <-f.streams:
		return s
	case <-ctx.Done():
		t.Fatalf("no lease stream opened: %v", ctx.Err())
		return nil
	}
}

// fakeStream is one accepted lease stream. Credit is per-stream on purpose: a
// broker-wide credit channel would let a reconnected stream consume the wakeup
// meant for its predecessor, which is exactly the confusion the reconnect
// tests exist to detect.
type fakeStream struct {
	srv hopperv1.HopperService_LeaseJobsServer
	sub *hopperv1.Subscription

	mu      sync.Mutex
	credit  uint64
	credits []uint32 // every credit message, in arrival order
	recvErr error

	wake chan struct{} // coalescing credit notification
	done chan struct{} // closed when the reader stops
}

// read consumes credit messages until the client half-closes or the transport
// ends. It never blocks on delivering a notification, so a test that is not
// watching credit cannot stall the stream.
func (s *fakeStream) read() {
	defer close(s.done)
	for {
		msg, err := s.srv.Recv()
		if err != nil {
			s.mu.Lock()
			s.recvErr = err
			s.mu.Unlock()
			return
		}
		c, ok := msg.Msg.(*hopperv1.LeaseJobsRequest_Credit)
		if !ok {
			s.mu.Lock()
			s.recvErr = status.Error(codes.InvalidArgument, "only credit messages may follow subscribe")
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		s.credit += uint64(c.Credit)
		s.credits = append(s.credits, c.Credit)
		s.mu.Unlock()
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// idle waits for half-close or cancellation. EOF is a clean end: it is what a
// graceful worker shutdown looks like from this side.
func (s *fakeStream) idle(ctx context.Context) error {
	select {
	case <-s.done:
		return s.endErr()
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (s *fakeStream) endErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if errors.Is(s.recvErr, io.EOF) {
		return nil
	}
	return s.recvErr
}

// awaitEOF waits for the client's half-close.
func (s *fakeStream) awaitEOF(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if errors.Is(s.recvErr, io.EOF) {
			return nil
		}
		return fmt.Errorf("stream ended without a half-close: %w", s.recvErr)
	case <-ctx.Done():
		return fmt.Errorf("waiting for half-close: %w", ctx.Err())
	}
}

// awaitCredit blocks until the worker has advertised at least n free slots.
func (s *fakeStream) awaitCredit(ctx context.Context, n uint64) error {
	for {
		s.mu.Lock()
		have := s.credit
		s.mu.Unlock()
		if have >= n {
			return nil
		}
		select {
		case <-s.wake:
		case <-s.done:
			return fmt.Errorf("stream ended holding %d credit, wanted %d: %w", have, n, s.endErr())
		case <-ctx.Done():
			return fmt.Errorf("waiting for %d credit, have %d: %w", n, have, ctx.Err())
		}
	}
}

// creditTotal is the credit outstanding right now.
func (s *fakeStream) creditTotal() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.credit
}

// creditMessages is every credit update in arrival order, excluding the
// subscription's initial grant.
func (s *fakeStream) creditMessages() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint32(nil), s.credits...)
}

// spend removes credit that this stream is about to use.
func (s *fakeStream) spend(n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.credit < n {
		return fmt.Errorf("spending %d credit with only %d outstanding", n, s.credit)
	}
	s.credit -= n
	return nil
}

// send writes a delivery without consulting credit, which is how a test
// manufactures the over-delivery a well-behaved broker would never commit.
func (s *fakeStream) send(job *hopperv1.Job) error {
	return s.srv.Send(&hopperv1.LeaseJobsResponse{Job: job})
}

// deliver waits for a credit, spends it, and sends. This is the honest path.
func (s *fakeStream) deliver(ctx context.Context, job *hopperv1.Job) error {
	if err := s.awaitCredit(ctx, 1); err != nil {
		return err
	}
	if err := s.spend(1); err != nil {
		return err
	}
	return s.send(job)
}

// delivery builds a well-formed leased row. Tests mutate the fields they care
// about, so a malformed-delivery case says exactly what is wrong with it.
type delivery struct {
	ID, LeaseID uuid.UUID
	Queue       string
	Payload     []byte
	Attempt     int32
	MaxAttempts int32
	Grant       time.Duration
	LeasedAt    time.Time
}

func newDelivery(queue string) *delivery {
	return &delivery{
		ID:          uuid.New(),
		LeaseID:     uuid.New(),
		Queue:       queue,
		Payload:     []byte(`{}`),
		Attempt:     1,
		MaxAttempts: 5,
		Grant:       30 * time.Second,
		LeasedAt:    time.Now(),
	}
}

// proto renders the delivery the way the broker's own conversion would.
func (d *delivery) proto() *hopperv1.Job {
	leaseID := d.LeaseID.String()
	j := &hopperv1.Job{
		Id:           d.ID.String(),
		Queue:        d.Queue,
		Payload:      d.Payload,
		State:        hopperv1.JobState_JOB_STATE_LEASED,
		Attempts:     d.Attempt,
		MaxAttempts:  d.MaxAttempts,
		LeaseId:      &leaseID,
		LeasedAt:     timestamppb.New(d.LeasedAt),
		LeaseExpires: timestamppb.New(d.LeasedAt.Add(d.Grant)),
		RunAt:        timestamppb.New(d.LeasedAt),
		CreatedAt:    timestamppb.New(d.LeasedAt),
		UpdatedAt:    timestamppb.New(d.LeasedAt),
	}
	if d.LeaseID == uuid.Nil {
		j.LeaseId = nil
	}
	return j
}

// run is a Worker running in the background, with a result the test can read
// as often as it likes. The completion signal is close-only so cleanup can
// wait on it without consuming the error a test still wants to assert on.
type run struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error // written before done is closed; read only after
}

// runWorker starts w.Run and registers the cleanup that stops it. Cleanup
// cancels and requires that Run actually returned, because a Run that never
// returns is the failure most worth catching and the easiest to miss.
func runWorker(t *testing.T, w *Worker) *run {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel, done: make(chan struct{})}

	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(barrier):
			t.Error("Run did not return after its context was cancelled")
		}
	})

	go func() {
		defer close(r.done)
		r.err = w.Run(ctx)
	}()
	return r
}

// stop starts a graceful drain by cancelling Run's context.
func (r *run) stop() { r.cancel() }

// wait returns Run's result, failing the test if it has not returned yet. It
// is safe to call more than once.
func (r *run) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(barrier):
		t.Fatal("Run did not return within the barrier")
		return nil
	}
}

// stopAndWait is the common pairing: drain, then report what Run returned.
func (r *run) stopAndWait(t *testing.T) error {
	t.Helper()
	r.stop()
	return r.wait(t)
}
