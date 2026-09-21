package broker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// streamTracker watches one lease handler from the server side. A client-side
// Recv error only proves the client's view of the stream ended; whether the
// handler goroutine itself returned is visible only here.
type streamTracker struct {
	once    sync.Once
	entered chan struct{}
	exited  chan error // the handler's return value
}

func newStreamTracker() *streamTracker {
	return &streamTracker{entered: make(chan struct{}), exited: make(chan error, 1)}
}

func (tr *streamTracker) option() grpc.ServerOption {
	return grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		tr.once.Do(func() { close(tr.entered) })
		err := h(srv, ss)
		tr.exited <- err
		return err
	})
}

func (tr *streamTracker) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-tr.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the LeaseJobs handler never started")
	}
}

func (tr *streamTracker) waitExited(t *testing.T) error {
	t.Helper()
	select {
	case err := <-tr.exited:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the LeaseJobs handler did not return")
		return nil
	}
}

// signalHandler is a slog.Handler that closes a channel when a record with the
// given message is logged. It gives a test a synchronisation point inside the
// handler without sleeping.
type signalHandler struct {
	msg  string
	once *sync.Once
	ch   chan struct{}
}

func newSignalLogger(msg string) (*slog.Logger, <-chan struct{}) {
	h := signalHandler{msg: msg, once: new(sync.Once), ch: make(chan struct{})}
	return slog.New(h), h.ch
}

func (h signalHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h signalHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.once.Do(func() { close(h.ch) })
	}
	return nil
}
func (h signalHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h signalHandler) WithGroup(string) slog.Handler      { return h }

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// fakeLeaseStream drives Service.LeaseJobs directly, without gRPC, so a test
// controls exactly what Recv and Send do. Real transports cannot be told to
// block a Send at a chosen instant.
//
// ctx stands in for the transport context: gRPC cancels it only after the
// handler returns, so tests cancel it themselves, and only then. Send and Recv
// both block on it, as the real ones do, which is what lets a test assert that
// the goroutines the handler leaves behind eventually exit.
type fakeLeaseStream struct {
	grpc.ServerStream // nil; only Context is implemented, so any other call panics

	ctx    context.Context
	cancel context.CancelFunc
	in     chan *hopperv1.LeaseJobsRequest // closing it is a client half-close
	sendFn func(context.Context, *hopperv1.LeaseJobsResponse) error

	sending, receiving atomic.Int32 // calls currently inside Send / Recv
}

func newFakeLeaseStream(t *testing.T, sendFn func(context.Context, *hopperv1.LeaseJobsResponse) error) *fakeLeaseStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &fakeLeaseStream{
		ctx:    ctx,
		cancel: cancel,
		// Buffered so a test can queue any number of messages without blocking
		// on a handler that is deliberately stuck in Send.
		in:     make(chan *hopperv1.LeaseJobsRequest, 256),
		sendFn: sendFn,
	}
}

func (f *fakeLeaseStream) Context() context.Context { return f.ctx }

func (f *fakeLeaseStream) Recv() (*hopperv1.LeaseJobsRequest, error) {
	f.receiving.Add(1)
	defer f.receiving.Add(-1)
	select {
	case msg, ok := <-f.in:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	case <-f.ctx.Done():
		return nil, status.FromContextError(f.ctx.Err()).Err()
	}
}

func (f *fakeLeaseStream) Send(resp *hopperv1.LeaseJobsResponse) error {
	f.sending.Add(1)
	defer f.sending.Add(-1)
	return f.sendFn(f.ctx, resp)
}

// idle reports that no Send or Recv is still running.
func (f *fakeLeaseStream) idle() bool { return f.sending.Load() == 0 && f.receiving.Load() == 0 }

func subscribeMsg(credit uint32) *hopperv1.LeaseJobsRequest {
	return &hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "w", Credit: credit,
		}},
	}
}

func creditMsg(n uint32) *hopperv1.LeaseJobsRequest {
	return &hopperv1.LeaseJobsRequest{Msg: &hopperv1.LeaseJobsRequest_Credit{Credit: n}}
}
