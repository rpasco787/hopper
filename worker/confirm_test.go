package worker

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// logSink keeps log messages so a test can assert on the events the worker
// process tests in Task 11 will later parse out of stdout.
type logSink struct {
	mu     sync.Mutex
	lines  []string
	errors map[string][]string
}

func (s *logSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *logSink) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *logSink) WithGroup(string) slog.Handler            { return s }

func (s *logSink) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, r.Message)
	if s.errors == nil {
		s.errors = map[string][]string{}
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "err" {
			s.errors[r.Message] = append(s.errors[r.Message], a.Value.String())
		}
		return true
	})
	return nil
}

func (s *logSink) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.lines)
}

func (s *logSink) errorValues(message string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.errors[message])
}

// runHandler starts a worker with one real handler and returns its run and the
// broker's view of its stream. Unlike the Task 3 helpers this uses the real
// delivery runner, so acquisition, execution and confirmation all happen.
func runHandler(t *testing.T, ctx context.Context, f *fake, opts Options,
	queue string, h HandlerFunc) (*run, *fakeStream) {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quietLogger()
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 3 * time.Second
	}
	w := New(f.addr, opts)
	w.Handle(queue, h)
	r := runWorker(t, w)
	return r, f.awaitStream(t, ctx)
}

// awaitCalls waits until method has been called n times.
func awaitCalls(t *testing.T, ctx context.Context, f *fake, method string, n int) {
	t.Helper()
	for {
		if started, _ := f.counts(method); started >= n {
			return
		}
		select {
		case <-time.After(2 * time.Millisecond):
		case <-ctx.Done():
			started, _ := f.counts(method)
			t.Fatalf("%s was called %d times, wanted %d: %v", method, started, n, ctx.Err())
		}
	}
}

func TestASuccessfulHandlerIsAcked(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sink := &logSink{}

	ran := make(chan Job, 1)
	r, s := runHandler(t, ctx, f, Options{Logger: slog.New(sink)}, "q",
		func(_ context.Context, j Job) error { ran <- j; return nil })

	fx := newFixture("q")
	fx.Payload = []byte(`{"ms":1}`)
	require.NoError(t, s.deliver(ctx, fx.proto()))

	select {
	case j := <-ran:
		require.Equal(t, fx.ID, j.ID)
		require.JSONEq(t, `{"ms":1}`, string(j.Payload))
	case <-ctx.Done():
		t.Fatal("the handler never ran")
	}

	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))

	acks := f.ackRequests()
	require.Len(t, acks, 1)
	require.Equal(t, fx.ID.String(), acks[0].GetId())
	require.Equal(t, fx.LeaseID.String(), acks[0].GetLeaseId())
	require.Empty(t, f.nackRequests())

	beats, _ := f.counts("Heartbeat")
	require.GreaterOrEqual(t, beats, 1, "ownership is validated before the handler runs")

	msgs := sink.messages()
	require.Contains(t, msgs, "job started")
	require.Contains(t, msgs, "handler finished")
	require.Contains(t, msgs, "job confirmed")
	require.Less(t, slices.Index(msgs, "job started"), slices.Index(msgs, "handler finished"))
	require.Less(t, slices.Index(msgs, "handler finished"), slices.Index(msgs, "job confirmed"),
		"completion is logged after the broker accepted it, not when user code returned")
}

func TestAFailingHandlerIsNackedWithASanitizedError(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	sink := &logSink{}

	dirty := "boom \x00 " + strings.Repeat("é", maxErrorBytes) + "\xff"
	r, s := runHandler(t, ctx, f, Options{Logger: slog.New(sink)}, "q",
		func(context.Context, Job) error { return errors.New(dirty) })

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Nack", 1)
	require.NoError(t, r.stopAndWait(t))

	nacks := f.nackRequests()
	require.Len(t, nacks, 1)
	msg := nacks[0].GetError()
	require.NotContains(t, msg, "\x00", "a NUL would fail the Nack itself, not the job")
	require.LessOrEqual(t, len(msg), maxErrorBytes)
	require.True(t, strings.HasPrefix(msg, "boom  "))
	require.Empty(t, f.ackRequests())

	logged := sink.errorValues("handler finished")
	require.Len(t, logged, 1)
	require.Equal(t, msg, logged[0], "the bounded value persisted by Nack is also the value exposed to log handlers")
	require.NotContains(t, logged[0], "\x00")
	require.LessOrEqual(t, len(logged[0]), maxErrorBytes)
}

func TestAPanickingHandlerIsNackedAndTheWorkerCarriesOn(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	var calls int
	var mu sync.Mutex
	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			panic("payload made no sense")
		}
		return nil
	})

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Nack", 1)

	nacks := f.nackRequests()
	require.Contains(t, nacks[0].GetError(), "handler panicked: payload made no sense")
	require.Contains(t, nacks[0].GetError(), "worker.invoke", "the stack goes in the error too")
	require.LessOrEqual(t, len(nacks[0].GetError()), maxErrorBytes)

	// The token came back and the worker is still working.
	require.NoError(t, s.awaitCredit(ctx, 1))
	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))
}

func TestTransientConfirmationRetriesKeepTheSameIdentifiers(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	var attempts int
	f.set(func() {
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			f.mu.Lock()
			attempts++
			n := attempts
			f.mu.Unlock()
			if n < 3 {
				// The write may well have committed before the response was
				// lost; that is exactly why the retry must present the same
				// token rather than giving up or re-running the handler.
				return status.Error(codes.Unavailable, "broker is shutting down")
			}
			return nil
		}
	})

	var runs int
	var mu sync.Mutex
	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return nil
	})

	fx := newFixture("q")
	require.NoError(t, s.deliver(ctx, fx.proto()))
	awaitCalls(t, ctx, f, "Ack", 3)
	require.NoError(t, r.stopAndWait(t))

	acks := f.ackRequests()
	require.Len(t, acks, 3)
	for i, a := range acks {
		require.Equal(t, fx.ID.String(), a.GetId(), "attempt %d", i)
		require.Equal(t, fx.LeaseID.String(), a.GetLeaseId(), "attempt %d", i)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, runs, "a lost response is not a reason to run the handler again")
}

func TestTerminalConfirmationCodesAreNotRetried(t *testing.T) {
	for _, code := range []codes.Code{
		codes.Aborted, codes.FailedPrecondition, codes.NotFound, codes.Internal,
	} {
		t.Run(code.String(), func(t *testing.T) {
			ctx := waitCtx(t)
			f := startFake(t)
			f.set(func() {
				f.onAck = func(context.Context, *hopperv1.AckRequest) error {
					return status.Error(code, code.String())
				}
			})
			r, s := runHandler(t, ctx, f, Options{}, "q",
				func(context.Context, Job) error { return nil })

			require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
			awaitCalls(t, ctx, f, "Ack", 1)
			require.NoError(t, s.awaitCredit(ctx, 1), "the delivery ends either way")
			require.NoError(t, r.stopAndWait(t))

			started, _ := f.counts("Ack")
			require.Equal(t, 1, started, "%s is an answer, not a network hiccup", code)
		})
	}
}

func TestNoConfirmationAfterTheLeaseIsLost(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	// Acquisition succeeds; every later beat says the token is stale.
	var beats int
	f.set(func() {
		f.onHeartbeat = func(context.Context, *hopperv1.HeartbeatRequest) error {
			f.mu.Lock()
			beats++
			n := beats
			f.mu.Unlock()
			if n == 1 {
				return nil
			}
			return status.Error(codes.Aborted, "lease lost")
		}
	})

	cause := make(chan error, 1)
	r, s := runHandler(t, ctx, f, Options{LeaseDuration: 150 * time.Millisecond}, "q",
		func(hctx context.Context, _ Job) error {
			<-hctx.Done()
			cause <- context.Cause(hctx)
			return hctx.Err()
		})

	fx := newFixture("q")
	fx.Grant = 150 * time.Millisecond
	require.NoError(t, s.deliver(ctx, fx.proto()))

	select {
	case err := <-cause:
		require.ErrorIs(t, err, ErrLeaseLost, "the handler is told why it was stopped")
	case <-ctx.Done():
		t.Fatal("the handler was never cancelled")
	}

	require.NoError(t, s.awaitCredit(ctx, 1), "the delivery finished and returned its token")
	require.NoError(t, r.stopAndWait(t))
	require.Empty(t, f.ackRequests(), "a job we no longer hold is not ours to confirm")
	require.Empty(t, f.nackRequests())
}

func TestAHeartbeatCancelledByNormalCompletionStillAcks(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	// The regression this guards: when the handler finishes, the SDK cancels
	// the heartbeat RPC in flight. That produces a CANCELLED status which looks
	// exactly like a rejection — and treating it as one would suppress the Ack
	// for a job that really did succeed.
	blocked := make(chan struct{}, 1)
	g := newGate(t)
	var beats int
	f.set(func() {
		f.onHeartbeat = func(hctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			f.mu.Lock()
			beats++
			n := beats
			f.mu.Unlock()
			if n == 1 {
				return nil // acquisition
			}
			select {
			case blocked <- struct{}{}:
			default:
			}
			return g.wait(hctx) // a periodic beat that never answers
		}
	})

	r, s := runHandler(t, ctx, f, Options{}, "q", func(context.Context, Job) error {
		// Return only once a renewal is definitely stuck mid-flight.
		select {
		case <-blocked:
		case <-time.After(barrier):
		}
		return nil
	})

	fx := newFixture("q")
	fx.Grant = 600 * time.Millisecond
	require.NoError(t, s.deliver(ctx, fx.proto()))

	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))
	require.Len(t, f.ackRequests(), 1, "the lease was still live under its last confirmed deadline")
	require.Empty(t, f.nackRequests())
}

func TestExecutionPinsHeartbeatsToTheDeliveredGrant(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	r, s := runHandler(t, ctx, f, Options{LeaseDuration: 12 * time.Second}, "q",
		func(context.Context, Job) error { return nil })

	fx := newFixture("q")
	fx.Grant = 30 * time.Second
	require.NoError(t, s.deliver(ctx, fx.proto()))
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))

	requests := f.heartbeatRequests()
	require.NotEmpty(t, requests)
	require.Equal(t, 30*time.Second, requests[0].GetLeaseDuration().AsDuration(),
		"a broker configuration change must not replace the grant this delivery actually received")
}

func TestAnUnregisteredQueueIsNackedUnstarted(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	ran := make(chan struct{}, 1)
	r, s := runHandler(t, ctx, f, Options{}, "known",
		func(context.Context, Job) error { ran <- struct{}{}; return nil })

	fx := newFixture("unknown")
	require.NoError(t, s.deliver(ctx, fx.proto()))
	awaitCalls(t, ctx, f, "Nack", 1)
	require.NoError(t, r.stopAndWait(t))

	nacks := f.nackRequests()
	require.Len(t, nacks, 1)
	require.Equal(t, fx.LeaseID.String(), nacks[0].GetLeaseId())
	require.Contains(t, nacks[0].GetError(), `no handler is registered for queue "unknown"`)
	require.Empty(t, ran, "user code never ran")

	beats, _ := f.counts("Heartbeat")
	require.Equal(t, 1, beats, "ownership is proven even for a job we will only hand back")
}

func TestADeliveryThatLosesToShutdownIsNackedUnstarted(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	// Hold acquisition until drain has begun, so the delivery is valid and
	// owned but arrives on the wrong side of the shutdown transition.
	g := newGate(t)
	acquiring := make(chan struct{}, 1)
	f.set(func() {
		f.onHeartbeat = func(hctx context.Context, _ *hopperv1.HeartbeatRequest) error {
			select {
			case acquiring <- struct{}{}:
			default:
			}
			return g.wait(hctx)
		}
	})

	ran := make(chan struct{}, 1)
	r, s := runHandler(t, ctx, f, Options{ShutdownTimeout: 5 * time.Second}, "q",
		func(context.Context, Job) error { ran <- struct{}{}; return nil })

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-acquiring:
	case <-ctx.Done():
		t.Fatal("acquisition never started")
	}

	r.stop()
	require.NoError(t, s.awaitEOF(ctx), "drain has begun: intake is closed")
	g.release()

	awaitCalls(t, ctx, f, "Nack", 1)
	require.NoError(t, r.wait(t), "the drain completed inside its budget")

	nacks := f.nackRequests()
	require.Len(t, nacks, 1)
	require.Contains(t, nacks[0].GetError(), "never started")
	require.Empty(t, ran, "once drain wins the race, user code does not start")
	require.Empty(t, f.ackRequests())
}

func TestForcedShutdownEndsABlockedConfirmation(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	g := newGate(t)
	acking := make(chan struct{}, 1)
	f.set(func() {
		f.onAck = func(actx context.Context, _ *hopperv1.AckRequest) error {
			select {
			case acking <- struct{}{}:
			default:
			}
			return g.wait(actx)
		}
	})

	const budget = 400 * time.Millisecond
	r, s := runHandler(t, ctx, f, Options{ShutdownTimeout: budget}, "q",
		func(context.Context, Job) error { return nil })

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-acking:
	case <-ctx.Done():
		t.Fatal("the Ack never reached the broker")
	}

	start := time.Now()
	r.stop()
	err := r.wait(t)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 awaiting confirmation",
		"a pending confirmation is counted apart from a running handler")
	require.Less(t, elapsed, 2*budget)

	// And nothing keeps retrying behind the worker's back.
	before, _ := f.counts("Ack")
	time.Sleep(150 * time.Millisecond)
	after, _ := f.counts("Ack")
	require.Equal(t, before, after, "the SDK stops when it says it has stopped")
}

func TestForcedShutdownEndsConfirmationBackoff(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	f.set(func() {
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			return status.Error(codes.Unavailable, "broker is shutting down")
		}
	})

	const budget = 400 * time.Millisecond
	r, s := runHandler(t, ctx, f, Options{ShutdownTimeout: budget}, "q",
		func(context.Context, Job) error { return nil })

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Ack", 2)

	start := time.Now()
	r.stop()
	require.ErrorIs(t, r.wait(t), ErrAbandoned)
	require.Less(t, time.Since(start), 2*budget, "the retry timer is interruptible")

	before, _ := f.counts("Ack")
	time.Sleep(150 * time.Millisecond)
	after, _ := f.counts("Ack")
	require.Equal(t, before, after, "no retries continue after abandonment")
}

func TestAHandlerReturningAfterAbandonmentConfirmsNothing(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	g := newGate(t)
	const budget = 300 * time.Millisecond
	started := make(chan struct{}, 1)
	r, s := runHandler(t, ctx, f, Options{ShutdownTimeout: budget}, "q",
		func(context.Context, Job) error {
			started <- struct{}{}
			// Deliberately ignores cancellation, as a handler doing blocking
			// work with no context support would.
			<-g.ch
			return nil
		})

	require.NoError(t, s.deliver(ctx, newFixture("q").proto()))
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("the handler never started")
	}

	r.stop()
	err := r.wait(t)
	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorContains(t, err, "1 running handlers")

	// Only now does the handler finish. Its job was already counted as
	// abandoned and its lease is expiring; writing now would be writing after
	// the worker reported it had stopped.
	g.release()
	time.Sleep(200 * time.Millisecond)
	require.Empty(t, f.ackRequests())
	require.Empty(t, f.nackRequests())
}

func TestConfirmationStopsAtTheLocalLeaseDeadline(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)

	f.set(func() {
		f.onAck = func(context.Context, *hopperv1.AckRequest) error {
			return status.Error(codes.Unavailable, "broker is shutting down")
		}
	})

	// A short grant means the local deadline arrives long before the shutdown
	// budget would. Retrying past it would only ask the broker about a token
	// the reaper is already free to take.
	r, s := runHandler(t, ctx, f, Options{ShutdownTimeout: 10 * time.Second}, "q",
		func(context.Context, Job) error { return nil })

	fx := newFixture("q")
	fx.Grant = 400 * time.Millisecond
	require.NoError(t, s.deliver(ctx, fx.proto()))

	require.NoError(t, s.awaitCredit(ctx, 1), "the delivery ends when its lease does")
	before, _ := f.counts("Ack")
	time.Sleep(200 * time.Millisecond)
	after, _ := f.counts("Ack")
	require.Equal(t, before, after, "retries stop at the lease, not at the shutdown budget")

	require.NoError(t, r.stopAndWait(t), "and the worker still drains cleanly")
}
