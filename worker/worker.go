// Package worker is hopper's worker SDK.
//
// A worker holds one LeaseJobs stream, runs a bounded number of handlers at a
// time, keeps each delivery's lease alive while its handler runs, and confirms
// the outcome. It speaks only gRPC: it has no database connection and imports
// nothing from internal/, which is what makes it usable from someone else's
// program.
//
// Delivery is at least once (docs/semantics.md §1). The SDK narrows the window
// where a duplicate can happen — it validates a lease before invoking a
// handler, cancels handler contexts the moment a lease is lost, and retries
// confirmation while the lease is still live — but it cannot close it. Handlers
// must be idempotent, keyed on Job.ID or a business key in the payload.
//
// Typical use:
//
//	w := worker.New("127.0.0.1:9090", worker.Options{Concurrency: 16})
//	w.Handle("email", sendEmail)
//	if err := w.Run(ctx); err != nil {
//		log.Fatal(err)
//	}
//
// Run blocks until ctx is cancelled and the drain finishes, or until a
// permanent failure.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

const (
	defaultConcurrency     = 8
	defaultShutdownTimeout = 30 * time.Second
	// maxConcurrency is a sanity bound, not a tuned limit. Credit is uint32 on
	// the wire, so anything below that fits; 65536 goroutines of handler is
	// already far past the point where the database is the constraint.
	maxConcurrency = 1 << 16
)

// HandlerFunc runs one job. Returning nil acks it; returning an error nacks it,
// and the error text is stored in last_error after sanitizing.
//
// The context is cancelled when the lease is lost, when the job is cancelled,
// or at the forced-shutdown deadline. context.Cause distinguishes them:
// ErrLeaseLost, ErrLeaseExpired, ErrJobCancelled, ErrShuttingDown. A handler
// that ignores cancellation cannot be stopped — Go has no way to kill a
// goroutine — so it will keep running while the job is redelivered elsewhere.
type HandlerFunc func(context.Context, Job) error

// Options configures a Worker. The zero value of each field selects a default;
// nothing here is required.
type Options struct {
	// ID is recorded on leased rows as leased_by, for observability only.
	// Empty takes hostname-pid.
	ID string
	// Concurrency is the most handlers that may run at once, and also the
	// credit the worker advertises. Zero takes 8.
	Concurrency int
	// LeaseDuration is the visibility timeout to request. Zero asks the broker
	// to choose, which is usually what you want: the broker's default is the
	// one an operator tuned.
	LeaseDuration time.Duration
	// ShutdownTimeout bounds the whole drain — stream teardown, handlers and
	// confirmations share it. Zero takes 30s. It must fit inside the
	// deployment's own grace period, or the process is killed mid-drain.
	ShutdownTimeout time.Duration
	// Logger receives lifecycle and per-delivery events. Nil takes slog.Default.
	Logger *slog.Logger
	// DialOptions are appended after the SDK's defaults, so passing
	// grpc.WithTransportCredentials replaces the plaintext default.
	DialOptions []grpc.DialOption
}

// Worker runs handlers against one broker. Create it with New, register
// handlers, then call Run once.
type Worker struct {
	target string
	opts   Options
	log    *slog.Logger

	// handlers is written only by Handle, which is documented as setup-only.
	// started guards it against the one misuse that is easy to make by
	// accident — registering from inside a handler — by panicking rather than
	// corrupting the map. It does not make concurrent Handle calls safe, and
	// nothing here pretends it does.
	handlers map[string]HandlerFunc
	started  atomic.Bool

	// runner owns a delivery once it is off the stream. It is a field so tests
	// can drive dispatch and capacity without staging a whole execution.
	runner deliveryRunner
}

// New creates a Worker. It does no network work; the connection is made by Run.
func New(target string, opts Options) *Worker {
	if opts.Concurrency == 0 {
		opts.Concurrency = defaultConcurrency
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = defaultShutdownTimeout
	}
	if opts.ID == "" {
		opts.ID = defaultID()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	// Defaults replace zero values only. A negative duration or concurrency
	// survives to Run, which rejects it: silently correcting a negative would
	// hide the bug that produced it.
	w := &Worker{
		target:   target,
		opts:     opts,
		log:      opts.Logger.With("worker_id", opts.ID),
		handlers: make(map[string]HandlerFunc),
	}
	w.runner = w.execute
	return w
}

// Handle registers a handler for a queue and returns the Worker so calls can
// be chained.
//
// Registration is setup-only: call it from the goroutine that created the
// Worker, before Run. Concurrent registration is not supported, and
// registering after Run has started panics — by then the subscription naming
// the queues has already been sent, so a late handler would never be reached
// and the silence would be mystifying.
//
// It panics on an empty queue, a nil handler, or a duplicate registration.
// All three are programmer errors visible at startup, and Handle returns a
// *Worker precisely so that the common case reads as a list.
func (w *Worker) Handle(queue string, h HandlerFunc) *Worker {
	if w.started.Load() {
		panic("worker: Handle called after Run; registration is setup-only")
	}
	if queue == "" {
		panic("worker: Handle called with an empty queue")
	}
	if h == nil {
		panic(fmt.Sprintf("worker: Handle called with a nil handler for queue %q", queue))
	}
	if _, dup := w.handlers[queue]; dup {
		panic(fmt.Sprintf("worker: queue %q already has a handler", queue))
	}
	w.handlers[queue] = h
	return w
}

// queues returns the registered queues in a stable order, so a subscription
// and the diagnostics that quote it read the same way on every run.
func (w *Worker) queues() []string {
	return slices.Sorted(maps.Keys(w.handlers))
}

// Run consumes jobs until ctx is cancelled or a permanent failure stops it.
//
// Cancelling ctx starts a graceful drain: the worker stops accepting work,
// half-closes its stream, and gives running handlers and their confirmations
// until ShutdownTimeout to finish. Run returns nil if the drain completed, or
// an error wrapping ErrAbandoned if the deadline passed first.
//
// Run is single-use. A second call returns an error rather than starting a
// second stream under the same identity.
func (w *Worker) Run(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return errors.New("worker: Run has already been called")
	}
	if err := w.validate(); err != nil {
		return err
	}
	return w.consume(ctx)
}

// validate rejects configuration before any network work, so a bad value can
// never leave a half-connected worker behind.
func (w *Worker) validate() error {
	if w.target == "" {
		return errors.New("worker: a broker target is required")
	}
	if len(w.handlers) == 0 {
		return ErrNoHandlers
	}
	if w.opts.Concurrency < 0 {
		return fmt.Errorf("worker: concurrency %d must not be negative", w.opts.Concurrency)
	}
	if w.opts.Concurrency > maxConcurrency {
		return fmt.Errorf("worker: concurrency %d exceeds the maximum of %d", w.opts.Concurrency, maxConcurrency)
	}
	if w.opts.LeaseDuration < 0 {
		return fmt.Errorf("worker: lease duration %s must not be negative", w.opts.LeaseDuration)
	}
	if w.opts.ShutdownTimeout < 0 {
		return fmt.Errorf("worker: shutdown timeout %s must not be negative", w.opts.ShutdownTimeout)
	}
	return nil
}

// consume dials, holds a lease stream, and drains on the way out.
//
// The order matters. The lifecycle and its cancellation watcher are installed
// before anything can block on the network, so a caller who cancels during the
// dial gets the same bounded shutdown as one who cancels an hour later. The
// drain is begun explicitly after the stream ends, because a permanent stream
// failure has to start the same clock a cancelled context does.
func (w *Worker) consume(ctx context.Context) error {
	lc := newLifecycle(ctx, w.opts.ShutdownTimeout, w.log)
	defer lc.stop()

	conn, err := w.dial()
	if err != nil {
		lc.beginDrain("dial failed")
		return errors.Join(err, lc.await())
	}
	defer conn.Close()

	var sessionErr error
	// A worker already draining does not open a stream. Leasing jobs it has
	// just promised not to start would consume their attempts for nothing.
	if lc.isDraining() {
		w.log.Info("not opening a lease stream; the worker is already draining")
	} else {
		s := &session{
			w:    w,
			lc:   lc,
			rpc:  hopperv1.NewHopperServiceClient(conn),
			pool: newCapacity(w.opts.Concurrency, w.log),
			log:  w.log,
		}
		sessionErr = s.run()
	}

	lc.beginDrain("lease stream ended")
	// Both errors are kept. A permanent failure explains why the worker
	// stopped; an abandonment says what it left behind, and swallowing either
	// one because the other is present loses half the story.
	return errors.Join(sessionErr, lc.await())
}

// dial follows client.Dial's precedence: SDK defaults first, caller options
// after, so grpc.WithTransportCredentials replaces the plaintext default
// rather than conflicting with it.
//
// Keepalive matches what the server enforces by default (a ping no oftener
// than every five minutes). It detects a broker that vanished without a FIN;
// it is not the shutdown mechanism, which is the lifecycle deadline.
func (w *Worker) dial() (*grpc.ClientConn, error) {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    5 * time.Minute,
			Timeout: 20 * time.Second,
		}),
	}
	conn, err := grpc.NewClient(w.target, append(base, w.opts.DialOptions...)...)
	if err != nil {
		return nil, fmt.Errorf("worker: dial %s: %w", w.target, err)
	}
	return conn, nil
}

// defaultID names this worker for the leased_by column. It is observability
// only, so a hostname lookup that fails is not worth failing startup over.
func defaultID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
