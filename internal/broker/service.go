package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// Defaults for the knobs a request may leave unset. cmd/broker overrides them
// from flags; they live here so that a Service built in a test is usable
// without configuring anything.
const (
	MaxMessageBytes      = 4 << 20 // explicit transport ceiling; not an atomicity guarantee
	DefaultLeaseDuration = 30 * time.Second
	DefaultLeaseBatch    = 32
	DefaultPollInterval  = 250 * time.Millisecond
	DefaultListLimit     = 100
	MaxListLimit         = 1000
)

// Config is the request-independent behaviour of a Service.
type Config struct {
	// LeaseDuration is used when a stream or heartbeat does not ask for one.
	LeaseDuration time.Duration
	// MaxLeaseBatch caps rows per database call. It does not cap all buffered
	// or unacknowledged deliveries across a stream (docs/semantics.md §11).
	MaxLeaseBatch int
	// PollInterval is how long a lease stream waits after finding the queue
	// empty, unless a wakeup ends the wait first. It is the fallback for a
	// lost wakeup and the only path for jobs that become available without an
	// Enqueue: promoted delays and retries, and redrives (docs/semantics.md §9).
	PollInterval time.Duration
	// Wakeups carries enqueue announcements between brokers. Nil keeps them
	// within this broker; RunWakeups does the rest.
	Wakeups store.Wakeups
	Logger  *slog.Logger
}

// Service implements hopperv1.HopperServiceServer over a store.Store.
//
// It embeds the generated Unimplemented struct, which is what makes adding an
// RPC to the proto a compile-time no-op here rather than a build break: the
// new method answers Unimplemented until someone writes it. That is also why
// GetJob and ListJobs can be added in a later task without the service failing
// to build in this one.
type Service struct {
	hopperv1.UnimplementedHopperServiceServer

	store store.Store
	cfg   Config

	// shutdown is closed when the process is draining. Lease streams select on
	// it and return Unavailable. Without it grpc.Server.GracefulStop would
	// block forever: it waits for in-flight RPCs, and a lease stream has no
	// reason of its own to end.
	shutdown  chan struct{}
	drainOnce sync.Once

	// auxActive counts handlers of auxiliary streams (health Watch, reflection)
	// that drainStreams has started and that have not yet returned. Those
	// handlers are abandoned, not joined, when a drain ends their RPC, so this
	// is how a test proves they exit once gRPC cancels their context.
	auxActive atomic.Int32

	// wakes ends idle lease streams' waits when their queues get work.
	wakes *wakeups
}

// NewService returns a Service. Zero values in cfg take the package defaults.
func NewService(st store.Store, cfg Config) *Service {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = DefaultLeaseDuration
	}
	if cfg.MaxLeaseBatch <= 0 {
		cfg.MaxLeaseBatch = DefaultLeaseBatch
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Service{store: st, cfg: cfg, shutdown: make(chan struct{}), wakes: newWakeups()}
}

// Drain tells every lease stream to finish. It is safe to call more than once,
// which matters because a signal handler and a deferred cleanup may both call
// it on the way out.
func (s *Service) Drain() {
	s.drainOnce.Do(func() { close(s.shutdown) })
}

// fail logs err with its real detail and returns the client-safe status. Every
// handler funnels its store errors through this, so "the client sees Internal"
// and "the operator sees the pgx error" are the same line of code and cannot
// drift apart.
func (s *Service) fail(ctx context.Context, op string, err error, attrs ...any) error {
	out := toStatus(err)
	switch status.Code(out) {
	case codes.Internal:
		s.cfg.Logger.ErrorContext(ctx, "rpc failed",
			append([]any{"rpc", op, "err", err}, attrs...)...)
	case codes.Unavailable:
		// Warn, not debug: an operator needs to see a database outage, and
		// the driver's error says which kind.
		s.cfg.Logger.WarnContext(ctx, "rpc unavailable",
			append([]any{"rpc", op, "err", err}, attrs...)...)
	default:
		s.cfg.Logger.DebugContext(ctx, "rpc rejected",
			append([]any{"rpc", op, "code", status.Code(out).String()}, attrs...)...)
	}
	return out
}
