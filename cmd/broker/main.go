// Command broker serves the hopper.v1 gRPC API over a PostgreSQL store, runs
// the reaper, promoter and janitor sweeps and the cron scheduler, and shuts
// down on SIGTERM/SIGINT.
//
// It is the composition root: the one place that owns process concerns —
// flags, signals, the connection pool and migration. The library packages
// underneath know nothing about any of them.
//
// This is a trusted-network prototype: plaintext, no authentication, loopback
// by default. Anyone who can connect can read and mutate every job, including
// lease tokens. Binding a non-loopback address opts into that trust model.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	// The runtime image is distroless/static, which ships no zoneinfo, so a
	// schedule in any timezone but UTC would fail to load there. Embedding the
	// database costs about 450 KB in the binary.
	_ "time/tzdata"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/metrics"
	"github.com/rpasco787/hopper/internal/scheduler"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/sweep"
)

type config struct {
	dsn, listen, metricsListen, debugListen, logLevel                       string
	metricsQueues                                                           []string
	leaseDuration, reaperInterval, pollInterval, retention, shutdownTimeout time.Duration
	maxLeaseBatch, dbMaxConns                                               int

	scheduler         bool
	schedulerInterval time.Duration
	cronFile          string
	// schedules is the parsed, validated content of cronFile; nil when no
	// file was given, which leaves cron_schedules unmanaged.
	schedules []scheduler.Definition
}

// envVars maps each flag to the environment variable that can set it. A flag
// given on the command line wins over its variable.
var envVars = [][2]string{
	{"dsn", "HOPPER_DSN"}, {"listen", "HOPPER_LISTEN"}, {"log-level", "HOPPER_LOG_LEVEL"},
	{"metrics-listen", "HOPPER_METRICS_LISTEN"}, {"metrics-queues", "HOPPER_METRICS_QUEUES"},
	{"lease-duration", "HOPPER_LEASE_DURATION"}, {"reaper-interval", "HOPPER_REAPER_INTERVAL"},
	{"poll-interval", "HOPPER_POLL_INTERVAL"}, {"retention", "HOPPER_RETENTION"},
	{"shutdown-timeout", "HOPPER_SHUTDOWN_TIMEOUT"}, {"max-lease-batch", "HOPPER_MAX_LEASE_BATCH"},
	{"scheduler", "HOPPER_SCHEDULER"}, {"scheduler-interval", "HOPPER_SCHEDULER_INTERVAL"},
	{"cron-file", "HOPPER_CRON_FILE"}, {"debug-listen", "HOPPER_DEBUG_LISTEN"},
	{"db-max-conns", "HOPPER_DB_MAX_CONNS"},
}

const MaxMetricsQueues = 100

// DefaultDBMaxConns sizes each broker's PostgreSQL pool. pgxpool's own default
// is the machine's CPU count (8 on the benchmark host), which starved a broker
// serving 64 worker slots: every lease, heartbeat and ack queued for a
// connection. 32 was the knee of the measured curve (docs/BENCHMARKS.md).
// Brokers times this must fit the server's max_connections, which is 100 by
// default; two brokers use at most 64.
const DefaultDBMaxConns = 32

// minDBMaxConns is the smallest pool that still leaves a connection for
// queries once the connections held for a whole term are taken: the scheduler
// role's, in whichever broker leads.
func minDBMaxConns(scheduler bool) int {
	n := 1
	if scheduler {
		n++
	}
	return n
}

// parseConfig resolves flags and environment into a validated config. Every
// rejection happens here, before the process has opened a pool or a listener,
// so a bad value can never leave a half-started broker behind.
func parseConfig(args []string) (config, error) {
	var c config
	var metricsQueues string
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.StringVar(&c.dsn, "dsn", "", "PostgreSQL connection string (HOPPER_DSN)")
	fs.StringVar(&c.listen, "listen", "127.0.0.1:9090", "trusted-network gRPC address (HOPPER_LISTEN)")
	fs.StringVar(&c.metricsListen, "metrics-listen", "127.0.0.1:9091", "Prometheus HTTP address; empty disables metrics (HOPPER_METRICS_LISTEN)")
	fs.StringVar(&c.debugListen, "debug-listen", "", "pprof HTTP address; empty disables profiling (HOPPER_DEBUG_LISTEN)")
	fs.StringVar(&metricsQueues, "metrics-queues", "", "comma-separated queue metric allowlist (HOPPER_METRICS_QUEUES)")
	fs.StringVar(&c.logLevel, "log-level", "info", "debug/info/warn/error (HOPPER_LOG_LEVEL)")
	fs.DurationVar(&c.leaseDuration, "lease-duration", broker.DefaultLeaseDuration, "default visibility timeout (HOPPER_LEASE_DURATION)")
	fs.DurationVar(&c.reaperInterval, "reaper-interval", time.Second, "reaper tick (HOPPER_REAPER_INTERVAL)")
	fs.DurationVar(&c.pollInterval, "poll-interval", broker.DefaultPollInterval, "empty-queue poll interval (HOPPER_POLL_INTERVAL)")
	fs.DurationVar(&c.retention, "retention", 24*time.Hour, "positive finished-job retention window (HOPPER_RETENTION)")
	fs.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "RPC drain deadline (HOPPER_SHUTDOWN_TIMEOUT)")
	fs.IntVar(&c.maxLeaseBatch, "max-lease-batch", broker.DefaultLeaseBatch, "rows per lease query (HOPPER_MAX_LEASE_BATCH)")
	fs.IntVar(&c.dbMaxConns, "db-max-conns", DefaultDBMaxConns, "PostgreSQL connections per broker (HOPPER_DB_MAX_CONNS)")
	fs.BoolVar(&c.scheduler, "scheduler", true, "run the cron scheduler in this process (HOPPER_SCHEDULER)")
	fs.DurationVar(&c.schedulerInterval, "scheduler-interval", 5*time.Second, "leadership retry and tick period (HOPPER_SCHEDULER_INTERVAL)")
	fs.StringVar(&c.cronFile, "cron-file", "", "JSON schedule declarations; empty leaves cron_schedules unmanaged (HOPPER_CRON_FILE)")
	// Parse flags first so a supplied flag wins even over an invalid env value.
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	supplied := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { supplied[f.Name] = true })
	for _, pair := range envVars {
		if v, ok := os.LookupEnv(pair[1]); ok && !supplied[pair[0]] {
			if err := fs.Set(pair[0], v); err != nil {
				return c, fmt.Errorf("invalid %s", pair[1]) // no DSN/value leakage
			}
		}
	}
	if c.dsn == "" {
		return c, errors.New("-dsn or HOPPER_DSN is required")
	}
	// One knob for the pool size, so there is no precedence to remember.
	if strings.Contains(c.dsn, "pool_max_conns") {
		return c, errors.New("set -db-max-conns rather than pool_max_conns in the DSN")
	}
	if c.listen == "" {
		return c, errors.New("listen address is required")
	}
	if c.metricsListen != "" {
		if _, _, err := net.SplitHostPort(c.metricsListen); err != nil {
			return c, errors.New("invalid metrics-listen address")
		}
	}
	if c.debugListen != "" {
		if _, _, err := net.SplitHostPort(c.debugListen); err != nil {
			return c, errors.New("invalid debug-listen address")
		}
	}
	if metricsQueues != "" {
		seen := make(map[string]struct{})
		for _, raw := range strings.Split(metricsQueues, ",") {
			queue := strings.TrimSpace(raw)
			if queue == "" {
				return c, errors.New("metrics-queues contains an empty entry")
			}
			if queue == "_other" {
				return c, errors.New("metrics-queues contains the reserved queue")
			}
			if _, duplicate := seen[queue]; duplicate {
				return c, errors.New("metrics-queues contains a duplicate")
			}
			seen[queue] = struct{}{}
			c.metricsQueues = append(c.metricsQueues, queue)
			if len(c.metricsQueues) > MaxMetricsQueues {
				return c, fmt.Errorf("metrics-queues exceeds the maximum of %d", MaxMetricsQueues)
			}
		}
	}
	if _, err := parseLevel(c.logLevel); err != nil {
		return c, err
	}
	for _, d := range []struct {
		name  string
		value time.Duration
	}{
		{"lease-duration", c.leaseDuration}, {"reaper-interval", c.reaperInterval},
		{"poll-interval", c.pollInterval}, {"retention", c.retention}, {"shutdown-timeout", c.shutdownTimeout},
		{"scheduler-interval", c.schedulerInterval},
	} {
		if d.value <= 0 {
			return c, fmt.Errorf("%s must be positive", d.name)
		}
	}
	if c.maxLeaseBatch <= 0 {
		return c, errors.New("max-lease-batch must be positive")
	}
	if c.dbMaxConns <= 0 {
		return c, errors.New("db-max-conns must be positive")
	}
	if c.dbMaxConns > math.MaxInt32 {
		return c, fmt.Errorf("db-max-conns exceeds %d", math.MaxInt32)
	}
	if floor := minDBMaxConns(c.scheduler); c.dbMaxConns < floor {
		return c, fmt.Errorf("db-max-conns must be at least %d with -scheduler=%t", floor, c.scheduler)
	}
	if c.cronFile != "" {
		defs, err := scheduler.LoadFile(c.cronFile)
		if err != nil {
			return c, err
		}
		c.schedules = defs
	}
	return c, nil
}

// awaitServing waits until gRPC serving ends, metrics serving fails, or the
// process context is cancelled. A clean metrics shutdown is ignored because
// it may race the ordinary shutdown path.
func awaitServing(ctx context.Context, serveErr, metricsErr <-chan error) (err error, served bool) {
	for {
		select {
		case err = <-serveErr:
			return err, true
		case err = <-metricsErr:
			if errors.Is(err, http.ErrServerClosed) {
				metricsErr = nil
				continue
			}
			return err, false
		case <-ctx.Done():
			return nil, false
		}
	}
}

// finalServingError preserves the event that ended serving while also waiting
// for and reporting the gRPC server's result when another event began shutdown.
func finalServingError(servingErr error, served bool, serveErr <-chan error, shutdownErr, metricsShutdownErr error) error {
	if !served {
		servingErr = errors.Join(servingErr, <-serveErr)
	}
	return errors.Join(servingErr, shutdownErr, metricsShutdownErr)
}

func parseLevel(level string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return 0, fmt.Errorf("invalid log level %q (want debug, info, warn or error)", level)
	}
	return l, nil
}

func newLogger(level string) (*slog.Logger, error) {
	l, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})), nil
}

// isLoopback reports whether addr binds only the local machine. An empty host
// or an unspecified address (0.0.0.0, ::) listens on every interface.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "broker: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	logger, err := newLogger(cfg.logLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	poolCfg.MaxConns = int32(cfg.dbMaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	st := postgres.New(pool)
	var (
		reg      *prometheus.Registry
		bm       *metrics.Broker
		grpcOpts []grpc.ServerOption
		grpcMet  *grpcprom.ServerMetrics
	)
	if cfg.metricsListen != "" {
		reg = metrics.NewRegistry()
		bm, err = metrics.NewBroker(reg, cfg.metricsQueues)
		if err != nil {
			return err
		}
		st = st.WithObserver(bm)
		if err := metrics.RegisterPool(reg, func() metrics.PoolStat { return pool.Stat() }); err != nil {
			return err
		}
		grpcMet, err = metrics.NewGRPCServerMetrics(reg)
		if err != nil {
			return err
		}
		grpcOpts = append(grpcOpts,
			grpc.ChainUnaryInterceptor(grpcMet.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(grpcMet.StreamServerInterceptor()))
		logger.Info("metrics queues", "count", len(cfg.metricsQueues))
	}
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer lis.Close()
	var metricsLis net.Listener
	if cfg.metricsListen != "" {
		metricsLis, err = net.Listen("tcp", cfg.metricsListen)
		if err != nil {
			_ = lis.Close()
			return fmt.Errorf("metrics listen: %w", err)
		}
		defer metricsLis.Close()
	}
	var debugLis net.Listener
	if cfg.debugListen != "" {
		debugLis, err = net.Listen("tcp", cfg.debugListen)
		if err != nil {
			return fmt.Errorf("debug listen: %w", err)
		}
		defer debugLis.Close()
	}
	if !isLoopback(cfg.listen) {
		logger.Warn("listening beyond loopback: the API is plaintext and unauthenticated, "+
			"so anyone who can connect can read and mutate every job, including lease tokens",
			"addr", cfg.listen)
	}
	srv := broker.NewServer(st, broker.Config{
		LeaseDuration: cfg.leaseDuration, MaxLeaseBatch: cfg.maxLeaseBatch,
		PollInterval: cfg.pollInterval, Logger: logger,
	}, grpcOpts...)
	if grpcMet != nil {
		grpcMet.InitializeMetrics(srv)
	}

	sweepCtx, stopSweeps := context.WithCancel(context.Background())
	var sweeps sync.WaitGroup
	// This defer runs before pool.Close, including an unexpected Serve failure.
	defer func() { stopSweeps(); sweeps.Wait() }()
	for _, loop := range sweep.All(st, cfg.retention) {
		if loop.Name == "reaper" {
			loop.Interval = cfg.reaperInterval
		}
		loop.Logger = logger
		sweeps.Add(1)
		go func(l *sweep.Loop) {
			defer sweeps.Done()
			if err := l.Run(sweepCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("sweep loop stopped", "loop", l.Name, "err", err)
			}
		}(loop)
	}
	if cfg.scheduler {
		sched := &scheduler.Scheduler{
			Store: st, Interval: cfg.schedulerInterval, Logger: logger, Definitions: cfg.schedules,
		}
		sweeps.Add(1)
		go func() {
			defer sweeps.Done()
			if err := sched.Run(sweepCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("scheduler stopped", "err", err)
			}
		}()
	}
	if bm != nil {
		sampler := &metrics.DepthSampler{Source: st, Metrics: bm, Logger: logger}
		sweeps.Add(1)
		go func() {
			defer sweeps.Done()
			if err := sampler.Run(sweepCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("queue depth sampler stopped", "err", err)
			}
		}()
	}
	logger.Info("scheduler configured", "enabled", cfg.scheduler,
		"interval", cfg.schedulerInterval.String(), "definitions", len(cfg.schedules))
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	var (
		httpSrv    *http.Server
		metricsErr chan error
	)
	if metricsLis != nil {
		httpSrv = metrics.NewHTTPServer(reg)
		metricsErr = make(chan error, 1)
		go func() {
			if err := httpSrv.Serve(metricsLis); err != nil {
				metricsErr <- fmt.Errorf("metrics: serve: %w", err)
				return
			}
			metricsErr <- nil
		}()
		logger.Info("metrics listening", "addr", metricsLis.Addr().String())
	}
	var debugSrv *http.Server
	if debugLis != nil {
		// Profiling is a diagnostic, not a dependency: if it fails, say so and
		// keep serving the queue.
		debugSrv = newDebugServer()
		go func() {
			if err := debugSrv.Serve(debugLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("debug server stopped", "err", err)
			}
		}()
		logger.Warn("debug endpoint listening: it serves CPU, heap and goroutine profiles to anyone who can connect",
			"addr", debugLis.Addr().String())
	}
	logger.Info("broker listening", "addr", lis.Addr().String(),
		"lease_duration", cfg.leaseDuration.String(), "reaper_interval", cfg.reaperInterval.String(),
		"retention", cfg.retention.String(), "db_max_conns", cfg.dbMaxConns)

	servingErr, served := awaitServing(ctx, serveErr, metricsErr)
	stop() // restore default handling before draining; a second signal can exit
	logger.Info("shutting down", "timeout", cfg.shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	stopSweeps()
	sweeps.Wait()
	if debugSrv != nil {
		_ = debugSrv.Close() // ends any profile in flight; nothing waits on it
	}
	var metricsShutdownErr error
	if httpSrv != nil {
		metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 2*time.Second)
		metricsShutdownErr = httpSrv.Shutdown(metricsCtx)
		cancelMetrics()
		if metricsShutdownErr != nil {
			metricsShutdownErr = errors.Join(metricsShutdownErr, httpSrv.Close())
		}
	}
	if err := finalServingError(servingErr, served, serveErr, shutdownErr, metricsShutdownErr); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}
