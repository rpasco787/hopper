// Command broker serves the hopper.v1 gRPC API over a PostgreSQL store, runs
// the reaper, promoter and janitor sweeps, and shuts down on SIGTERM/SIGINT.
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
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/sweep"
)

type config struct {
	dsn, listen, logLevel                                                   string
	leaseDuration, reaperInterval, pollInterval, retention, shutdownTimeout time.Duration
	maxLeaseBatch                                                           int
}

// envVars maps each flag to the environment variable that can set it. A flag
// given on the command line wins over its variable.
var envVars = [][2]string{
	{"dsn", "HOPPER_DSN"}, {"listen", "HOPPER_LISTEN"}, {"log-level", "HOPPER_LOG_LEVEL"},
	{"lease-duration", "HOPPER_LEASE_DURATION"}, {"reaper-interval", "HOPPER_REAPER_INTERVAL"},
	{"poll-interval", "HOPPER_POLL_INTERVAL"}, {"retention", "HOPPER_RETENTION"},
	{"shutdown-timeout", "HOPPER_SHUTDOWN_TIMEOUT"}, {"max-lease-batch", "HOPPER_MAX_LEASE_BATCH"},
}

// parseConfig resolves flags and environment into a validated config. Every
// rejection happens here, before the process has opened a pool or a listener,
// so a bad value can never leave a half-started broker behind.
func parseConfig(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("broker", flag.ContinueOnError)
	fs.StringVar(&c.dsn, "dsn", "", "PostgreSQL connection string (HOPPER_DSN)")
	fs.StringVar(&c.listen, "listen", "127.0.0.1:9090", "trusted-network gRPC address (HOPPER_LISTEN)")
	fs.StringVar(&c.logLevel, "log-level", "info", "debug/info/warn/error (HOPPER_LOG_LEVEL)")
	fs.DurationVar(&c.leaseDuration, "lease-duration", broker.DefaultLeaseDuration, "default visibility timeout (HOPPER_LEASE_DURATION)")
	fs.DurationVar(&c.reaperInterval, "reaper-interval", time.Second, "reaper tick (HOPPER_REAPER_INTERVAL)")
	fs.DurationVar(&c.pollInterval, "poll-interval", broker.DefaultPollInterval, "empty-queue poll interval (HOPPER_POLL_INTERVAL)")
	fs.DurationVar(&c.retention, "retention", 24*time.Hour, "positive finished-job retention window (HOPPER_RETENTION)")
	fs.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "RPC drain deadline (HOPPER_SHUTDOWN_TIMEOUT)")
	fs.IntVar(&c.maxLeaseBatch, "max-lease-batch", broker.DefaultLeaseBatch, "rows per lease query (HOPPER_MAX_LEASE_BATCH)")
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
	if c.listen == "" {
		return c, errors.New("listen address is required")
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
	} {
		if d.value <= 0 {
			return c, fmt.Errorf("%s must be positive", d.name)
		}
	}
	if c.maxLeaseBatch <= 0 {
		return c, errors.New("max-lease-batch must be positive")
	}
	return c, nil
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

	pool, err := pgxpool.New(ctx, cfg.dsn)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()
	if err := postgres.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	st := postgres.New(pool)
	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer lis.Close()
	if !isLoopback(cfg.listen) {
		logger.Warn("listening beyond loopback: the API is plaintext and unauthenticated, "+
			"so anyone who can connect can read and mutate every job, including lease tokens",
			"addr", cfg.listen)
	}
	srv := broker.NewServer(st, broker.Config{
		LeaseDuration: cfg.leaseDuration, MaxLeaseBatch: cfg.maxLeaseBatch,
		PollInterval: cfg.pollInterval, Logger: logger,
	})

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
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	logger.Info("broker listening", "addr", lis.Addr().String(),
		"lease_duration", cfg.leaseDuration.String(), "reaper_interval", cfg.reaperInterval.String(),
		"retention", cfg.retention.String())

	var servingErr error
	served := false
	select {
	case servingErr = <-serveErr:
		served = true
	case <-ctx.Done():
	}
	stop() // restore default handling before draining; a second signal can exit
	logger.Info("shutting down", "timeout", cfg.shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	stopSweeps()
	sweeps.Wait()
	if !served {
		servingErr = <-serveErr
	}
	if err := errors.Join(servingErr, shutdownErr); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}
