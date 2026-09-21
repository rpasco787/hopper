// Command worker runs the Hopper worker SDK with three small example handlers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	workersdk "github.com/rpasco787/hopper/worker"
)

const maxWorkerConcurrency = 1 << 16

type config struct {
	broker, id, logLevel           string
	concurrency                    int
	leaseDuration, shutdownTimeout time.Duration
}

var envVars = [][2]string{
	{"broker", "HOPPER_BROKER"},
	{"worker-id", "HOPPER_WORKER_ID"},
	{"concurrency", "HOPPER_CONCURRENCY"},
	{"lease-duration", "HOPPER_LEASE_DURATION"},
	{"shutdown-timeout", "HOPPER_SHUTDOWN_TIMEOUT"},
	{"log-level", "HOPPER_LOG_LEVEL"},
}

func parseConfig(args []string) (config, error) {
	c := config{concurrency: 8, shutdownTimeout: 30 * time.Second}
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.StringVar(&c.broker, "broker", "127.0.0.1:9090", "broker gRPC address (HOPPER_BROKER)")
	fs.StringVar(&c.id, "worker-id", "", "worker identity; empty uses hostname/PID (HOPPER_WORKER_ID)")
	fs.IntVar(&c.concurrency, "concurrency", 8, "maximum active deliveries (HOPPER_CONCURRENCY)")
	fs.DurationVar(&c.leaseDuration, "lease-duration", 0, "requested lease duration; zero uses broker default (HOPPER_LEASE_DURATION)")
	fs.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 30*time.Second, "graceful drain budget (HOPPER_SHUTDOWN_TIMEOUT)")
	fs.StringVar(&c.logLevel, "log-level", "info", "debug/info/warn/error (HOPPER_LOG_LEVEL)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	supplied := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { supplied[f.Name] = true })
	for _, pair := range envVars {
		if value, ok := os.LookupEnv(pair[1]); ok && !supplied[pair[0]] {
			if err := fs.Set(pair[0], value); err != nil {
				return c, fmt.Errorf("invalid %s", pair[1])
			}
		}
	}
	if c.broker == "" {
		return c, errors.New("broker address is required")
	}
	if c.concurrency <= 0 {
		return c, errors.New("concurrency must be positive")
	}
	if c.concurrency > maxWorkerConcurrency {
		return c, fmt.Errorf("concurrency %d exceeds the maximum of %d", c.concurrency, maxWorkerConcurrency)
	}
	if c.leaseDuration < 0 {
		return c, errors.New("lease-duration must not be negative")
	}
	if c.shutdownTimeout < 0 {
		return c, errors.New("shutdown-timeout must not be negative")
	}
	if _, err := parseLevel(c.logLevel); err != nil {
		return c, err
	}
	return c, nil
}

func parseLevel(text string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(text)); err != nil {
		return 0, fmt.Errorf("invalid log level %q (want debug, info, warn or error)", text)
	}
	return level, nil
}

func newLogger(levelText string, out io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(levelText)
	if err != nil {
		return nil, err
	}
	return slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level})), nil
}

func resolvedWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

type workerRun func(context.Context, *workersdk.Worker) error

func runWithContext(ctx context.Context, args []string, stdout io.Writer, start workerRun) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	logger, err := newLogger(cfg.logLevel, stdout)
	if err != nil {
		return err
	}
	if cfg.id == "" {
		cfg.id = resolvedWorkerID()
	}
	w := workersdk.New(cfg.broker, workersdk.Options{
		ID: cfg.id, Concurrency: cfg.concurrency, LeaseDuration: cfg.leaseDuration,
		ShutdownTimeout: cfg.shutdownTimeout, Logger: logger,
	})
	w.Handle("sleep", sleepHandler)
	w.Handle("fail_n_times", failNTimesHandler)
	w.Handle("cpu_burn", cpuBurnHandler)
	logger.Info("worker starting", "worker_id", cfg.id, "broker", cfg.broker,
		"concurrency", cfg.concurrency, "lease_duration", cfg.leaseDuration.String(),
		"shutdown_timeout", cfg.shutdownTimeout.String())
	if err := start(ctx, w); err != nil {
		return err
	}
	logger.Info("worker stopped", "worker_id", cfg.id)
	return nil
}

func run(args []string, stdout io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return runWithContext(ctx, args, stdout,
		func(ctx context.Context, w *workersdk.Worker) error { return w.Run(ctx) })
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}
