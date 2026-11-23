// Command worker runs the Hopper worker SDK with three small example handlers.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rpasco787/hopper/internal/metrics"
	workersdk "github.com/rpasco787/hopper/worker"
)

const maxWorkerConcurrency = 1 << 16

type config struct {
	broker, id, logLevel           string
	metricsListen                  string
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
	{"metrics-listen", "HOPPER_METRICS_LISTEN"},
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
	fs.StringVar(&c.metricsListen, "metrics-listen", "", "Prometheus HTTP address; empty disables metrics (HOPPER_METRICS_LISTEN)")
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
	if c.metricsListen != "" {
		if _, _, err := net.SplitHostPort(c.metricsListen); err != nil {
			return c, errors.New("invalid metrics-listen address")
		}
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

var workerHandlers = []struct {
	queue   string
	handler workersdk.HandlerFunc
}{
	{queue: "sleep", handler: sleepHandler},
	{queue: "fail_n_times", handler: failNTimesHandler},
	{queue: "cpu_burn", handler: cpuBurnHandler},
}

func runWithContext(ctx context.Context, args []string, stdout io.Writer, start workerRun, suppliedMetricsListener ...net.Listener) error {
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
	if len(suppliedMetricsListener) > 1 {
		return errors.New("worker: more than one metrics listener supplied")
	}
	var (
		observer   workersdk.Observer
		httpSrv    *http.Server
		metricsLis net.Listener
		metricsErr chan error
	)
	if cfg.metricsListen != "" {
		reg := metrics.NewRegistry()
		queues := make([]string, 0, len(workerHandlers))
		for _, registered := range workerHandlers {
			queues = append(queues, registered.queue)
		}
		wm, err := metrics.NewWorker(reg, queues)
		if err != nil {
			return err
		}
		observer = wm
		if len(suppliedMetricsListener) == 1 {
			metricsLis = suppliedMetricsListener[0]
		} else {
			metricsLis, err = net.Listen("tcp", cfg.metricsListen)
			if err != nil {
				return fmt.Errorf("metrics listen: %w", err)
			}
		}
		defer metricsLis.Close()
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
	w := workersdk.New(cfg.broker, workersdk.Options{
		ID: cfg.id, Concurrency: cfg.concurrency, LeaseDuration: cfg.leaseDuration,
		ShutdownTimeout: cfg.shutdownTimeout, Logger: logger, Observer: observer,
	})
	for _, registered := range workerHandlers {
		w.Handle(registered.queue, registered.handler)
	}
	logger.Info("worker starting", "worker_id", cfg.id, "broker", cfg.broker,
		"concurrency", cfg.concurrency, "lease_duration", cfg.leaseDuration.String(),
		"shutdown_timeout", cfg.shutdownTimeout.String())
	if httpSrv == nil {
		if err := start(ctx, w); err != nil {
			return err
		}
		logger.Info("worker stopped", "worker_id", cfg.id)
		return nil
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	startErr := make(chan error, 1)
	go func() { startErr <- start(workerCtx, w) }()
	var (
		runErr          error
		metricsServeErr error
		serveResultRead bool
	)
	select {
	case runErr = <-startErr:
	case metricsServeErr = <-metricsErr:
		serveResultRead = true
		if !errors.Is(metricsServeErr, http.ErrServerClosed) {
			cancelWorker()
		}
		runErr = <-startErr
	}

	metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 2*time.Second)
	metricsShutdownErr := httpSrv.Shutdown(metricsCtx)
	cancelMetrics()
	if metricsShutdownErr != nil {
		metricsShutdownErr = errors.Join(metricsShutdownErr, httpSrv.Close())
	}
	if !serveResultRead {
		metricsServeErr = <-metricsErr
	}
	if errors.Is(metricsServeErr, http.ErrServerClosed) {
		metricsServeErr = nil
	}
	if err := errors.Join(runErr, metricsServeErr, metricsShutdownErr); err != nil {
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
