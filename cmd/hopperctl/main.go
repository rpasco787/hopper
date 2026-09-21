// Command hopperctl contains the producer-side command-line tools for Hopper.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
)

const usageText = `usage: hopperctl enqueue [flags]

Enqueue jobs in bounded batches. DLQ operations are not available yet.
`

type enqueueConfig struct {
	broker, queue, payload, keyPrefix string
	n, batch, priority, maxAttempts   int64
	timeout                           time.Duration
}

func parseEnqueueConfig(args []string) (enqueueConfig, error) {
	c := enqueueConfig{
		broker: "127.0.0.1:9090", payload: `{}`, n: 1, batch: 100, timeout: time.Minute,
	}
	fs := flag.NewFlagSet("hopperctl enqueue", flag.ContinueOnError)
	fs.StringVar(&c.broker, "broker", c.broker, "broker gRPC address (HOPPER_BROKER)")
	fs.StringVar(&c.queue, "queue", "", "destination queue (required)")
	fs.StringVar(&c.payload, "payload", c.payload, "JSON payload")
	fs.Int64Var(&c.n, "n", c.n, "number of jobs")
	fs.Int64Var(&c.batch, "batch", c.batch, "jobs per RPC (1..1000)")
	fs.Int64Var(&c.priority, "priority", 0, "signed 16-bit priority")
	fs.Int64Var(&c.maxAttempts, "max-attempts", 0, "maximum attempts; zero uses broker default")
	fs.StringVar(&c.keyPrefix, "key-prefix", "", "stable idempotency-key prefix")
	fs.DurationVar(&c.timeout, "timeout", c.timeout, "overall command timeout")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	brokerSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "broker" {
			brokerSet = true
		}
	})
	if env, ok := os.LookupEnv("HOPPER_BROKER"); ok && !brokerSet {
		c.broker = env
	}
	if c.broker == "" {
		return c, errors.New("broker address is required")
	}
	if c.queue == "" {
		return c, errors.New("queue is required")
	}
	if c.n <= 0 {
		return c, errors.New("n must be positive")
	}
	if c.batch < 1 || c.batch > 1000 {
		return c, errors.New("batch must be between 1 and 1000")
	}
	if !json.Valid([]byte(c.payload)) {
		return c, errors.New("payload must be valid JSON")
	}
	if c.priority < math.MinInt16 || c.priority > math.MaxInt16 {
		return c, errors.New("priority must fit in int16")
	}
	if c.maxAttempts < 0 || c.maxAttempts > math.MaxInt32 {
		return c, errors.New("max-attempts must be between 0 and 2147483647")
	}
	if c.timeout <= 0 {
		return c, errors.New("timeout must be positive")
	}
	return c, nil
}

type producer interface {
	EnqueueBatch(context.Context, []client.Job) ([]client.Result, error)
	Close() error
}

type dialProducer func(string) (producer, error)

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer, dial dialProducer) (err error) {
	if len(args) == 0 {
		return errors.New("usage: hopperctl enqueue [flags]; DLQ operations are not available yet")
	}
	if args[0] != "enqueue" {
		return fmt.Errorf("unknown command %q; DLQ operations are not available yet", args[0])
	}
	cfg, err := parseEnqueueConfig(args[1:])
	if err != nil {
		return err
	}
	p, err := dial(cfg.broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer func() { err = errors.Join(err, p.Close()) }()

	commandCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	confirmed, deduplicated := int64(0), int64(0)
	for start := int64(0); start < cfg.n; {
		remaining := cfg.n - start
		size := min(cfg.batch, remaining)
		jobs := make([]client.Job, int(size))
		for offset := int64(0); offset < size; offset++ {
			index := start + offset
			job := client.Job{
				Queue: cfg.queue, Payload: json.RawMessage(cfg.payload),
				Priority: int16(cfg.priority), MaxAttempts: int32(cfg.maxAttempts),
			}
			if cfg.keyPrefix != "" {
				job.IdempotencyKey = fmt.Sprintf("%s-%d", cfg.keyPrefix, index)
			}
			jobs[int(offset)] = job
		}

		results, callErr := p.EnqueueBatch(commandCtx, jobs)
		if callErr != nil {
			end := start + size
			fmt.Fprintf(stderr, "enqueue failed after %d confirmed jobs; indexes [%d,%d) are ambiguous: writes may have committed. ",
				confirmed, start, end)
			if cfg.keyPrefix != "" {
				fmt.Fprintf(stderr, "Rerun with the same -key-prefix %q to deduplicate while retained rows exist. ", cfg.keyPrefix)
			} else {
				fmt.Fprint(stderr, "no stable key prefix was supplied; do not blindly retry this batch. ")
			}
			if status.Code(callErr) == codes.ResourceExhausted {
				fmt.Fprint(stderr, "Use a smaller -batch for future attempts; do not resend this batch without stable keys. ")
			}
			fmt.Fprintln(stderr)
			return fmt.Errorf("enqueue indexes [%d,%d): %w", start, end, callErr)
		}
		confirmed += int64(len(results))
		for _, result := range results {
			if result.Deduplicated {
				deduplicated++
			}
		}
		start += size
	}
	fmt.Fprintf(stdout, "acknowledged=%d deduplicated=%d\n", confirmed, deduplicated)
	return nil
}

func realDial(target string) (producer, error) { return client.Dial(target) }

func run(args []string, stdout, stderr io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return runWithContext(ctx, args, stdout, stderr, realDial)
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(os.Stderr, usageText)
			return
		}
		fmt.Fprintf(os.Stderr, "hopperctl: %v\n", err)
		os.Exit(1)
	}
}
