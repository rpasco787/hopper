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

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

const usageText = `usage: hopperctl <enqueue|smoke> [flags]

enqueue: enqueue jobs in bounded batches.
smoke:   enqueue one job and wait until a worker completes it.

DLQ operations are not available yet.
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
	brokerFromEnv(fs, &c.broker)
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

// brokerFromEnv applies HOPPER_BROKER unless -broker was given explicitly.
func brokerFromEnv(fs *flag.FlagSet, broker *string) {
	brokerSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "broker" {
			brokerSet = true
		}
	})
	if env, ok := os.LookupEnv("HOPPER_BROKER"); ok && !brokerSet {
		*broker = env
	}
}

type smokeConfig struct {
	broker, queue, payload string
	timeout, poll          time.Duration
}

func parseSmokeConfig(args []string) (smokeConfig, error) {
	c := smokeConfig{
		broker: "127.0.0.1:9090", queue: "sleep", payload: `{"ms":10}`,
		timeout: time.Minute, poll: 250 * time.Millisecond,
	}
	fs := flag.NewFlagSet("hopperctl smoke", flag.ContinueOnError)
	fs.StringVar(&c.broker, "broker", c.broker, "broker gRPC address (HOPPER_BROKER)")
	fs.StringVar(&c.queue, "queue", c.queue, "a queue some worker serves")
	fs.StringVar(&c.payload, "payload", c.payload, "JSON payload")
	fs.DurationVar(&c.timeout, "timeout", c.timeout, "how long to wait for completion")
	fs.DurationVar(&c.poll, "poll", c.poll, "interval between GetJob calls")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	brokerFromEnv(fs, &c.broker)
	switch {
	case c.broker == "":
		return c, errors.New("broker address is required")
	case c.queue == "":
		return c, errors.New("queue is required")
	case !json.Valid([]byte(c.payload)):
		return c, errors.New("payload must be valid JSON")
	case c.timeout <= 0:
		return c, errors.New("timeout must be positive")
	case c.poll <= 0:
		return c, errors.New("poll must be positive")
	}
	return c, nil
}

// producer is the broker calls hopperctl makes.
type producer interface {
	EnqueueBatch(context.Context, []client.Job) ([]client.Result, error)
	GetJob(context.Context, uuid.UUID) (client.Result, error)
	Close() error
}

type dialProducer func(string) (producer, error)

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer, dial dialProducer) error {
	if len(args) == 0 {
		return errors.New("usage: hopperctl <enqueue|smoke> [flags]; DLQ operations are not available yet")
	}
	switch args[0] {
	case "enqueue":
		return runEnqueue(ctx, args[1:], stdout, stderr, dial)
	case "smoke":
		return runSmoke(ctx, args[1:], stdout, dial)
	default:
		return fmt.Errorf("unknown command %q; want enqueue or smoke. DLQ operations are not available yet", args[0])
	}
}

func runSmoke(ctx context.Context, args []string, stdout io.Writer, dial dialProducer) (err error) {
	cfg, err := parseSmokeConfig(args)
	if err != nil {
		return err
	}
	p, err := dial(cfg.broker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer func() { err = errors.Join(err, p.Close()) }()
	return smoke(ctx, cfg, p, stdout)
}

var (
	stateCompleted = hopperv1.JobState_JOB_STATE_COMPLETED.String()
	stateDead      = hopperv1.JobState_JOB_STATE_DEAD.String()
	stateCancelled = hopperv1.JobState_JOB_STATE_CANCELLED.String()
)

// smoke enqueues one job and waits for a worker to complete it. Success needs
// a reachable broker, a writable database and a worker serving the queue, so
// it is the end-to-end check behind the chart's `helm test`.
func smoke(ctx context.Context, cfg smokeConfig, p producer, stdout io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	began := time.Now()
	results, err := p.EnqueueBatch(ctx, []client.Job{{Queue: cfg.queue, Payload: json.RawMessage(cfg.payload)}})
	if err != nil {
		return fmt.Errorf("smoke: enqueue: %w", err)
	}
	if len(results) != 1 {
		return fmt.Errorf("smoke: enqueue returned %d results for one job", len(results))
	}
	id, last := results[0].ID, results[0].State
	fmt.Fprintf(stdout, "enqueued job %s on queue %s\n", id, cfg.queue)

	ticker := time.NewTicker(cfg.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("smoke: job %s did not complete within %s (last state %s)", id, cfg.timeout, last)
		case <-ticker.C:
		}
		r, err := p.GetJob(ctx, id)
		if err != nil {
			// A broker restarting under us is survivable; anything else is an answer.
			if status.Code(err) == codes.Unavailable && ctx.Err() == nil {
				continue
			}
			return fmt.Errorf("smoke: get job %s: %w", id, err)
		}
		last = r.State
		switch last {
		case stateCompleted:
			fmt.Fprintf(stdout, "job %s completed after %s\n", id, time.Since(began).Round(time.Millisecond))
			return nil
		case stateDead, stateCancelled:
			return fmt.Errorf("smoke: job %s ended %s", id, last)
		}
	}
}

func runEnqueue(ctx context.Context, args []string, stdout, stderr io.Writer, dial dialProducer) (err error) {
	cfg, err := parseEnqueueConfig(args)
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
