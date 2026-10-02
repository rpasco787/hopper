// Command loadgen offers a measured load to hopper brokers and reports what
// came back: enqueue latency per call, and, once every job has finished,
// pickup and end-to-end latency per job and the completed throughput
// (docs/BENCHMARKS.md).
//
// It speaks only the public gRPC API. It assumes it owns -queue for the
// length of the run, which a fresh benchmark stack guarantees: it waits until
// the queue holds no unfinished job, then reads back only the jobs it
// enqueued.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

const (
	maxProducers    = 1024
	maxBatch        = 1000 // the broker's EnqueueBatch limit
	maxPayloadBytes = 1 << 20
)

type config struct {
	brokers      []string
	queue, run   string
	producers    int
	batch        int
	payloadBytes int
	workMs       int64
	// rate is the offered load in jobs per second across all producers;
	// zero sends each producer's next call as soon as the last one answers.
	rate float64
	// n and duration bound the run; whichever is reached first ends it.
	n            int64
	duration     time.Duration
	maxAttempts  int
	keys         bool
	wait         bool
	drainTimeout time.Duration
	rpcTimeout   time.Duration
	jsonOut      bool
}

func parseConfig(args []string) (config, error) {
	c := config{producers: 4, batch: 1, wait: true, drainTimeout: 5 * time.Minute, rpcTimeout: 10 * time.Second}
	var brokers string
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.StringVar(&brokers, "broker", "127.0.0.1:9090", "comma-separated broker addresses; producer i uses broker i mod N (HOPPER_BROKER)")
	fs.StringVar(&c.queue, "queue", "sleep", "queue to enqueue into; loadgen assumes it owns it")
	fs.StringVar(&c.run, "run", "", "run id for logs and idempotency keys; empty takes lg-<unix time>")
	fs.IntVar(&c.producers, "producers", c.producers, "concurrent producers")
	fs.IntVar(&c.batch, "batch", c.batch, "jobs per call; 1 uses Enqueue, more uses EnqueueBatch (max 1000)")
	fs.IntVar(&c.payloadBytes, "payload-bytes", 0, "pad each payload to at least this many bytes")
	fs.Int64Var(&c.workMs, "work-ms", 0, "handler milliseconds, sent as the sleep handler's ms")
	fs.Float64Var(&c.rate, "rate", 0, "offered jobs per second across all producers; 0 is unthrottled")
	fs.Int64Var(&c.n, "n", 0, "jobs to enqueue; 0 means until -duration")
	fs.DurationVar(&c.duration, "duration", 0, "how long to produce; 0 means until -n")
	fs.IntVar(&c.maxAttempts, "max-attempts", 0, "max_attempts per job; 0 takes the broker default")
	fs.BoolVar(&c.keys, "keys", false, "give every job a unique idempotency key")
	fs.BoolVar(&c.wait, "wait", c.wait, "wait for every job to finish and report pickup and end-to-end latency")
	fs.DurationVar(&c.drainTimeout, "drain-timeout", c.drainTimeout, "how long to wait for the queue to drain")
	fs.DurationVar(&c.rpcTimeout, "rpc-timeout", c.rpcTimeout, "deadline for one call")
	fs.BoolVar(&c.jsonOut, "json", false, "print the report as JSON on stdout; the summary goes to stderr")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	brokerSet := false
	fs.Visit(func(f *flag.Flag) { brokerSet = brokerSet || f.Name == "broker" })
	if env, ok := os.LookupEnv("HOPPER_BROKER"); ok && !brokerSet {
		brokers = env
	}
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b == "" {
			return c, errors.New("broker list contains an empty address")
		}
		c.brokers = append(c.brokers, strings.TrimSpace(b))
	}
	if c.run == "" {
		c.run = fmt.Sprintf("lg-%d", time.Now().Unix())
	}
	switch {
	case c.queue == "":
		return c, errors.New("queue is required")
	case c.producers < 1 || c.producers > maxProducers:
		return c, fmt.Errorf("producers must be between 1 and %d", maxProducers)
	case c.batch < 1 || c.batch > maxBatch:
		return c, fmt.Errorf("batch must be between 1 and %d", maxBatch)
	case c.payloadBytes < 0 || c.payloadBytes > maxPayloadBytes:
		return c, fmt.Errorf("payload-bytes must be between 0 and %d", maxPayloadBytes)
	case c.workMs < 0:
		return c, errors.New("work-ms must not be negative")
	case c.rate < 0:
		return c, errors.New("rate must not be negative")
	case c.n < 0 || c.duration < 0:
		return c, errors.New("n and duration must not be negative")
	case c.n == 0 && c.duration == 0:
		return c, errors.New("set -n, -duration or both")
	case c.maxAttempts < 0:
		return c, errors.New("max-attempts must not be negative")
	case c.drainTimeout <= 0 || c.rpcTimeout <= 0:
		return c, errors.New("drain-timeout and rpc-timeout must be positive")
	}
	return c, nil
}

// payload is the one body every job carries: the sleep handler's ms, padded
// with a "pad" string to at least payloadBytes.
func payload(workMs int64, payloadBytes int) []byte {
	body := fmt.Sprintf(`{"ms":%d}`, workMs)
	if pad := payloadBytes - len(body) - len(`,"pad":""`); pad > 0 {
		body = fmt.Sprintf(`{"ms":%d,"pad":"%s"}`, workMs, strings.Repeat("x", pad))
	}
	return []byte(body)
}

// api is the part of the generated client loadgen calls, so tests can stand
// in for a broker.
type api interface {
	Enqueue(context.Context, *hopperv1.EnqueueRequest, ...grpc.CallOption) (*hopperv1.EnqueueResponse, error)
	EnqueueBatch(context.Context, *hopperv1.EnqueueBatchRequest, ...grpc.CallOption) (*hopperv1.EnqueueBatchResponse, error)
	ListJobs(context.Context, *hopperv1.ListJobsRequest, ...grpc.CallOption) (*hopperv1.ListJobsResponse, error)
}

type dialer func(target string) (api, io.Closer, error)

func realDial(target string) (api, io.Closer, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", target, err)
	}
	return hopperv1.NewHopperServiceClient(conn), conn, nil
}

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer, dial dialer) (err error) {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	clients := make([]api, len(cfg.brokers))
	for i, target := range cfg.brokers {
		c, closer, err := dial(target)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, closer.Close()) }()
		clients[i] = c
	}

	rep := &Report{
		Run: cfg.run, Brokers: cfg.brokers, Queue: cfg.queue, Producers: cfg.producers,
		Batch: cfg.batch, WorkMs: cfg.workMs, PayloadBytes: cfg.payloadBytes, OfferedRate: cfg.rate,
	}
	ids := produce(ctx, cfg, clients, rep)
	if cfg.wait && rep.Errors == 0 {
		if err := await(ctx, cfg, clients[0], ids, rep); err != nil {
			rep.FirstError = err.Error()
			rep.Errors++
		}
	}
	rep.writeSummary(stderr)
	if cfg.jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			return err
		}
	}
	if rep.Failed() {
		return fmt.Errorf("run %s failed: %d errors (first: %s), %d unfinished",
			rep.Run, rep.Errors, rep.FirstError, rep.Unfinished)
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := runWithContext(ctx, os.Args[1:], os.Stdout, os.Stderr, realDial); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}
