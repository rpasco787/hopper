// Package client is a producer-side wrapper around the generated hopper.v1
// client.
//
// It exists to keep three things out of calling code: proto message types,
// base64-encoded payloads (proto3 JSON encodes `bytes` that way, which
// surprises everyone once), and the raw status codes. In exchange it offers
// exactly what a producer needs — enqueue, cancel, get — and nothing a worker
// needs. Workers use the Session 5 SDK, which speaks the streaming half of the
// API this package deliberately does not wrap.
//
// It is a public package, unlike everything under internal/, because a
// producer is by definition someone else's program.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// Errors a producer can reasonably branch on. They mirror the store's
// sentinels across the wire, so a caller need not know gRPC codes.
var (
	ErrNotFound = errors.New("hopper: job not found")
	ErrTerminal = errors.New("hopper: job is already in a terminal state")
)

// Client is safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn
	rpc  hopperv1.HopperServiceClient
}

// Dial connects to a broker. The connection is lazy: this does not fail
// because the broker is down, it fails on the first call, which is what you
// want in a producer that starts before its broker does.
//
// The default is plaintext, because the broker is a trusted-network prototype
// that serves plaintext. Options come after that default, so passing
// grpc.WithTransportCredentials replaces it.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	// Defaults precede caller options, so explicit credentials can override them.
	base := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(target, append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("hopper: dial %s: %w", target, err)
	}
	return &Client{conn: conn, rpc: hopperv1.NewHopperServiceClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Job is what a producer sends.
type Job struct {
	Queue string
	// Payload must marshal to JSON. Pass json.RawMessage to send bytes you
	// already have; a plain []byte would marshal as a base64 string.
	Payload  any
	Priority int16
	// RunAt zero means now.
	RunAt time.Time
	// MaxAttempts zero takes the broker default.
	MaxAttempts int32
	// IdempotencyKey empty means no dedup. Set it and Enqueue is safe to
	// retry: the second call returns the first job's id with Deduplicated set.
	IdempotencyKey string
}

// Result is what came back.
type Result struct {
	ID    uuid.UUID
	State string
	// Deduplicated is true when an existing job was returned and nothing was
	// inserted (docs/semantics.md §8).
	Deduplicated bool
}

func spec(j Job) (*hopperv1.JobSpec, error) {
	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return nil, fmt.Errorf("hopper: marshal payload: %w", err)
	}
	if j.Payload == nil {
		payload = nil
	}
	s := &hopperv1.JobSpec{
		Queue:       j.Queue,
		Payload:     payload,
		Priority:    int32(j.Priority),
		MaxAttempts: j.MaxAttempts,
	}
	if !j.RunAt.IsZero() {
		s.RunAt = timestamppb.New(j.RunAt)
	}
	if j.IdempotencyKey != "" {
		s.IdempotencyKey = &j.IdempotencyKey
	}
	return s, nil
}

// Enqueue adds one job. There is no automatic retry: a failure here can be
// ambiguous (the write may have committed), so retrying is the caller's
// decision, and it is only safe with an IdempotencyKey.
func (c *Client) Enqueue(ctx context.Context, j Job) (Result, error) {
	s, err := spec(j)
	if err != nil {
		return Result{}, err
	}
	resp, err := c.rpc.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: s})
	if err != nil {
		return Result{}, wrap("enqueue", err)
	}
	return toResult(resp.GetJob(), resp.GetDeduplicated())
}

// EnqueueBatch is non-atomic. A failure can leave committed jobs; stable
// idempotency keys deduplicate retries while the original rows still exist.
// It does not retry by itself.
func (c *Client) EnqueueBatch(ctx context.Context, jobs []Job) ([]Result, error) {
	specs := make([]*hopperv1.JobSpec, len(jobs))
	for i, j := range jobs {
		s, err := spec(j)
		if err != nil {
			return nil, fmt.Errorf("hopper: jobs[%d]: %w", i, err)
		}
		specs[i] = s
	}
	resp, err := c.rpc.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: specs})
	if err != nil {
		return nil, wrap("enqueue batch", err)
	}
	out := make([]Result, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		res, err := toResult(r.GetJob(), r.GetDeduplicated())
		if err != nil {
			return nil, err
		}
		out = append(out, res)
	}
	return out, nil
}

// Cancel is idempotent: cancelling an already-cancelled job succeeds. A
// completed or dead job returns ErrTerminal.
func (c *Client) Cancel(ctx context.Context, id uuid.UUID) (Result, error) {
	resp, err := c.rpc.Cancel(ctx, &hopperv1.CancelRequest{Id: id.String()})
	if err != nil {
		return Result{}, wrap("cancel", err)
	}
	return toResult(resp.GetJob(), false)
}

func (c *Client) GetJob(ctx context.Context, id uuid.UUID) (Result, error) {
	resp, err := c.rpc.GetJob(ctx, &hopperv1.GetJobRequest{Id: id.String()})
	if err != nil {
		return Result{}, wrap("get job", err)
	}
	return toResult(resp.GetJob(), false)
}

func toResult(j *hopperv1.Job, dedup bool) (Result, error) {
	id, err := uuid.Parse(j.GetId())
	if err != nil {
		return Result{}, fmt.Errorf("hopper: broker returned a malformed id %q: %w", j.GetId(), err)
	}
	return Result{ID: id, State: j.GetState().String(), Deduplicated: dedup}, nil
}

// wrap turns a status back into an error a caller can branch on with
// errors.Is, keeping the status underneath for anyone who wants the code.
func wrap(op string, err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("hopper: %s: %w", op, errors.Join(ErrNotFound, err))
	case codes.FailedPrecondition:
		return fmt.Errorf("hopper: %s: %w", op, errors.Join(ErrTerminal, err))
	default:
		return fmt.Errorf("hopper: %s: %w", op, err)
	}
}
