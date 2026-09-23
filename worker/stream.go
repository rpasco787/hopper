package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// errOverDelivery ends a stream that sent more jobs than it was given credit
// for. The excess job is not started and not counted: the alternative is to
// manufacture a token, which would break the one bound the worker promises.
var errOverDelivery = errors.New("worker: the broker exceeded the credit it was given")

// errStreamClosed is the cause recorded when a stream ended without a failure:
// the broker answered the half-close, or sent its final status cleanly.
var errStreamClosed = errors.New("worker: lease stream closed")

// capacity is the worker's fixed pool of concurrency tokens. At any moment
// every token is in exactly one of three places:
//
//	free tokens + credit outstanding on the stream + active deliveries = Concurrency
//
// A token held by a delivery covers everything that delivery does — validating
// the lease, running the handler, and confirming the outcome — because a job
// whose outcome is still unknown has not freed a slot for anything else. That
// includes a handler that ignored its cancellation: it keeps its token until it
// returns, since letting a new handler start in its place is exactly how a
// worker ends up running more jobs than it said it would.
type capacity struct {
	free chan struct{}
	size int
	log  *slog.Logger
}

func newCapacity(n int, log *slog.Logger) *capacity {
	c := &capacity{free: make(chan struct{}, n), size: n, log: log}
	for range n {
		c.free <- struct{}{}
	}
	return c
}

// takeAll claims every currently free token without blocking.
func (c *capacity) takeAll() int {
	n := 0
	for {
		select {
		case <-c.free:
			n++
		default:
			return n
		}
	}
}

// release returns one token. Releasing more than were taken is an accounting
// bug in the SDK rather than anything a deployment can cause, so it is logged
// loudly and dropped: raising the ceiling instead would be a silent breach of
// the concurrency limit the caller configured.
func (c *capacity) release() {
	select {
	case c.free <- struct{}{}:
	default:
		c.log.Error("worker: released more capacity than exists; this is an SDK bug", "concurrency", c.size)
	}
}

// delivery is one validated job off the stream, with everything the SDK needs
// to own its lease.
type delivery struct {
	job     Job
	leaseID uuid.UUID
	// grant is lease_expires - leased_at: how long the broker gave this
	// delivery. It is clock-skew independent because both instants come from
	// the same database clock. It is *not* the time remaining at receipt —
	// nothing in the message can say that — which is why the SDK confirms
	// ownership with a heartbeat before starting a handler.
	grant time.Duration
	// receivedAt is the local monotonic instant this delivery was read off the
	// stream. Acquisition is budgeted from here.
	receivedAt time.Time
	// handler is nil when nothing is registered for this queue. Such a job is
	// nacked unstarted rather than dropped.
	handler HandlerFunc
}

// acquisitionBudget is how long this delivery may spend proving it still holds
// its lease. It is fixed at receipt rather than recomputed per attempt, so a
// slow broker cannot walk a delivery forward indefinitely: at worst the whole
// grant is spent trying, and then the job is left to be redelivered.
func (d *delivery) acquisitionBudget() time.Time {
	return d.receivedAt.Add(d.grant - safetyMargin(d.grant))
}

// deliveryRunner owns a delivery from dispatch to completion. It is a field on
// Worker so tests can substitute a controlled runner without a fake broker
// having to stage a whole execution.
type deliveryRunner func(lc *lifecycle, wk *work, rpc hopperv1.HopperServiceClient, d *delivery)

// session is one lease stream and its accounting. A new stream is a new
// session; deliveries from the old one keep running and keep their tokens.
type session struct {
	w    *Worker
	lc   *lifecycle
	rpc  hopperv1.HopperServiceClient
	pool *capacity
	log  *slog.Logger

	stream hopperv1.HopperService_LeaseJobsClient

	mu sync.Mutex
	// outstanding is credit the broker may still spend on this stream. It is
	// incremented before a Send, so a delivery that arrives before that Send
	// returns still has accounting behind it.
	outstanding int
	delivered   int
	reconciled  bool
}

// run holds one stream until it ends, then returns the reason.
func (s *session) run() error {
	// The stream context exists before LeaseJobs can block on it, so a forced
	// shutdown during establishment cancels the attempt rather than waiting
	// for it. It descends from the force context, which is what lets the
	// deadline reach the transport even when the send owner is stuck.
	streamCtx, cancel := context.WithCancelCause(s.lc.forceCtx)
	defer cancel(errStreamClosed)

	stream, err := s.rpc.LeaseJobs(streamCtx)
	if err != nil {
		return s.fail(fmt.Errorf("worker: open lease stream: %w", err))
	}
	s.stream = stream

	// Advertise only what is free now. After a reconnect that is Concurrency
	// minus the handlers still running from the previous stream, whose leases
	// this new stream does not revoke.
	initial := s.pool.takeAll()
	s.reserve(initial)
	sub := &hopperv1.Subscription{
		Queues:   s.w.queues(),
		WorkerId: s.w.opts.ID,
		Credit:   uint32(initial),
	}
	if s.w.opts.LeaseDuration > 0 {
		sub.LeaseDuration = durationpb.New(s.w.opts.LeaseDuration)
	}
	if err := stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: sub},
	}); err != nil {
		// No reader exists yet, so this reservation can only be returned here.
		// reconcile is idempotent, so the teardown path below cannot double it.
		s.reconcile()
		return s.fail(fmt.Errorf("worker: subscribe: %w", err))
	}
	s.log.Info("lease stream opened", "queues", sub.GetQueues(), "credit", initial)

	// Each side ends the stream with its own reason when it stops, so whichever
	// stopped first is the reason the session ended and the other's error is
	// the consequence. Without this the survivor blocks: a reader that failed
	// leaves the writer waiting for a token that will never buy anything, and a
	// writer that failed leaves the reader on a stream that can never produce
	// another job.
	readDone := make(chan error, 1)
	go func() {
		err := s.read()
		cancel(cmp.Or(err, errStreamClosed))
		readDone <- err
	}()

	if err := s.write(streamCtx); err != nil {
		cancel(err)
	}
	// Wait for the final status where there is one. A broker that never answers
	// the half-close is bounded by the shared deadline, not by a second timeout
	// of its own.
	<-readDone
	s.reconcile()
	s.log.Info("lease stream closed", "delivered", s.deliveryCount())

	cause := context.Cause(streamCtx)
	if errors.Is(cause, errStreamClosed) {
		return nil
	}
	return s.fail(cause)
}

// fail reports err unless the SDK's own forced shutdown caused it. A transport
// the deadline cancelled did not fail; it was stopped on purpose, and saying
// otherwise would turn every abandoned shutdown into a spurious consume error
// alongside the abandonment that actually describes what happened.
func (s *session) fail(err error) error {
	if s.lc.forceCtx.Err() != nil {
		s.log.Debug("lease stream ended by forced shutdown", "cause", err)
		return nil
	}
	return err
}

// read consumes deliveries until the stream ends.
func (s *session) read() error {
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("worker: lease stream: %w", err)
		}
		if err := s.accept(resp.GetJob()); err != nil {
			return err
		}
	}
}

// accept turns one message into a running delivery, or explains why it cannot.
// It never makes a network call: the reader must stay free to observe the next
// message, including the EOF that ends the stream.
func (s *session) accept(p *hopperv1.Job) error {
	if !s.spend() {
		return fmt.Errorf("%w: job %q arrived with no credit outstanding", errOverDelivery, p.GetId())
	}
	d, err := parseDelivery(p)
	if err != nil {
		// Nothing in a malformed message can be trusted, so there is no id and
		// lease pair worth nacking. Return the capacity and say what arrived.
		s.log.Error("discarding a malformed delivery", "err", err,
			"job_id", p.GetId(), "lease_id", p.GetLeaseId())
		s.pool.release()
		return nil
	}
	d.handler = s.w.handlers[d.job.Queue]
	s.dispatch(d)
	return nil
}

// dispatch runs a delivery on its own goroutine. Everything after this point —
// the ownership heartbeat, the handler, the confirmation — happens there, so a
// slow broker cannot stall the reader.
func (s *session) dispatch(d *delivery) {
	wk := s.lc.begin()
	go func() {
		// The token goes back before the delivery is struck from the counts,
		// so nothing can observe a drained worker that is still short a slot.
		defer wk.end()
		defer s.pool.release()
		s.w.runner(s.lc, wk, s.rpc, d)
	}()
}

// write owns every Send and the half-close. One owner means no interleaved
// writes and no race over who closes the stream.
func (s *session) write(ctx context.Context) error {
	for {
		taken := 0
		select {
		case <-s.pool.free:
			taken = 1
		case <-s.lc.drainSignal:
			return s.halfClose()
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		taken += s.pool.takeAll()

		// A token freeing at the same instant the caller cancels must not buy
		// more work: check drain again now that we hold them.
		select {
		case <-s.lc.drainSignal:
			s.reserve(taken)
			return s.halfClose()
		default:
		}

		// Reserve before sending. If this Send is in flight when a delivery
		// answers it, the accounting is already there; if it fails, the
		// reservation stays and teardown returns it exactly once. Refunding
		// here as well would hand the pool a token it still holds.
		s.reserve(taken)
		if err := s.stream.Send(&hopperv1.LeaseJobsRequest{
			Msg: &hopperv1.LeaseJobsRequest_Credit{Credit: uint32(taken)},
		}); err != nil {
			return fmt.Errorf("worker: send credit: %w", err)
		}
	}
}

func (s *session) halfClose() error {
	if err := s.stream.CloseSend(); err != nil {
		return fmt.Errorf("worker: half-close lease stream: %w", err)
	}
	return nil
}

func (s *session) reserve(n int) {
	if n == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outstanding += n
}

// spend accounts one arriving delivery against outstanding credit.
func (s *session) spend() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.outstanding == 0 {
		return false
	}
	s.outstanding--
	s.delivered++
	return true
}

func (s *session) deliveryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered
}

// reconcile returns credit this stream will never spend. It runs once, after
// both the reader and the writer have stopped: until then a delivery could
// still be arriving against a Send whose outcome is unknown, and refunding it
// early would let the worker run more handlers than it has tokens for.
func (s *session) reconcile() {
	s.mu.Lock()
	if s.reconciled {
		s.mu.Unlock()
		return
	}
	s.reconciled = true
	n := s.outstanding
	s.outstanding = 0
	s.mu.Unlock()

	for range n {
		s.pool.release()
	}
}

// parseDelivery validates a leased row the way the broker's own conversion
// built it. Everything a delivery needs must be present and sensible before a
// handler can be invoked: an invented default here would be the SDK asserting
// ownership of a job on no evidence.
func parseDelivery(p *hopperv1.Job) (*delivery, error) {
	if p == nil {
		return nil, errors.New("the message carried no job")
	}
	id, err := uuid.Parse(p.GetId())
	if err != nil {
		return nil, fmt.Errorf("job id %q: %w", p.GetId(), err)
	}
	if p.GetQueue() == "" {
		return nil, fmt.Errorf("job %s has no queue", id)
	}
	if p.GetState() != hopperv1.JobState_JOB_STATE_LEASED {
		return nil, fmt.Errorf("job %s arrived in state %s, not leased", id, p.GetState())
	}
	if p.LeaseId == nil {
		return nil, fmt.Errorf("job %s has no lease id", id)
	}
	leaseID, err := uuid.Parse(p.GetLeaseId())
	if err != nil {
		return nil, fmt.Errorf("job %s lease id %q: %w", id, p.GetLeaseId(), err)
	}
	if leaseID == uuid.Nil {
		return nil, fmt.Errorf("job %s has the nil lease id", id)
	}
	leasedAt, err := instant("leased_at", p.GetLeasedAt())
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	expires, err := instant("lease_expires", p.GetLeaseExpires())
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	grant := expires.Sub(leasedAt)
	if grant <= 0 {
		return nil, fmt.Errorf("job %s has a lease of %s", id, grant)
	}
	createdAt, err := instant("created_at", p.GetCreatedAt())
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	// attempts is counted at lease time, so a leased row is on at least its
	// first delivery; max_attempts is never stored as zero (the store
	// substitutes its default at enqueue).
	if p.GetAttempts() < 1 {
		return nil, fmt.Errorf("job %s reports attempt %d", id, p.GetAttempts())
	}
	if p.GetMaxAttempts() < 1 {
		return nil, fmt.Errorf("job %s reports max attempts %d", id, p.GetMaxAttempts())
	}

	return &delivery{
		job: Job{
			ID:          id,
			Queue:       p.GetQueue(),
			Payload:     json.RawMessage(p.GetPayload()),
			Attempt:     p.GetAttempts(),
			MaxAttempts: p.GetMaxAttempts(),
			EnqueuedAt:  createdAt,
		},
		leaseID:    leaseID,
		grant:      grant,
		receivedAt: time.Now(),
	}, nil
}

func instant(field string, ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil {
		return time.Time{}, fmt.Errorf("%s is absent", field)
	}
	if err := ts.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("%s is invalid: %w", field, err)
	}
	return ts.AsTime(), nil
}
