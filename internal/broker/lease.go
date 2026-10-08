package broker

import (
	"context"
	"errors"
	"io"
	"math"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// LeaseJobs spends cooperative worker credits. A successful Send is not a
// receipt acknowledgment. Rows from earlier batches may still be buffered.
func (s *Service) LeaseJobs(stream hopperv1.HopperService_LeaseJobsServer) error {
	ctx, cancel := context.WithCancelCause(stream.Context())
	defer cancel(context.Canceled)
	stopped := func() error {
		cause := context.Cause(ctx)
		if errors.Is(cause, io.EOF) {
			return nil
		}
		return toStatus(cause)
	}
	shuttingDown := status.Error(codes.Unavailable, "broker is shutting down")
	// Covers the initial receive, a database call, and a transport-blocked send.
	go func() {
		select {
		case <-s.shutdown:
			cancel(shuttingDown)
		case <-ctx.Done():
		}
	}()

	type received struct {
		msg *hopperv1.LeaseJobsRequest
		err error
	}
	firstResult := make(chan received, 1)
	go func() {
		msg, err := stream.Recv()
		firstResult <- received{msg, err}
	}()
	// Raw Recv/Send unblock when gRPC cancels the transport after this handler
	// returns. Never join a transport-blocked goroutine before returning.
	var first *hopperv1.LeaseJobsRequest
	select {
	case result := <-firstResult:
		if result.err != nil {
			cancel(result.err)
			return stopped()
		}
		first = result.msg
	case <-ctx.Done():
		return stopped()
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return status.Error(codes.InvalidArgument, "the first message must be a subscription")
	}
	if len(sub.GetQueues()) == 0 {
		return status.Error(codes.InvalidArgument, "queues is required")
	}
	for i, q := range sub.GetQueues() {
		if q == "" {
			return status.Errorf(codes.InvalidArgument, "queues[%d] is empty", i)
		}
	}
	if sub.GetWorkerId() == "" {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}
	leaseDur, err := s.leaseDuration(sub.GetLeaseDuration())
	if err != nil {
		return err
	}
	// The reader never blocks on credit delivery: coalesce notifications while
	// retaining the count atomically. This lets it observe half-close even if
	// the writer is blocked and many credit messages precede EOF.
	var credit atomic.Uint64
	credit.Store(uint64(sub.GetCredit()))
	wake := make(chan struct{}, 1)
	// Registered before the first lease, so an enqueue that lands between an
	// empty lease and the wait still ends the wait.
	defer s.wakes.subscribe(wake, sub.GetQueues())()
	go func() {
		for {
			if ctx.Err() != nil {
				return
			}
			msg, err := stream.Recv()
			if err != nil {
				cancel(err)
				return
			}
			c, ok := msg.Msg.(*hopperv1.LeaseJobsRequest_Credit)
			if !ok {
				cancel(status.Error(codes.InvalidArgument, "only credit messages may follow subscribe"))
				return
			}
			if c.Credit == 0 {
				continue
			}
			var before uint64
			for {
				old := credit.Load()
				if old+uint64(c.Credit) > math.MaxUint32 {
					cancel(status.Error(codes.ResourceExhausted, "outstanding credit exceeds uint32 range"))
					return
				}
				if credit.CompareAndSwap(old, old+uint64(c.Credit)) {
					before = old
					break
				}
			}
			// Only a stream that ran out of credit is waiting for more. One that
			// still has some already took everything leasable at its last lease,
			// and more credit does not change that.
			if before == 0 {
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}()
	checkStop := func() error {
		select {
		case <-s.shutdown:
			cancel(shuttingDown)
		default:
		}
		return ctx.Err()
	}
	logger := s.cfg.Logger.With("worker_id", sub.GetWorkerId(), "queues", sub.GetQueues())
	logger.InfoContext(ctx, "lease stream opened", "credit", credit.Load())
	defer logger.InfoContext(ctx, "lease stream closed")
	for {
		if checkStop() != nil {
			return stopped()
		}
		if credit.Load() == 0 {
			select {
			case <-wake:
			case <-ctx.Done():
				return stopped()
			}
			continue
		}
		if checkStop() != nil {
			return stopped()
		}
		gen := s.wakes.generation() // before the lease, so park can tell if a wake slipped past it
		n := min(credit.Load(), uint64(s.cfg.MaxLeaseBatch))
		jobs, err := s.store.Lease(ctx, sub.GetQueues(), int(n), leaseDur, sub.GetWorkerId())
		if ctx.Err() != nil {
			return stopped()
		}
		if err != nil {
			return s.fail(ctx, "LeaseJobs", err)
		}
		for i, job := range jobs {
			if checkStop() != nil {
				return stopped()
			}
			p, err := jobToProto(job)
			if err != nil {
				return s.fail(ctx, "LeaseJobs", err)
			}
			sent := make(chan error, 1)
			// Only one Send runs at a time. The buffered result lets a blocked
			// writer exit after this handler returns and closes its transport.
			go func() { sent <- stream.Send(&hopperv1.LeaseJobsResponse{Job: p}) }()
			select {
			case err := <-sent:
				if err != nil {
					logger.WarnContext(ctx, "lease send failed",
						"job_id", p.GetId(), "lease_id", p.GetLeaseId(),
						"batch_not_confirmed_sent", len(jobs)-i, "err", err)
					return err
				}
				logger.DebugContext(ctx, "job leased",
					"job_id", p.GetId(), "lease_id", p.GetLeaseId(),
					"queue", jobs[i].Queue, "attempt", jobs[i].Attempts)
				credit.Add(^uint64(0)) // subtract one; only the writer spends credit
			case <-ctx.Done():
				return stopped()
			}
		}
		if uint64(len(jobs)) == n {
			continue // a full batch: the queue may hold more
		}
		// A short batch took everything leasable, so another lease right now
		// could only hear "nothing". Wait for a wakeup or the next poll.
		if !s.wakes.park(wake, gen) {
			continue // an enqueue went unserved while the lease ran
		}
		select {
		case <-time.After(s.cfg.PollInterval):
		case <-wake:
		case <-ctx.Done():
			return stopped()
		}
		s.wakes.unpark(wake)
	}
}
