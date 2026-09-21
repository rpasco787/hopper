package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// confirmRPCTimeout bounds one Ack or Nack attempt. The lease deadline and the
// shutdown deadline bound the sequence.
const confirmRPCTimeout = 5 * time.Second

// confirmation is how an attempt to report an outcome ended. The four are kept
// apart because they mean different things to whoever reads the log: a refused
// confirmation is a job someone else may now hold, while an unconfirmed one is
// a job that very likely ran and is about to be redelivered anyway.
type confirmation int

const (
	confirmed confirmation = iota
	confirmationRefused
	confirmationUnconfirmed
	confirmationAbandoned
)

func (c confirmation) String() string {
	switch c {
	case confirmed:
		return "confirmed"
	case confirmationRefused:
		return "refused"
	case confirmationUnconfirmed:
		return "unconfirmed"
	default:
		return "abandoned"
	}
}

// confirmer reports one delivery's outcome. Ack and Nack share it so that
// retry policy, bounds and identifier stability cannot drift apart.
type confirmer struct {
	rpc      hopperv1.HopperServiceClient
	log      *slog.Logger
	forceCtx context.Context
	lease    *lease
	jobID    uuid.UUID
	leaseID  uuid.UUID
}

func (c *confirmer) ack() (confirmation, error) {
	// The request is built once so every retry presents identical identifiers.
	req := &hopperv1.AckRequest{Id: c.jobID.String(), LeaseId: c.leaseID.String()}
	return c.confirm("ack", func(ctx context.Context) error {
		_, err := c.rpc.Ack(ctx, req)
		return err
	})
}

func (c *confirmer) nack(message string) (confirmation, error) {
	req := &hopperv1.NackRequest{
		Id:      c.jobID.String(),
		LeaseId: c.leaseID.String(),
		Error:   sanitizeError(message),
	}
	return c.confirm("nack", func(ctx context.Context) error {
		_, err := c.rpc.Nack(ctx, req)
		return err
	})
}

// confirm runs one confirmation to a conclusion.
//
// It stops at the earlier of the local lease deadline and the forced-shutdown
// deadline. Retrying past the lease would be pointless — the broker would
// refuse a token the reaper has taken back — and retrying past the shutdown
// deadline is the thing that makes a drain overrun its budget.
//
// A re-Ack is safe while the completed row still carries the token, which is
// why lease_id is never cleared on completion; a re-Nack is safe while the
// same-token row is scheduled or dead. Once the promoter moves that row to
// available, a retry can return ABORTED with the very same token. ABORTED
// therefore means "this delivery is no longer actionable", not that the job was
// re-leased, not that the reaper acted, and not that anything is running twice.
func (c *confirmer) confirm(op string, call func(context.Context) error) (confirmation, error) {
	b := newBackoff()
	for attempt := 1; ; attempt++ {
		deadline, cause := c.lease.state()
		if cause != nil {
			return confirmationUnconfirmed, cause
		}
		if err := c.forceCtx.Err(); err != nil {
			return confirmationAbandoned, context.Cause(c.forceCtx)
		}
		now := time.Now()
		if !now.Before(deadline) {
			return confirmationUnconfirmed, ErrLeaseExpired
		}

		ctx, cancel := context.WithDeadline(c.forceCtx, earlier(now.Add(confirmRPCTimeout), deadline))
		err := call(ctx)
		cancel()
		if err == nil {
			return confirmed, nil
		}
		if c.forceCtx.Err() != nil {
			return confirmationAbandoned, context.Cause(c.forceCtx)
		}
		if !retryableConfirmation(err) {
			return confirmationRefused, err
		}
		c.log.Debug("retrying a confirmation", "operation", op, "attempt", attempt, "err", err)

		// The wait is interruptible and never outlives the lease it is about.
		delay := min(b.next(), time.Until(deadline))
		if delay <= 0 {
			return confirmationUnconfirmed, ErrLeaseExpired
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-c.forceCtx.Done():
			timer.Stop()
			return confirmationAbandoned, context.Cause(c.forceCtx)
		}
	}
}

// retryableConfirmation is deliberately narrow. UNAVAILABLE and
// DEADLINE_EXCEEDED describe the network; everything else describes the job or
// the store. INTERNAL in particular is not retried: a generic PostgreSQL
// failure may be a permanent rejection of the input or an ambiguous commit, and
// hammering it recovers neither.
func retryableConfirmation(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// logConfirmation records what happened. "job confirmed" appears only after the
// broker accepted the call: logging completion at handler return would report
// success for jobs that are about to be redelivered.
func logConfirmation(log *slog.Logger, op string, outcome confirmation, err error) {
	switch outcome {
	case confirmed:
		log.Info("job confirmed", "operation", op, "outcome", outcome.String())
	case confirmationRefused:
		log.Warn("job confirmation refused; someone else may have it now",
			"operation", op, "outcome", outcome.String(), "err", err)
	case confirmationUnconfirmed:
		log.Warn("job left unconfirmed; its lease ran out and it will be redelivered",
			"operation", op, "outcome", outcome.String(), "err", err)
	default:
		log.Warn("job left unconfirmed; the worker was shutting down",
			"operation", op, "outcome", outcome.String(), "err", err)
	}
}
