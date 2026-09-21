package broker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// leaseDuration resolves an optional wire duration against the broker default.
// A zero or absent value means "you choose"; a negative one is a client bug.
func (s *Service) leaseDuration(d *durationpb.Duration) (time.Duration, error) {
	if d == nil {
		return s.cfg.LeaseDuration, nil
	}
	if err := d.CheckValid(); err != nil {
		return 0, status.Error(codes.InvalidArgument, "lease_duration is invalid")
	}
	got := d.AsDuration()
	// AsDuration saturates on overflow; require an exact round trip.
	back := durationpb.New(got)
	if back.Seconds != d.Seconds || back.Nanos != d.Nanos {
		return 0, status.Error(codes.InvalidArgument, "lease_duration exceeds Go duration range")
	}
	switch {
	case got == 0:
		return s.cfg.LeaseDuration, nil
	case got < 0:
		return 0, status.Errorf(codes.InvalidArgument, "lease_duration %s must be positive", got)
	}
	return got, nil
}

// leaseKeyed parses the (id, lease_id) pair every worker-side call carries.
func leaseKeyed(id, leaseID string) (uuid.UUID, uuid.UUID, error) {
	jobID, err := parseID("id", id)
	if err != nil {
		return uuid.UUID{}, uuid.UUID{}, err
	}
	lease, err := parseID("lease_id", leaseID)
	if err != nil {
		return uuid.UUID{}, uuid.UUID{}, err
	}
	return jobID, lease, nil
}

func (s *Service) Ack(ctx context.Context, req *hopperv1.AckRequest) (*hopperv1.AckResponse, error) {
	id, leaseID, err := leaseKeyed(req.GetId(), req.GetLeaseId())
	if err != nil {
		return nil, err
	}
	if err := s.store.Ack(ctx, id, leaseID); err != nil {
		return nil, s.fail(ctx, "Ack", err)
	}
	return &hopperv1.AckResponse{}, nil
}

func (s *Service) Nack(ctx context.Context, req *hopperv1.NackRequest) (*hopperv1.NackResponse, error) {
	id, leaseID, err := leaseKeyed(req.GetId(), req.GetLeaseId())
	if err != nil {
		return nil, err
	}
	// The error string is not validated. It is a handler's message and hopper
	// only stores it; the column is text, and truncating or rejecting it here
	// would lose the one clue an operator reading the DLQ has.
	if err := s.store.Nack(ctx, id, leaseID, req.GetError()); err != nil {
		return nil, s.fail(ctx, "Nack", err)
	}
	return &hopperv1.NackResponse{}, nil
}

// Heartbeat is ExtendLease on the wire (docs/semantics.md §4). There is no
// idempotent-success case: a heartbeat that misses always means the caller no
// longer holds the job, which is why the SDK cancels the handler context on
// any error from this call.
func (s *Service) Heartbeat(ctx context.Context, req *hopperv1.HeartbeatRequest) (*hopperv1.HeartbeatResponse, error) {
	id, leaseID, err := leaseKeyed(req.GetId(), req.GetLeaseId())
	if err != nil {
		return nil, err
	}
	dur, err := s.leaseDuration(req.GetLeaseDuration())
	if err != nil {
		return nil, err
	}
	if err := s.store.ExtendLease(ctx, id, leaseID, dur); err != nil {
		return nil, s.fail(ctx, "Heartbeat", err)
	}
	return &hopperv1.HeartbeatResponse{}, nil
}

// Cancel is idempotent at this boundary although the store's Cancel is not.
//
// docs/semantics.md §10 says a second Cancel returns ErrTerminal, and the
// store is right to: it really did not cancel anything, and its job is to
// report what it saw. But a Cancel whose response was lost will be retried,
// and telling that client "failed" about a job that is in exactly the state it
// asked for is a bad API. So one terminal state — cancelled — becomes a
// success carrying already_cancelled. completed and dead still fail, because
// those are different states than the caller asked for.
func (s *Service) Cancel(ctx context.Context, req *hopperv1.CancelRequest) (*hopperv1.CancelResponse, error) {
	id, err := parseID("id", req.GetId())
	if err != nil {
		return nil, err
	}

	job, err := s.store.Cancel(ctx, id)
	if err != nil {
		var terminal *store.TerminalStateError
		if !errors.As(err, &terminal) || terminal.State != store.StateCancelled {
			return nil, s.fail(ctx, "Cancel", err)
		}
		// Already cancelled. Read the row so the response is as useful as the
		// one the first caller got. A NotFound here means retention deleted
		// the job between the two statements, which is a real NotFound.
		job, err = s.store.GetJob(ctx, id)
		if err != nil {
			return nil, s.fail(ctx, "Cancel", err)
		}
		p, err := jobToProto(job)
		if err != nil {
			return nil, s.fail(ctx, "Cancel", err)
		}
		return &hopperv1.CancelResponse{Job: p, AlreadyCancelled: true}, nil
	}

	p, err := jobToProto(job)
	if err != nil {
		return nil, s.fail(ctx, "Cancel", err)
	}
	return &hopperv1.CancelResponse{Job: p}, nil
}

func (s *Service) GetJob(ctx context.Context, req *hopperv1.GetJobRequest) (*hopperv1.GetJobResponse, error) {
	id, err := parseID("id", req.GetId())
	if err != nil {
		return nil, err
	}
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		return nil, s.fail(ctx, "GetJob", err)
	}
	p, err := jobToProto(job)
	if err != nil {
		return nil, s.fail(ctx, "GetJob", err)
	}
	return &hopperv1.GetJobResponse{Job: p}, nil
}
