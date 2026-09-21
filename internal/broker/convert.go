// Package broker implements the hopper.v1 gRPC service over a store.Store.
//
// Handlers validate requests, convert wire/store types and map errors.
// The package also owns gRPC registration and stream draining. SQL remains in
// internal/store/postgres, maintenance in internal/sweep, and process signals
// and pool ownership in cmd/broker. The payoff is that everything in this
// package can be tested against a fake store with no database at all.
package broker

import (
	"encoding/json"
	"math"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// stateNames is the one place the two state vocabularies meet. Both directions
// are derived from it, so a state can never be convertible one way only.
var stateNames = map[store.State]hopperv1.JobState{
	store.StateScheduled: hopperv1.JobState_JOB_STATE_SCHEDULED,
	store.StateAvailable: hopperv1.JobState_JOB_STATE_AVAILABLE,
	store.StateLeased:    hopperv1.JobState_JOB_STATE_LEASED,
	store.StateCompleted: hopperv1.JobState_JOB_STATE_COMPLETED,
	store.StateCancelled: hopperv1.JobState_JOB_STATE_CANCELLED,
	store.StateDead:      hopperv1.JobState_JOB_STATE_DEAD,
}

func stateToProto(s store.State) (hopperv1.JobState, error) {
	p, ok := stateNames[s]
	if !ok {
		// Unreachable unless a state was added to store without being added
		// here. Returning an error rather than UNSPECIFIED means the job fails
		// to serialize loudly instead of arriving stateless.
		return 0, status.Errorf(codes.Internal, "unknown job state %q", s)
	}
	return p, nil
}

func stateFromProto(p hopperv1.JobState) (store.State, error) {
	for s, v := range stateNames {
		if v == p {
			return s, nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "unknown job state %v", p)
}

// nullableTime converts a nullable Postgres timestamp. A nil *time.Time has to
// become a nil *timestamppb.Timestamp, not the epoch: timestamppb.New(zero)
// is a real instant in the year 1, which would read as "finished long ago".
func nullableTime(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func jobToProto(j store.Job) (*hopperv1.Job, error) {
	state, err := stateToProto(j.State)
	if err != nil {
		return nil, err
	}
	p := &hopperv1.Job{
		Id:             j.ID.String(),
		Queue:          j.Queue,
		Payload:        j.Payload,
		State:          state,
		Priority:       int32(j.Priority),
		RunAt:          timestamppb.New(j.RunAt),
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		LeasedBy:       j.LeasedBy,
		LeasedAt:       nullableTime(j.LeasedAt),
		LeaseExpires:   nullableTime(j.LeaseExpires),
		IdempotencyKey: j.IdempotencyKey,
		LastError:      j.LastError,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		UpdatedAt:      timestamppb.New(j.UpdatedAt),
		FinishedAt:     nullableTime(j.FinishedAt),
	}
	if j.LeaseID != nil {
		s := j.LeaseID.String()
		p.LeaseId = &s
	}
	return p, nil
}

// specToParams validates and converts. Every failure here is already a gRPC
// status, so handlers can return it unchanged — validation is the one place
// the broker knows more about what went wrong than toStatus could infer.
//
// The validation is deliberately thin: queue presence, JSON well-formedness,
// and the two range checks that exist because the wire types are wider than
// the columns. Everything else is the store's business, and duplicating its
// rules here is how the two drift apart.
func specToParams(spec *hopperv1.JobSpec) (store.EnqueueParams, error) {
	var zero store.EnqueueParams
	if spec == nil {
		return zero, status.Error(codes.InvalidArgument, "job is required")
	}
	if spec.GetQueue() == "" {
		return zero, status.Error(codes.InvalidArgument, "queue is required")
	}
	// Reject JSON syntax errors before any store call. PostgreSQL jsonb has
	// additional Unicode/numeric restrictions; those remain database errors.
	// Batch prevalidation guarantees apply only to these explicit checks.
	if len(spec.GetPayload()) > 0 && !json.Valid(spec.GetPayload()) {
		return zero, status.Error(codes.InvalidArgument, "payload must be valid JSON")
	}
	if p := spec.GetPriority(); p > math.MaxInt16 || p < math.MinInt16 {
		return zero, status.Errorf(codes.InvalidArgument,
			"priority %d is out of range (%d..%d)", p, math.MinInt16, math.MaxInt16)
	}
	if spec.GetMaxAttempts() < 0 {
		return zero, status.Errorf(codes.InvalidArgument,
			"max_attempts %d must not be negative", spec.GetMaxAttempts())
	}

	p := store.EnqueueParams{
		Queue:          spec.GetQueue(),
		Payload:        spec.GetPayload(),
		Priority:       int16(spec.GetPriority()),
		MaxAttempts:    spec.GetMaxAttempts(),
		IdempotencyKey: spec.IdempotencyKey,
	}
	// An absent run_at stays the zero time.Time, which is how Enqueue knows to
	// coalesce it to the database's now() rather than trusting our clock.
	if ts := spec.GetRunAt(); ts != nil {
		if err := ts.CheckValid(); err != nil {
			return zero, status.Error(codes.InvalidArgument, "run_at is invalid")
		}
		p.RunAt = ts.AsTime()
	}
	return p, nil
}

// parseID turns a wire id into a uuid. Field is named so the message says
// which one was wrong on a request carrying both an id and a lease_id.
func parseID(field, s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "%s is required", field)
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "%s is not a uuid: %v", field, err)
	}
	return id, nil
}
