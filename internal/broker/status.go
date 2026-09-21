package broker

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/internal/store"
)

// toStatus maps a store error to a gRPC status. It is docs/semantics.md §5 on
// the wire, and each code is a promise about whether a retry can help:
//
//	NotFound            no such job; a retry will not conjure one.
//	Aborted             the lease token lost a race. Stop working on the job.
//	FailedPrecondition  the job is in a state this call cannot act on.
//	Internal            store failure; may be permanent input rejection or ambiguous commit.
//
// Aborted for ErrLeaseLost rather than FailedPrecondition because it is
// precisely gRPC's "concurrency conflict, such as a sequencer check failure" —
// which is what a stale lease_id is. FailedPrecondition for ErrCancelled
// rather than Canceled because Canceled means the caller cancelled this RPC,
// and conflating the two would make a job outcome indistinguishable from a
// context deadline to every generic retry interceptor on the client side.
//
// A status that is already a status passes through: validation errors built by
// specToParams and parseID carry codes this function could not infer.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case status.Code(err) != codes.Unknown:
		// Includes anything wrapping a status, because status.Code unwraps via
		// the GRPCStatus() method on the chain.
		return err
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "job not found")
	case errors.Is(err, store.ErrLeaseLost):
		return status.Error(codes.Aborted, "lease lost")
	case errors.Is(err, store.ErrCancelled):
		return status.Error(codes.FailedPrecondition, "job cancelled")
	case errors.Is(err, store.ErrTerminal):
		// The state is in the message because a *TerminalStateError knows it
		// and the caller usually wants it. Richer typed details
		// (google.rpc.ErrorInfo) would need genproto; not worth a dependency
		// until something parses it.
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "cancelled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
