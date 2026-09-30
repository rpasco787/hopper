package broker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/internal/store"
)

// TestToStatus is docs/semantics.md §5 turned into wire codes. Each row is a
// promise to a client about whether retrying can possibly help.
func TestToStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want codes.Code
	}{
		{"nil", nil, codes.OK},
		{"not found", store.ErrNotFound, codes.NotFound},
		{"wrapped not found", fmt.Errorf("store: ack: %w", store.ErrNotFound), codes.NotFound},
		// Aborted, not FailedPrecondition: this is a token check that lost a
		// race, which is exactly what Aborted is for. The client must stop
		// working on the job, not fix a precondition and retry.
		{"lease lost", store.ErrLeaseLost, codes.Aborted},
		// FailedPrecondition, not Canceled: Canceled means *this RPC* was
		// cancelled by its caller, and every retry interceptor treats it that
		// way. The job being cancelled is a different fact.
		{"job cancelled", store.ErrCancelled, codes.FailedPrecondition},
		{"terminal", &store.TerminalStateError{State: store.StateDead}, codes.FailedPrecondition},
		{"context cancelled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		// Unavailable only when the store says so: the store knows the
		// driver, and a bare string that merely looks like a network error
		// could be anything.
		{"unknown", errors.New("connection refused"), codes.Internal},
		{"database unavailable", fmt.Errorf("store: ack: %w: %w", store.ErrUnavailable, errors.New("dial tcp: connection refused")), codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := toStatus(tc.err)
			require.Equal(t, tc.want, status.Code(got))
			if tc.want == codes.OK {
				require.NoError(t, got)
			}
		})
	}
}

// TestToStatusDoesNotLeakInternals: a client that sends a valid request and
// gets Internal back has learned that the broker broke, and nothing else. The
// detail belongs in the broker's logs.
func TestToStatusDoesNotLeakInternals(t *testing.T) {
	err := toStatus(fmt.Errorf("store: ack: %w",
		errors.New(`ERROR: relation "jobs" does not exist (SQLSTATE 42P01)`)))
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "SQLSTATE")
	require.NotContains(t, err.Error(), "jobs")
}

// A status built by our own validation must pass through untouched rather than
// being flattened to Internal.
func TestToStatusPassesThroughExistingStatuses(t *testing.T) {
	in := status.Error(codes.InvalidArgument, "queue is required")
	require.Equal(t, codes.InvalidArgument, status.Code(toStatus(in)))
	require.Equal(t, codes.InvalidArgument,
		status.Code(toStatus(fmt.Errorf("broker: enqueue: %w", in))))
}
