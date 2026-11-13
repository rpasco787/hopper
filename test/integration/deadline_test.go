package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// brokerQueriesWaitingOnALock counts backends blocked on a lock while running
// GetJob's statement.
func brokerQueriesWaitingOnALock(ctx context.Context) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `select count(*) from pg_stat_activity
		where wait_event_type = 'Lock' and pid <> pg_backend_pid()
		  and query like '%from jobs where id = $1%'`).Scan(&n)
	return n, err
}

// TestClientDeadlineCancelsTheQuery: a GetJob blocked behind a table lock is
// abandoned when its caller's deadline passes. The broker hands the RPC's
// context to pgx, which cancels the statement, so no backend is left waiting.
func TestClientDeadlineCancelsTheQuery(t *testing.T) {
	ctx := ctxT(t)
	rpc := newClient(t) // truncates jobs, so it must run before the lock

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, "lock table jobs in access exclusive mode")
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, err := rpc.GetJob(callCtx, &hopperv1.GetJobRequest{Id: uuid.NewString()})
		result <- err
	}()

	require.Eventually(t, func() bool {
		n, err := brokerQueriesWaitingOnALock(ctx)
		return err == nil && n == 1
	}, 400*time.Millisecond, 10*time.Millisecond, "GetJob never reached PostgreSQL's lock queue")
	select {
	case err := <-result:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("GetJob did not return at its deadline")
	}
	require.Eventually(t, func() bool {
		n, err := brokerQueriesWaitingOnALock(ctx)
		return err == nil && n == 0
	}, 5*time.Second, 50*time.Millisecond, "the broker's query is still waiting on the lock")
}
