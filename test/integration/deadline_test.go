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
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// brokerQueriesWaitingOnALock counts backends blocked on a lock while running
// the requested statement pattern.
func brokerQueriesWaitingOnALock(ctx context.Context, like string) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `select count(*) from pg_stat_activity
		where wait_event_type = 'Lock' and pid <> pg_backend_pid()
		  and query like $1`, like).Scan(&n)
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
		n, err := brokerQueriesWaitingOnALock(ctx, "%from jobs where id = $1%")
		return err == nil && n == 1
	}, 400*time.Millisecond, 10*time.Millisecond, "GetJob never reached PostgreSQL's lock queue")
	select {
	case err := <-result:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("GetJob did not return at its deadline")
	}
	require.Eventually(t, func() bool {
		n, err := brokerQueriesWaitingOnALock(ctx, "%from jobs where id = $1%")
		return err == nil && n == 0
	}, 5*time.Second, 50*time.Millisecond, "the broker's query is still waiting on the lock")
}

// TestClientDeadlineCancelsABatchedAck: an Ack blocked behind a table lock,
// alone in its batch, is abandoned when its caller's deadline passes. The
// batcher cancels a statement once nobody waits for it, so no backend is
// left waiting.
func TestClientDeadlineCancelsABatchedAck(t *testing.T) {
	ctx := ctxT(t)
	addr := startBrokerWithConfig(t, postgres.New(pool), broker.Config{
		LeaseDuration: 30 * time.Second, PollInterval: 10 * time.Millisecond, ConfirmBatch: 64,
	}) // truncates jobs, so it must run before the lock
	rpc := newClientAt(t, addr)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(ctx, "lock table jobs in access exclusive mode")
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, err := rpc.Ack(callCtx, &hopperv1.AckRequest{Id: uuid.NewString(), LeaseId: uuid.NewString()})
		result <- err
	}()

	require.Eventually(t, func() bool {
		n, err := brokerQueriesWaitingOnALock(ctx, "%with mine as%")
		return err == nil && n == 1
	}, 400*time.Millisecond, 10*time.Millisecond, "the batched Ack never reached PostgreSQL's lock queue")
	select {
	case err := <-result:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(5 * time.Second):
		t.Fatal("the Ack did not return at its deadline")
	}
	require.Eventually(t, func() bool {
		n, err := brokerQueriesWaitingOnALock(ctx, "%with mine as%")
		return err == nil && n == 0
	}, 5*time.Second, 50*time.Millisecond, "the batch's statement is still waiting on the lock")
}
