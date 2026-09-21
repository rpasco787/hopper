package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

// TestBrokerLifecycle runs the real cmd/broker binary and observes it only from
// outside: over gRPC, through the database, and by its exit status. It is the
// evidence that the composition root actually starts all three sweeps and that
// shutdown, with a lease stream and a health Watch still open, terminates.
//
// The binary is built with -race so a data race in the process fails the test
// through its exit code rather than passing unseen.
func TestBrokerLifecycle(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "broker")
	output, err := exec.Command("go", "build", "-race", "-o", binary, "../../cmd/broker").CombinedOutput()
	require.NoError(t, err, "%s", output)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err = pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	// Seed work for the janitor and the reaper BEFORE the process starts, so
	// nothing here calls them by hand: only the binary's own loops can act.
	st := postgres.New(pool)
	old, _, err := st.Enqueue(ctx, store.EnqueueParams{Queue: "old"})
	require.NoError(t, err)
	_, err = st.Cancel(ctx, old.ID)
	require.NoError(t, err)
	// Fixture setup only; production SQL stays in the backend.
	_, err = pool.Exec(ctx, "update jobs set finished_at = now() - interval '48 hours' where id = $1", old.ID)
	require.NoError(t, err)
	expired, _, err := st.Enqueue(ctx, store.EnqueueParams{Queue: "expired", MaxAttempts: 1})
	require.NoError(t, err)
	_, err = st.Lease(ctx, []string{"expired"}, 1, time.Millisecond, "abandoned")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var due bool
		err := pool.QueryRow(ctx, "select lease_expires < now() from jobs where id = $1", expired.ID).Scan(&due)
		return err == nil && due
	}, time.Second, 5*time.Millisecond)

	cmd := exec.Command(binary, "-dsn", pool.Config().ConnConfig.ConnString(),
		"-listen", "127.0.0.1:0", "-reaper-interval", "25ms", "-poll-interval", "10ms",
		"-retention", "24h", "-shutdown-timeout", "2s", "-log-level", "info")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	addr := make(chan string, 1)
	scanned := make(chan struct{})
	require.NoError(t, cmd.Start())
	go func() {
		defer close(scanned)
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			var entry struct {
				Message string `json:"msg"`
				Addr    string `json:"addr"`
			}
			if json.Unmarshal(scan.Bytes(), &entry) == nil && entry.Message == "broker listening" {
				select {
				case addr <- entry.Addr:
				default:
				}
			}
		}
	}()
	exited := make(chan struct{})
	var exitErr error
	go func() { exitErr = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		select {
		case <-exited:
		default:
			_ = cmd.Process.Kill()
		}
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("broker did not exit after cleanup kill")
		}
		select {
		case <-scanned:
		case <-time.After(time.Second):
			t.Error("stdout reader did not exit")
		}
	})

	var target string
	select {
	case target = <-addr:
	case <-exited:
		t.Fatalf("broker exited before ready: %v", exitErr)
	case <-ctx.Done():
		t.Fatal("broker never announced its listener")
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := hopperv1.NewHopperServiceClient(conn)

	// The janitor deleted the old cancelled row and the reaper turned the
	// exhausted expired lease into a dead one, with no help from this test.
	require.Eventually(t, func() bool {
		_, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: old.ID.String()})
		dead, deadErr := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: expired.ID.String()})
		return status.Code(err) == codes.NotFound && deadErr == nil &&
			dead.GetJob().GetState() == hopperv1.JobState_JOB_STATE_DEAD
	}, 5*time.Second, 20*time.Millisecond, "the janitor and the reaper both ran")

	// The promoter: a delayed job must become leasable on its own.
	enq, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{
		Queue: "later", RunAt: timestamppb.New(time.Now().Add(time.Second)),
	}})
	require.NoError(t, err)
	stream, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"later"}, WorkerId: "process-test", Credit: 1,
		}},
	}))
	got, err := stream.Recv() // the automatic promoter must make it leasable
	require.NoError(t, err)
	require.Equal(t, enq.GetJob().GetId(), got.GetJob().GetId())
	_, err = client.Ack(ctx, &hopperv1.AckRequest{Id: got.GetJob().GetId(), LeaseId: got.GetJob().GetLeaseId()})
	require.NoError(t, err)
	after, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: got.GetJob().GetId()})
	require.NoError(t, err)
	require.Equal(t, hopperv1.JobState_JOB_STATE_COMPLETED, after.GetJob().GetState())

	watch, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx,
		&grpc_health_v1.HealthCheckRequest{Service: "hopper.v1.HopperService"})
	require.NoError(t, err)
	_, err = watch.Recv()
	require.NoError(t, err)

	// The lease stream (now at zero credit) and the health Watch are both still
	// open. SIGTERM must end the process anyway, cleanly and inside the timeout.
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-exited:
		require.NoError(t, exitErr, "a clean shutdown exits 0, and a race in the binary would not")
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not exit after SIGTERM")
	}
}
