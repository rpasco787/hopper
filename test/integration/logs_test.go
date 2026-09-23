package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/scheduler"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/sweep"
	workersdk "github.com/rpasco787/hopper/worker"
)

func TestJobLogsCarryIDsAndCorrelate(t *testing.T) {
	ctx := ctxT(t)
	brokerBuf, workerBuf := &lockedBuffer{}, &lockedBuffer{}
	brokerLogger := slog.New(slog.NewJSONHandler(brokerBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	workerLogger := slog.New(slog.NewJSONHandler(workerBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	st := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond})
	addr := startBrokerWithConfig(t, st, broker.Config{
		LeaseDuration: time.Second, PollInterval: 5 * time.Millisecond, Logger: brokerLogger,
	})
	promoter := sweep.Promoter(st)
	promoter.Interval = 5 * time.Millisecond
	startMaintenance(t, promoter)

	w := workersdk.New(addr, workersdk.Options{
		ID: "log-worker", Concurrency: 4, ShutdownTimeout: 5 * time.Second, Logger: workerLogger,
	})
	w.Handle("logq", func(_ context.Context, job workersdk.Job) error {
		var payload struct {
			Fail bool `json:"fail"`
		}
		if err := job.Unmarshal(&payload); err != nil {
			return err
		}
		if payload.Fail && job.Attempt == 1 {
			return errors.New("fail once")
		}
		return nil
	})
	workerRun := startSDKWorker(t, w)
	producer := producerAt(t, addr)
	const payloadMarker = "private-payload-marker-7f4b"
	handled := make(map[string]struct{})
	for i := range 10 {
		payload := json.RawMessage(`{"marker":"` + payloadMarker + `","fail":` + boolJSON(i < 3) + `}`)
		result, err := producer.Enqueue(ctx, client.Job{Queue: "logq", Payload: payload, MaxAttempts: 2})
		require.NoError(t, err)
		handled[result.ID.String()] = struct{}{}
	}
	waitForCompleted(t, ctx, "logq", 10)

	raw := newClientAt(t, addr)
	staleJob, staleLease := uuid.NewString(), uuid.NewString()
	_, err := raw.Ack(ctx, &hopperv1.AckRequest{Id: staleJob, LeaseId: staleLease})
	require.Equal(t, codes.NotFound, status.Code(err))
	for _, call := range []func() error{
		func() error {
			_, err := raw.GetJob(ctx, &hopperv1.GetJobRequest{Id: uuid.NewString()})
			return err
		},
		func() error {
			_, err := raw.Cancel(ctx, &hopperv1.CancelRequest{Id: uuid.NewString()})
			return err
		},
	} {
		require.Equal(t, codes.NotFound, status.Code(call()))
	}
	_, err = raw.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{
		{Queue: "batch-log"},
		{Queue: ""},
	}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	dbNow, err := st.Now(ctx)
	require.NoError(t, err)
	_, err = st.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name: "log-schedule", Queue: "cron-log", Payload: json.RawMessage(`{"safe":true}`),
		CronExpr: "@every 1s", Timezone: "UTC", Enabled: true, NextRunAt: dbNow.Add(-time.Second),
	})
	require.NoError(t, err)
	schedulerCtx, stopScheduler := context.WithCancel(context.Background())
	schedulerDone := make(chan error, 1)
	go func() {
		schedulerDone <- (&scheduler.Scheduler{Store: st, Interval: 5 * time.Millisecond, Logger: brokerLogger}).Run(schedulerCtx)
	}()
	require.Eventually(t, func() bool {
		return logMessageCount(decodeProcessLogs(t, brokerBuf.String()), "scheduler tick fired") > 0
	}, 5*time.Second, 10*time.Millisecond)
	stopScheduler()
	require.ErrorIs(t, <-schedulerDone, context.Canceled)
	require.NoError(t, workerRun.stop(t))

	brokerLines := decodeProcessLogs(t, brokerBuf.String())
	workerLines := decodeProcessLogs(t, workerBuf.String())
	all := append(append([]map[string]any(nil), brokerLines...), workerLines...)
	// Decision 9's two scopes. RPC messages are classified by operation because
	// the same message is intentionally used for both delivery- and job-scoped RPCs.
	deliveryScoped := map[string]bool{
		"job leased": true, "job acked": true, "job nacked": true,
		"lease send failed": true, "job started": true, "handler finished": true,
		"job confirmed": true, "nacking an unstarted job": true,
		"job confirmation refused; someone else may have it now":             true,
		"job left unconfirmed; its lease ran out and it will be redelivered": true,
		"job left unconfirmed; the worker was shutting down":                 true,
		"retrying a confirmation":                                            true, "heartbeat rejected; cancelling the handler": true,
		"lease expired locally; cancelling the handler":                  true,
		"lease expired while applying a renewal; cancelling the handler": true,
		"discarding a malformed delivery":                                true,
		"lease not confirmed; leaving the job to be redelivered":         true,
		"handler returned after the shutdown deadline; not confirming":   true,
	}
	jobScoped := map[string]bool{"job enqueued": true, "scheduler tick fired": true}
	seenDelivery, seenJob := map[string]bool{}, map[string]bool{}
	leaseIDs := map[string]bool{staleLease: true}
	for _, line := range all {
		msg, _ := line["msg"].(string)
		jobID, hasJob := line["job_id"].(string)
		leaseID, hasLease := line["lease_id"].(string)
		if hasLease && leaseID != "" {
			leaseIDs[leaseID] = true
		}
		if msg == "rpc failed" || msg == "rpc rejected" {
			rpc, _ := line["rpc"].(string)
			switch rpc {
			case "Ack", "Nack", "Heartbeat":
				require.NotEmpty(t, jobID, line)
				require.NotEmpty(t, leaseID, line)
				seenDelivery[msg+"/"+rpc] = true
			case "Cancel", "GetJob":
				require.NotEmpty(t, jobID, line)
				seenJob[msg+"/"+rpc] = true
			default:
				if hasJob {
					t.Fatalf("job-scoped RPC log has unknown operation: %#v", line)
				}
			}
			continue
		}
		switch {
		case deliveryScoped[msg]:
			require.NotEmpty(t, jobID, line)
			require.NotEmpty(t, leaseID, line)
			seenDelivery[msg] = true
		case jobScoped[msg]:
			require.NotEmpty(t, jobID, line)
			seenJob[msg] = true
		case hasJob:
			t.Fatalf("job-bearing log line belongs to neither scope: %#v", line)
		}
	}
	for _, message := range []string{"job leased", "job acked", "job nacked", "job started", "handler finished", "job confirmed", "rpc rejected/Ack"} {
		require.True(t, seenDelivery[message], message)
	}
	for _, message := range []string{"job enqueued", "scheduler tick fired", "rpc rejected/GetJob", "rpc rejected/Cancel"} {
		require.True(t, seenJob[message], message)
	}
	require.Equal(t, 1, logMessageCount(brokerLines, "enqueue batch rejected"))

	brokerLeases := leasesByJob(brokerLines, "job leased", handled)
	workerStarts := leasesByJob(workerLines, "job started", handled)
	require.Equal(t, brokerLeases, workerStarts)
	brokerAcks := leasesByJob(brokerLines, "job acked", handled)
	workerConfirmed := leasesByJobOperation(workerLines, "job confirmed", "ack", handled)
	require.Equal(t, brokerAcks, workerConfirmed)

	require.NotContains(t, brokerBuf.String(), payloadMarker)
	require.NotContains(t, workerBuf.String(), payloadMarker)
	for _, line := range all {
		copyLine := make(map[string]any, len(line))
		for key, value := range line {
			if key != "lease_id" {
				copyLine[key] = value
			}
		}
		withoutLeaseField, err := json.Marshal(copyLine)
		require.NoError(t, err)
		for leaseID := range leaseIDs {
			require.NotContains(t, string(withoutLeaseField), leaseID, line)
		}
	}
}

func boolJSON(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

func decodeProcessLogs(t *testing.T, data string) []map[string]any {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(data))
	var lines []map[string]any
	for scanner.Scan() {
		line := make(map[string]any)
		require.NoError(t, json.Unmarshal(bytes.Clone(scanner.Bytes()), &line))
		lines = append(lines, line)
	}
	require.NoError(t, scanner.Err())
	return lines
}

func logMessageCount(lines []map[string]any, message string) int {
	count := 0
	for _, line := range lines {
		if line["msg"] == message {
			count++
		}
	}
	return count
}

func leasesByJob(lines []map[string]any, message string, jobs map[string]struct{}) map[string]map[string]bool {
	return leasesByJobOperation(lines, message, "", jobs)
}

func leasesByJobOperation(lines []map[string]any, message, operation string, jobs map[string]struct{}) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(jobs))
	for jobID := range jobs {
		out[jobID] = map[string]bool{}
	}
	for _, line := range lines {
		if line["msg"] != message {
			continue
		}
		if operation != "" && line["operation"] != operation {
			continue
		}
		jobID, _ := line["job_id"].(string)
		if _, ok := jobs[jobID]; !ok {
			continue
		}
		leaseID, _ := line["lease_id"].(string)
		out[jobID][leaseID] = true
	}
	return out
}
