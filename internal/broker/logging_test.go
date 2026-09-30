package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

func captureJSONLogs(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})), &buf
}

func decodeJSONLogs(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	var lines []map[string]any
	for dec.More() {
		var line map[string]any
		require.NoError(t, dec.Decode(&line))
		lines = append(lines, line)
	}
	return lines
}

func logMessages(lines []map[string]any, message string) []map[string]any {
	var matches []map[string]any
	for _, line := range lines {
		if line["msg"] == message {
			matches = append(matches, line)
		}
	}
	return matches
}

func leasedJob(queue string) store.Job {
	job := sampleJob(queue)
	now := time.Now().UTC()
	leaseID := uuid.New()
	workerID := "w"
	expires := now.Add(time.Minute)
	job.State = store.StateLeased
	job.Attempts = 1
	job.LeaseID = &leaseID
	job.LeasedBy = &workerID
	job.LeasedAt = &now
	job.LeaseExpires = &expires
	return job
}

func TestFailureLogsCarryJobAndLease(t *testing.T) {
	t.Parallel()
	jobID, leaseID := uuid.New(), uuid.New()

	tests := []struct {
		name string
		call func(*Service) error
		st   func(error) *fakeStore
	}{
		{"Ack", func(s *Service) error {
			_, err := s.Ack(context.Background(), &hopperv1.AckRequest{Id: jobID.String(), LeaseId: leaseID.String()})
			return err
		}, func(err error) *fakeStore {
			return &fakeStore{ackFn: func(context.Context, uuid.UUID, uuid.UUID) error { return err }}
		}},
		{"Nack", func(s *Service) error {
			_, err := s.Nack(context.Background(), &hopperv1.NackRequest{Id: jobID.String(), LeaseId: leaseID.String()})
			return err
		}, func(err error) *fakeStore {
			return &fakeStore{nackFn: func(context.Context, uuid.UUID, uuid.UUID, string) error { return err }}
		}},
		{"Heartbeat", func(s *Service) error {
			_, err := s.Heartbeat(context.Background(), &hopperv1.HeartbeatRequest{Id: jobID.String(), LeaseId: leaseID.String()})
			return err
		}, func(err error) *fakeStore {
			return &fakeStore{extendFn: func(context.Context, uuid.UUID, uuid.UUID, time.Duration) error { return err }}
		}},
	}
	for _, tc := range tests {
		for _, failure := range []struct {
			name, message, level, code string
			err                        error
		}{
			{"internal", "rpc failed", "ERROR", "", errors.New("boom")},
			{"rejected", "rpc rejected", "DEBUG", "Aborted", store.ErrLeaseLost},
			{"unavailable", "rpc unavailable", "WARN", "", fmt.Errorf("store: %w: %w", store.ErrUnavailable, errors.New("dial tcp: connection refused"))},
		} {
			t.Run(tc.name+"/"+failure.name, func(t *testing.T) {
				logger, buf := captureJSONLogs(slog.LevelDebug)
				err := tc.call(NewService(tc.st(failure.err), Config{Logger: logger}))
				require.Error(t, err)
				lines := logMessages(decodeJSONLogs(t, buf), failure.message)
				require.Len(t, lines, 1)
				require.Equal(t, failure.level, lines[0]["level"])
				require.Equal(t, tc.name, lines[0]["rpc"])
				require.Equal(t, jobID.String(), lines[0]["job_id"])
				require.Equal(t, leaseID.String(), lines[0]["lease_id"])
				if failure.code != "" {
					require.Equal(t, failure.code, lines[0]["code"])
				}
			})
		}
	}
}

func TestCancelAndGetJobFailuresCarryJobID(t *testing.T) {
	t.Parallel()
	jobID := uuid.New()
	tests := []struct {
		name string
		call func(*Service) error
		st   func(error) *fakeStore
	}{
		{"Cancel", func(s *Service) error {
			_, err := s.Cancel(context.Background(), &hopperv1.CancelRequest{Id: jobID.String()})
			return err
		}, func(err error) *fakeStore {
			return &fakeStore{cancelFn: func(context.Context, uuid.UUID) (store.Job, error) { return store.Job{}, err }}
		}},
		{"GetJob", func(s *Service) error {
			_, err := s.GetJob(context.Background(), &hopperv1.GetJobRequest{Id: jobID.String()})
			return err
		}, func(err error) *fakeStore {
			return &fakeStore{getJobFn: func(context.Context, uuid.UUID) (store.Job, error) { return store.Job{}, err }}
		}},
	}
	for _, tc := range tests {
		for _, failure := range []struct {
			message string
			err     error
		}{
			{"rpc failed", errors.New("boom")},
			{"rpc rejected", store.ErrNotFound},
		} {
			t.Run(tc.name+"/"+failure.message, func(t *testing.T) {
				logger, buf := captureJSONLogs(slog.LevelDebug)
				require.Error(t, tc.call(NewService(tc.st(failure.err), Config{Logger: logger})))
				lines := logMessages(decodeJSONLogs(t, buf), failure.message)
				require.Len(t, lines, 1)
				require.Equal(t, jobID.String(), lines[0]["job_id"])
				require.NotContains(t, lines[0], "lease_id")
			})
		}
	}
}

func TestMalformedIDsAreRejectedQuietly(t *testing.T) {
	t.Parallel()
	valid := uuid.NewString()
	for _, tc := range []struct{ name, jobID, leaseID string }{
		{"job id", "not-a-uuid", valid},
		{"lease id", valid, "not-a-uuid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, buf := captureJSONLogs(slog.LevelDebug)
			svc := NewService(&fakeStore{}, Config{Logger: logger})
			_, err := svc.Ack(context.Background(), &hopperv1.AckRequest{Id: tc.jobID, LeaseId: tc.leaseID})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			lines := decodeJSONLogs(t, buf)
			require.Len(t, logMessages(lines, "rpc rejected"), 1)
			require.Empty(t, logMessages(lines, "rpc failed"))
		})
	}
}

func TestEnqueueBatchPartialFailureLogs(t *testing.T) {
	t.Parallel()
	logger, buf := captureJSONLogs(slog.LevelDebug)
	calls := 0
	svc := NewService(&fakeStore{enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
		defer func() { calls++ }()
		if calls == 1 {
			return store.Job{}, false, errors.New("boom")
		}
		job := sampleJob(p.Queue)
		return job, true, nil
	}}, Config{Logger: logger})
	_, err := svc.EnqueueBatch(context.Background(), &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{
		{Queue: "a"}, {Queue: "b"}, {Queue: "c"},
	}})
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, 2, calls, "the committed-prefix contract stops at the first failure")
	lines := decodeJSONLogs(t, buf)
	enqueued := logMessages(lines, "job enqueued")
	require.Len(t, enqueued, 1)
	require.Equal(t, "a", enqueued[0]["queue"])
	require.NotEmpty(t, enqueued[0]["job_id"])
	failed := logMessages(lines, "enqueue batch failed part way")
	require.Len(t, failed, 1)
	require.Equal(t, float64(1), failed[0]["index"])
	require.NotContains(t, failed[0], "job_id")
}

func TestLeaseSendFailureCarriesIDs(t *testing.T) {
	t.Parallel()
	logger, buf := captureJSONLogs(slog.LevelDebug)
	job := leasedJob("q")
	svc := NewService(&fakeStore{leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
		return []store.Job{job}, nil
	}}, Config{Logger: logger})
	stream := newFakeLeaseStream(t, func(context.Context, *hopperv1.LeaseJobsResponse) error { return errors.New("send boom") })
	stream.in <- subscribeMsg(1)
	require.Error(t, svc.LeaseJobs(stream))
	lines := logMessages(decodeJSONLogs(t, buf), "lease send failed")
	require.Len(t, lines, 1)
	require.Equal(t, job.ID.String(), lines[0]["job_id"])
	require.Equal(t, job.LeaseID.String(), lines[0]["lease_id"])
}

func TestDeliveryDebugLines(t *testing.T) {
	t.Parallel()
	logger, buf := captureJSONLogs(slog.LevelDebug)
	marker := "payload-marker-never-log"
	leased := leasedJob("q")
	leaseCalls := 0
	secondLease := make(chan struct{})
	svc := NewService(&fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			job := sampleJob(p.Queue)
			job.Payload = p.Payload
			return job, true, nil
		},
		ackFn:  func(context.Context, uuid.UUID, uuid.UUID) error { return nil },
		nackFn: func(context.Context, uuid.UUID, uuid.UUID, string) error { return nil },
		leaseFn: func(ctx context.Context, _ []string, _ int, _ time.Duration, _ string) ([]store.Job, error) {
			leaseCalls++
			if leaseCalls == 1 {
				return []store.Job{leased}, nil
			}
			close(secondLease)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, Config{Logger: logger, PollInterval: time.Millisecond})
	ctx := context.Background()
	enqueued, err := svc.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q", Payload: []byte(`{"value":"` + marker + `"}`)}})
	require.NoError(t, err)
	_, err = svc.Ack(ctx, &hopperv1.AckRequest{Id: leased.ID.String(), LeaseId: leased.LeaseID.String()})
	require.NoError(t, err)
	_, err = svc.Nack(ctx, &hopperv1.NackRequest{Id: leased.ID.String(), LeaseId: leased.LeaseID.String(), Error: "failed"})
	require.NoError(t, err)
	sent := make(chan struct{})
	stream := newFakeLeaseStream(t, func(context.Context, *hopperv1.LeaseJobsResponse) error {
		close(sent)
		return nil
	})
	stream.in <- subscribeMsg(1)
	done := make(chan error, 1)
	go func() { done <- svc.LeaseJobs(stream) }()
	waitFor(t, sent, "the leased job send")
	stream.in <- creditMsg(1)
	waitFor(t, secondLease, "the post-send lease call")
	svc.Drain()
	require.Equal(t, codes.Unavailable, status.Code(<-done))

	lines := decodeJSONLogs(t, buf)
	require.Len(t, logMessages(lines, "job enqueued"), 1)
	require.Equal(t, enqueued.GetJob().GetId(), logMessages(lines, "job enqueued")[0]["job_id"])
	require.Len(t, logMessages(lines, "job acked"), 1)
	require.Len(t, logMessages(lines, "job nacked"), 1)
	leasedLines := logMessages(lines, "job leased")
	require.Len(t, leasedLines, 1)
	require.Equal(t, leased.ID.String(), leasedLines[0]["job_id"])
	require.Equal(t, leased.LeaseID.String(), leasedLines[0]["lease_id"])
	require.Equal(t, "q", leasedLines[0]["queue"])
	require.Equal(t, float64(1), leasedLines[0]["attempt"])
	require.Equal(t, "w", leasedLines[0]["worker_id"])
	require.NotContains(t, buf.String(), marker)
}

func TestNothingNewAtInfo(t *testing.T) {
	t.Parallel()
	logger, buf := captureJSONLogs(slog.LevelInfo)
	leased := leasedJob("q")
	svc := NewService(&fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			return sampleJob(p.Queue), true, nil
		},
		ackFn: func(context.Context, uuid.UUID, uuid.UUID) error { return nil },
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return []store.Job{leased}, nil
		},
	}, Config{Logger: logger})
	ctx := context.Background()
	_, err := svc.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
	require.NoError(t, err)
	var stream *fakeLeaseStream
	stream = newFakeLeaseStream(t, func(context.Context, *hopperv1.LeaseJobsResponse) error { stream.cancel(); return nil })
	stream.in <- subscribeMsg(1)
	require.Equal(t, codes.Canceled, status.Code(svc.LeaseJobs(stream)))
	_, err = svc.Ack(ctx, &hopperv1.AckRequest{Id: leased.ID.String(), LeaseId: leased.LeaseID.String()})
	require.NoError(t, err)
	lines := decodeJSONLogs(t, buf)
	require.Len(t, lines, 2)
	require.Equal(t, "lease stream opened", lines[0]["msg"])
	require.Equal(t, "lease stream closed", lines[1]["msg"])
}
