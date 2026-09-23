package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
)

type processLog struct {
	Message          string `json:"msg"`
	Addr             string `json:"addr"`
	JobID            string `json:"job_id"`
	LeaseID          string `json:"lease_id"`
	Queue            string `json:"queue"`
	ActiveDeliveries int    `json:"active_deliveries"`
	RunningHandlers  int    `json:"running_handlers"`
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type workerProcess struct {
	cmd *exec.Cmd

	mu     sync.Mutex
	stdout bytes.Buffer
	logs   []processLog
	stderr lockedBuffer
	wake   chan struct{}

	waitDone chan struct{}
	waitErr  error
}

func buildWorkerProcess(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "worker")
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/worker")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build worker process:\n%s", out)
	return binary
}

func startWorkerProcess(t *testing.T, binary string, args ...string) *workerProcess {
	t.Helper()
	p := &workerProcess{
		cmd:      exec.Command(binary, args...),
		wake:     make(chan struct{}, 1),
		waitDone: make(chan struct{}),
	}
	stdout, err := p.cmd.StdoutPipe()
	require.NoError(t, err)
	p.cmd.Stderr = &p.stderr
	require.NoError(t, p.cmd.Start())

	// Cleanup is registered immediately after Start. waitDone has one owner, so
	// a failed assertion cannot leave either the process or its pipe scanner
	// behind for the next integration test.
	t.Cleanup(func() {
		select {
		case <-p.waitDone:
			return
		default:
			_ = p.cmd.Process.Kill()
		}
		select {
		case <-p.waitDone:
		case <-time.After(10 * time.Second):
			t.Errorf("worker process did not stop during cleanup\n%s", p.output())
		}
	})

	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			var event processLog
			p.mu.Lock()
			p.stdout.Write(line)
			p.stdout.WriteByte('\n')
			if json.Unmarshal(line, &event) == nil {
				p.logs = append(p.logs, event)
			}
			p.mu.Unlock()
			select {
			case p.wake <- struct{}{}:
			default:
			}
		}
	}()
	go func() {
		p.waitErr = p.cmd.Wait()
		<-scanDone
		close(p.waitDone)
	}()
	return p
}

func (p *workerProcess) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprintf("stdout:\n%s\nstderr:\n%s", p.stdout.String(), p.stderr.String())
}

func (p *workerProcess) logSnapshot() []processLog {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]processLog(nil), p.logs...)
}

func (p *workerProcess) waitForLog(t *testing.T, timeout time.Duration, match func(processLog) bool) (processLog, int) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		for i, event := range p.logSnapshot() {
			if match(event) {
				return event, i
			}
		}
		select {
		case <-p.wake:
		case <-p.waitDone:
			t.Fatalf("worker exited before expected log event: %v\n%s", p.waitErr, p.output())
		case <-timer.C:
			t.Fatalf("timed out waiting for worker log event\n%s", p.output())
		}
	}
}

func (p *workerProcess) signal(t *testing.T, signal os.Signal) {
	t.Helper()
	require.NoError(t, p.cmd.Process.Signal(signal), p.output())
}

func (p *workerProcess) wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.waitDone:
		return p.waitErr
	case <-time.After(timeout):
		t.Fatalf("worker process did not exit within %s\n%s", timeout, p.output())
		return nil
	}
}

func TestWorkerGracefulShutdownCompletesTheRunningJob(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBroker(t, st)
	producer := producerAt(t, addr)
	running, err := producer.Enqueue(ctx, client.Job{
		Queue: "sleep", Payload: json.RawMessage(`{"ms":2000}`), Priority: 10,
	})
	require.NoError(t, err)
	waiting, err := producer.Enqueue(ctx, client.Job{
		Queue: "sleep", Payload: json.RawMessage(`{"ms":10}`), Priority: -10,
	})
	require.NoError(t, err)

	p := startWorkerProcess(t, buildWorkerProcess(t),
		"-broker", addr,
		"-worker-id", "process-graceful",
		"-concurrency", "1",
		"-lease-duration", "2s",
		"-shutdown-timeout", "20s")
	_, startedIndex := p.waitForLog(t, 15*time.Second, func(event processLog) bool {
		return event.Message == "job started" && event.JobID == running.ID.String()
	})
	p.signal(t, syscall.SIGTERM)
	shutdown, shutdownIndex := p.waitForLog(t, 5*time.Second, func(event processLog) bool {
		return event.Message == "shutdown started"
	})
	require.Equal(t, 1, shutdown.RunningHandlers,
		"test setup missed the in-flight window; shutdown snapshot was %+v\n%s", shutdown, p.output())
	require.Equal(t, 1, shutdown.ActiveDeliveries)
	require.Greater(t, shutdownIndex, startedIndex)
	_, finishedIndex := p.waitForLog(t, 5*time.Second, func(event processLog) bool {
		return event.Message == "handler finished" && event.JobID == running.ID.String()
	})
	require.Less(t, shutdownIndex, finishedIndex,
		"test setup could not establish shutdown-before-handler-finish ordering\n%s", p.output())
	require.NoError(t, p.wait(t, 25*time.Second), p.output())

	runningRow, err := st.GetJob(ctx, running.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCompleted, runningRow.State)
	require.Equal(t, int32(1), runningRow.Attempts)
	waitingRow, err := st.GetJob(ctx, waiting.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateAvailable, waitingRow.State)
	require.Equal(t, int32(0), waitingRow.Attempts)

	reaped, err := st.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Zero(t, reaped)
	runningRow, err = st.GetJob(ctx, running.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCompleted, runningRow.State)
}

func TestWorkerShutdownDeadlineReportsAbandonment(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBroker(t, st)
	producer := producerAt(t, addr)
	job, err := producer.Enqueue(ctx, client.Job{
		Queue: "sleep", Payload: json.RawMessage(`{"ms":5000}`), MaxAttempts: 3,
	})
	require.NoError(t, err)

	p := startWorkerProcess(t, buildWorkerProcess(t),
		"-broker", addr,
		"-worker-id", "process-abandonment",
		"-concurrency", "1",
		"-lease-duration", "2s",
		"-shutdown-timeout", "100ms")
	p.waitForLog(t, 15*time.Second, func(event processLog) bool {
		return event.Message == "job started" && event.JobID == job.ID.String()
	})
	p.signal(t, syscall.SIGTERM)
	deadline, _ := p.waitForLog(t, 5*time.Second, func(event processLog) bool {
		return event.Message == "shutdown deadline reached"
	})
	require.Equal(t, "shutdown deadline reached", deadline.Message)
	waitErr := p.wait(t, 10*time.Second)
	require.Error(t, waitErr, p.output())
	var exitErr *exec.ExitError
	require.True(t, errors.As(waitErr, &exitErr), p.output())
	require.NotZero(t, exitErr.ExitCode(), p.output())
	require.Contains(t, p.output(), "shutdown deadline reached with 1 active deliveries")

	row, err := st.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.NotEqual(t, store.StateCompleted, row.State)
	_, err = pool.Exec(ctx, "update jobs set lease_expires = now() - interval '1 second' where id = $1", job.ID)
	require.NoError(t, err)
	reaped, err := st.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	row, err = st.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateScheduled, row.State)
	require.Equal(t, int32(1), row.Attempts,
		"reaping does not imply another worker leased the job")
}
