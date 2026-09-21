package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/internal/store/postgres"
	"github.com/rpasco787/hopper/internal/sweep"
	workersdk "github.com/rpasco787/hopper/worker"
)

type sdkRun struct {
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
	checked bool
}

func startSDKWorker(t *testing.T, w *workersdk.Worker) *sdkRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &sdkRun{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		r.err = w.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
			if !r.checked && r.err != nil {
				t.Errorf("worker Run returned unexpectedly: %v", r.err)
			}
		case <-time.After(10 * time.Second):
			t.Error("worker Run did not stop during cleanup")
		}
	})
	return r
}

func (r *sdkRun) wait(t *testing.T) error {
	t.Helper()
	r.checked = true
	select {
	case <-r.done:
		return r.err
	case <-time.After(10 * time.Second):
		t.Fatal("worker Run did not return")
		return nil
	}
}

func (r *sdkRun) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.wait(t)
}

func producerAt(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func enqueueBatch(t *testing.T, ctx context.Context, p *client.Client, queue string, n int, maxAttempts int32) []client.Result {
	t.Helper()
	jobs := make([]client.Job, n)
	for i := range jobs {
		jobs[i] = client.Job{Queue: queue, Payload: json.RawMessage(`{}`), MaxAttempts: maxAttempts}
	}
	results, err := p.EnqueueBatch(ctx, jobs)
	require.NoError(t, err)
	require.Len(t, results, n)
	return results
}

func storedCounts(t *testing.T, ctx context.Context, queue string) (total, completed, attemptsOne int) {
	t.Helper()
	err := pool.QueryRow(ctx, `
		select count(*),
		       count(*) filter (where state = 'completed'),
		       count(*) filter (where attempts = 1)
		from jobs where queue = $1`, queue).Scan(&total, &completed, &attemptsOne)
	require.NoError(t, err)
	return
}

func waitForCompleted(t *testing.T, ctx context.Context, queue string, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var completed int
		err := pool.QueryRow(ctx, "select count(*) from jobs where queue = $1 and state = 'completed'", queue).Scan(&completed)
		return err == nil && completed == want
	}, 20*time.Second, 10*time.Millisecond)
}

func TestWorkerDrainsQueueWithBoundedConcurrency(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBroker(t, st)
	p := producerAt(t, addr)
	results := enqueueBatch(t, ctx, p, "drain", 50, 0)

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var running, peak atomic.Int64
	w := workersdk.New(addr, workersdk.Options{
		ID: "drain-worker", Concurrency: 4, ShutdownTimeout: 5 * time.Second,
		Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("drain", func(_ context.Context, job workersdk.Job) error {
		n := running.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer running.Add(-1)
		mu.Lock()
		seen[job.ID]++
		mu.Unlock()
		return nil
	})
	r := startSDKWorker(t, w)
	waitForCompleted(t, ctx, "drain", 50)
	require.NoError(t, r.stop(t))

	mu.Lock()
	require.Len(t, seen, len(results))
	for _, result := range results {
		require.Equal(t, 1, seen[result.ID])
	}
	mu.Unlock()
	total, completed, attemptsOne := storedCounts(t, ctx, "drain")
	require.Equal(t, 50, total)
	require.Equal(t, 50, completed)
	require.Equal(t, 50, attemptsOne)
	require.LessOrEqual(t, peak.Load(), int64(4))
}

func TestWorkerLongHandlerRenewsBrokerSelectedLease(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBrokerWithConfig(t, st, broker.Config{
		LeaseDuration: 300 * time.Millisecond,
		PollInterval:  5 * time.Millisecond,
	})
	reaper := sweep.Reaper(st)
	reaper.Interval = 20 * time.Millisecond
	startMaintenance(t, reaper)
	p := producerAt(t, addr)
	result := enqueueBatch(t, ctx, p, "long", 1, 0)[0]

	w := workersdk.New(addr, workersdk.Options{
		ID: "long-worker", Concurrency: 1, ShutdownTimeout: 3 * time.Second,
		Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("long", func(ctx context.Context, _ workersdk.Job) error {
		timer := time.NewTimer(1200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	r := startSDKWorker(t, w)
	waitForCompleted(t, ctx, "long", 1)
	require.NoError(t, r.stop(t))
	row, err := st.GetJob(ctx, result.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCompleted, row.State)
	require.Equal(t, int32(1), row.Attempts)
}

func TestWorkerFailurePolicyPersistsSanitizedErrorsAndPanic(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond})
	addr := startBrokerWithConfig(t, st, broker.Config{LeaseDuration: time.Second, PollInterval: 5 * time.Millisecond})
	promoter := sweep.Promoter(st)
	promoter.Interval = 5 * time.Millisecond
	startMaintenance(t, promoter)
	p := producerAt(t, addr)
	errResult, err := p.Enqueue(ctx, client.Job{
		Queue: "failure", Payload: json.RawMessage(`{"kind":"error"}`), MaxAttempts: 3,
	})
	require.NoError(t, err)
	panicResult, err := p.Enqueue(ctx, client.Job{
		Queue: "failure", Payload: json.RawMessage(`{"kind":"panic"}`), MaxAttempts: 1,
	})
	require.NoError(t, err)

	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 2, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("failure", func(_ context.Context, job workersdk.Job) error {
		var payload struct {
			Kind string `json:"kind"`
		}
		if err := job.Unmarshal(&payload); err != nil {
			return err
		}
		if payload.Kind == "panic" {
			panic("integration kaboom")
		}
		return errors.New("bad\x00\xffé界")
	})
	r := startSDKWorker(t, w)
	require.Eventually(t, func() bool {
		var dead int
		err := pool.QueryRow(ctx, "select count(*) from jobs where queue = 'failure' and state = 'dead'").Scan(&dead)
		return err == nil && dead == 2
	}, 20*time.Second, 10*time.Millisecond)
	require.NoError(t, r.stop(t))

	errorRow, err := st.GetJob(ctx, errResult.ID)
	require.NoError(t, err)
	require.Equal(t, int32(3), errorRow.Attempts)
	require.NotNil(t, errorRow.LastError)
	require.NotContains(t, *errorRow.LastError, "\x00")
	require.True(t, utf8.ValidString(*errorRow.LastError))
	require.Contains(t, *errorRow.LastError, "�é界")

	panicRow, err := st.GetJob(ctx, panicResult.ID)
	require.NoError(t, err)
	require.Equal(t, int32(1), panicRow.Attempts)
	require.NotNil(t, panicRow.LastError)
	require.Contains(t, *panicRow.LastError, "handler panicked: integration kaboom")
	require.Contains(t, *panicRow.LastError, "runtime/debug.Stack")
}

func TestTwoWorkersCompleteEveryJobOnce(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBrokerWithConfig(t, st, broker.Config{LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond})
	p := producerAt(t, addr)
	results := enqueueBatch(t, ctx, p, "shared", 200, 0)

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	handler := func(_ context.Context, job workersdk.Job) error {
		mu.Lock()
		seen[job.ID]++
		mu.Unlock()
		return nil
	}
	runs := make([]*sdkRun, 0, 2)
	for _, id := range []string{"worker-a", "worker-b"} {
		w := workersdk.New(addr, workersdk.Options{
			ID: id, Concurrency: 4, ShutdownTimeout: 5 * time.Second,
			Logger: slog.New(slog.DiscardHandler),
		})
		w.Handle("shared", handler)
		runs = append(runs, startSDKWorker(t, w))
	}
	waitForCompleted(t, ctx, "shared", 200)
	for _, r := range runs {
		require.NoError(t, r.stop(t))
	}
	mu.Lock()
	require.Len(t, seen, len(results))
	for _, result := range results {
		require.Equal(t, 1, seen[result.ID])
	}
	mu.Unlock()
	_, completed, attemptsOne := storedCounts(t, ctx, "shared")
	require.Equal(t, 200, completed)
	require.Equal(t, 200, attemptsOne)
}

func TestCancelStopsRunningWorkerHandler(t *testing.T) {
	ctx := ctxT(t)
	st := postgres.New(pool)
	addr := startBrokerWithConfig(t, st, broker.Config{
		LeaseDuration: 300 * time.Millisecond, PollInterval: 5 * time.Millisecond,
	})
	p := producerAt(t, addr)
	result := enqueueBatch(t, ctx, p, "cancel", 1, 0)[0]
	started := make(chan struct{}, 1)
	cause := make(chan error, 1)
	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("cancel", func(hctx context.Context, _ workersdk.Job) error {
		started <- struct{}{}
		<-hctx.Done()
		cause <- context.Cause(hctx)
		return hctx.Err()
	})
	r := startSDKWorker(t, w)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler did not start")
	}
	_, err := p.Cancel(ctx, result.ID)
	require.NoError(t, err)
	select {
	case got := <-cause:
		require.ErrorIs(t, got, workersdk.ErrJobCancelled)
	case <-ctx.Done():
		t.Fatal("cancelled job did not stop its handler")
	}
	require.Eventually(t, func() bool {
		row, err := st.GetJob(ctx, result.ID)
		return err == nil && row.State == store.StateCancelled
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, r.stop(t))
}

func TestHopperctlBatchesAndStableKeyRerun(t *testing.T) {
	ctx := ctxT(t)
	addr := startBroker(t, postgres.New(pool))
	binary := filepath.Join(t.TempDir(), "hopperctl")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/hopperctl")
	build.Dir = filepath.Join("..", "..")
	buildOutput, err := build.CombinedOutput()
	require.NoError(t, err, string(buildOutput))

	run := func() string {
		commandCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(commandCtx, binary, "enqueue",
			"-broker", addr, "-queue", "cli", "-n", "5", "-batch", "2", "-key-prefix", "cli-stable")
		env := make([]string, 0, len(os.Environ()))
		for _, item := range os.Environ() {
			if !strings.HasPrefix(item, "HOPPER_") {
				env = append(env, item)
			}
		}
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return string(out)
	}
	first := run()
	require.Contains(t, first, "acknowledged=5 deduplicated=0")
	second := run()
	require.Contains(t, second, "acknowledged=5 deduplicated=5")
	require.Equal(t, 5, jobCount(t, ctx))
}

type gatedRenewStore struct {
	store.Store
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (s *gatedRenewStore) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, duration time.Duration) error {
	if s.calls.Add(1) == 1 {
		return s.Store.ExtendLease(ctx, id, leaseID, duration)
	}
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return s.Store.ExtendLease(ctx, id, leaseID, duration)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerLeaseLossCancelsHandler(t *testing.T) {
	ctx := ctxT(t)
	base := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: 5 * time.Second, Cap: 5 * time.Second})
	gated := &gatedRenewStore{Store: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gated.release) }) }
	t.Cleanup(release)
	addr := startBrokerWithConfig(t, gated, broker.Config{
		LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond,
	})
	p := producerAt(t, addr)
	result := enqueueBatch(t, ctx, p, "lease-loss", 1, 0)[0]
	started := make(chan struct{}, 1)
	cause := make(chan error, 1)
	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("lease-loss", func(hctx context.Context, _ workersdk.Job) error {
		started <- struct{}{}
		<-hctx.Done()
		cause <- context.Cause(hctx)
		return hctx.Err()
	})
	r := startSDKWorker(t, w)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("handler never started")
	}
	select {
	case <-gated.entered:
	case <-ctx.Done():
		t.Fatal("periodic renewal never reached the gate")
	}
	_, err := pool.Exec(ctx, "update jobs set lease_expires = now() - interval '1 second' where id = $1", result.ID)
	require.NoError(t, err)
	reaped, err := base.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	release()
	select {
	case got := <-cause:
		require.ErrorIs(t, got, workersdk.ErrLeaseLost)
	case <-ctx.Done():
		t.Fatal("lease loss did not cancel the handler")
	}
	require.NoError(t, r.stop(t))
	row, err := base.GetJob(ctx, result.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateScheduled, row.State)
}

type delayedLeaseStore struct {
	store.Store
	delayed  atomic.Bool
	leased   chan store.Job
	release  chan struct{}
	extended chan error
}

func (s *delayedLeaseStore) Lease(ctx context.Context, queues []string, n int, duration time.Duration, workerID string) ([]store.Job, error) {
	jobs, err := s.Store.Lease(ctx, queues, n, duration, workerID)
	if err != nil || len(jobs) == 0 || !s.delayed.CompareAndSwap(false, true) {
		return jobs, err
	}
	s.leased <- jobs[0]
	select {
	case <-s.release:
		return jobs, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *delayedLeaseStore) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, duration time.Duration) error {
	err := s.Store.ExtendLease(ctx, id, leaseID, duration)
	select {
	case s.extended <- err:
	default:
	}
	return err
}

func TestWorkerInitialValidationRejectsDelayedStaleDelivery(t *testing.T) {
	ctx := ctxT(t)
	base := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond})
	delayed := &delayedLeaseStore{
		Store: base, leased: make(chan store.Job, 1), release: make(chan struct{}), extended: make(chan error, 1),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(delayed.release) }) }
	t.Cleanup(release)
	addr := startBrokerWithConfig(t, delayed, broker.Config{
		LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond,
	})
	p := producerAt(t, addr)
	result := enqueueBatch(t, ctx, p, "stale", 1, 0)[0]
	ran := make(chan struct{}, 1)
	w := workersdk.New(addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("stale", func(context.Context, workersdk.Job) error { ran <- struct{}{}; return nil })
	r := startSDKWorker(t, w)

	var old store.Job
	select {
	case old = <-delayed.leased:
	case <-ctx.Done():
		t.Fatal("broker never leased the delayed delivery")
	}
	require.Equal(t, result.ID, old.ID)
	_, err := pool.Exec(ctx, "update jobs set lease_expires = now() - interval '1 second' where id = $1", result.ID)
	require.NoError(t, err)
	reaped, err := base.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.Eventually(t, func() bool {
		n, err := base.PromoteDue(ctx, 10)
		return err == nil && n == 1
	}, 3*time.Second, 5*time.Millisecond)
	newOwner, err := base.Lease(ctx, []string{"stale"}, 1, 2*time.Second, "new-owner")
	require.NoError(t, err)
	require.Len(t, newOwner, 1)
	require.NotEqual(t, *old.LeaseID, *newOwner[0].LeaseID)
	require.NoError(t, base.Ack(ctx, newOwner[0].ID, *newOwner[0].LeaseID))

	release()
	select {
	case err := <-delayed.extended:
		require.ErrorIs(t, err, store.ErrLeaseLost)
	case <-ctx.Done():
		t.Fatal("worker never validated the stale delivery")
	}
	require.Empty(t, ran)
	require.NoError(t, r.stop(t))
	row, err := base.GetJob(ctx, result.ID)
	require.NoError(t, err)
	require.Equal(t, store.StateCompleted, row.State)
}

type ambiguousAckStore struct {
	store.Store
	mu     sync.Mutex
	tokens []uuid.UUID
	first  bool
}

func (s *ambiguousAckStore) Ack(ctx context.Context, id, leaseID uuid.UUID) error {
	s.mu.Lock()
	s.tokens = append(s.tokens, leaseID)
	first := !s.first
	if first {
		s.first = true
	}
	s.mu.Unlock()
	err := s.Store.Ack(ctx, id, leaseID)
	if err == nil && first {
		return status.Error(codes.Unavailable, "ack response lost")
	}
	return err
}

func (s *ambiguousAckStore) snapshot() []uuid.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uuid.UUID(nil), s.tokens...)
}

type ambiguousNackStore struct {
	store.Store
	mu      sync.Mutex
	tokens  []uuid.UUID
	first   bool
	applied chan struct{}
	release chan struct{}
}

func (s *ambiguousNackStore) Nack(ctx context.Context, id, leaseID uuid.UUID, message string) error {
	s.mu.Lock()
	s.tokens = append(s.tokens, leaseID)
	first := !s.first
	if first {
		s.first = true
	}
	s.mu.Unlock()
	err := s.Store.Nack(ctx, id, leaseID, message)
	if err != nil || !first {
		return err
	}
	close(s.applied)
	select {
	case <-s.release:
		return status.Error(codes.Unavailable, "nack response lost")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ambiguousNackStore) snapshot() []uuid.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uuid.UUID(nil), s.tokens...)
}

func TestWorkerConfirmationAmbiguityReusesLeaseToken(t *testing.T) {
	t.Run("ack", func(t *testing.T) {
		ctx := ctxT(t)
		base := postgres.New(pool)
		wrapped := &ambiguousAckStore{Store: base}
		addr := startBrokerWithConfig(t, wrapped, broker.Config{LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond})
		p := producerAt(t, addr)
		enqueueBatch(t, ctx, p, "ambiguous-ack", 1, 0)
		var handled atomic.Int64
		w := workersdk.New(addr, workersdk.Options{Concurrency: 1, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler)})
		w.Handle("ambiguous-ack", func(context.Context, workersdk.Job) error { handled.Add(1); return nil })
		r := startSDKWorker(t, w)
		waitForCompleted(t, ctx, "ambiguous-ack", 1)
		require.NoError(t, r.stop(t))
		tokens := wrapped.snapshot()
		require.GreaterOrEqual(t, len(tokens), 2)
		for _, token := range tokens[1:] {
			require.Equal(t, tokens[0], token)
		}
		require.Equal(t, int64(1), handled.Load())
	})

	t.Run("nack promoted before retry", func(t *testing.T) {
		ctx := ctxT(t)
		base := postgres.New(pool).WithRetryPolicy(store.RetryPolicy{Base: time.Millisecond, Cap: time.Millisecond})
		wrapped := &ambiguousNackStore{
			Store: base, applied: make(chan struct{}), release: make(chan struct{}),
		}
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(wrapped.release) }) }
		t.Cleanup(release)
		addr := startBrokerWithConfig(t, wrapped, broker.Config{LeaseDuration: 2 * time.Second, PollInterval: 5 * time.Millisecond})
		p := producerAt(t, addr)
		result := enqueueBatch(t, ctx, p, "ambiguous-nack", 1, 5)[0]
		var handled atomic.Int64
		w := workersdk.New(addr, workersdk.Options{Concurrency: 1, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.DiscardHandler)})
		w.Handle("ambiguous-nack", func(context.Context, workersdk.Job) error {
			handled.Add(1)
			return errors.New("retry me")
		})
		r := startSDKWorker(t, w)
		select {
		case <-wrapped.applied:
		case <-ctx.Done():
			t.Fatal("first Nack was never applied")
		}
		require.Eventually(t, func() bool {
			n, err := base.PromoteDue(ctx, 10)
			return err == nil && n == 1
		}, 3*time.Second, 5*time.Millisecond)
		r.cancel()
		release()
		require.NoError(t, r.wait(t))
		tokens := wrapped.snapshot()
		require.GreaterOrEqual(t, len(tokens), 2)
		for _, token := range tokens[1:] {
			require.Equal(t, tokens[0], token)
		}
		require.Equal(t, int64(1), handled.Load())
		row, err := base.GetJob(ctx, result.ID)
		require.NoError(t, err)
		require.Equal(t, store.StateAvailable, row.State)
	})
}

type liveBroker struct {
	mu   sync.Mutex
	srv  *broker.Server
	done chan error
	addr string
}

func serveLiveBroker(t *testing.T, addr string, st store.Store, cfg broker.Config) *liveBroker {
	t.Helper()
	var lis net.Listener
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for {
		lis, err = net.Listen("tcp", addr)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, err)
	b := &liveBroker{srv: broker.NewServer(st, cfg), done: make(chan error, 1), addr: lis.Addr().String()}
	go func() { b.done <- b.srv.Serve(lis) }()
	t.Cleanup(func() { require.NoError(t, b.stop()) })
	return b
}

func (b *liveBroker) stop() error {
	b.mu.Lock()
	if b.srv == nil {
		b.mu.Unlock()
		return nil
	}
	srv := b.srv
	b.srv = nil
	b.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return err
	}
	select {
	case err := <-b.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestWorkerReconnectsAcrossRealBrokerRestart(t *testing.T) {
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)
	st := postgres.New(pool)
	cfg := broker.Config{LeaseDuration: 10 * time.Second, PollInterval: 5 * time.Millisecond}
	firstBroker := serveLiveBroker(t, "127.0.0.1:0", st, cfg)
	p := producerAt(t, firstBroker.addr)
	first, err := p.Enqueue(ctx, client.Job{Queue: "restart", Priority: 10})
	require.NoError(t, err)
	_, err = p.Enqueue(ctx, client.Job{Queue: "restart", Priority: 0})
	require.NoError(t, err)

	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	t.Cleanup(release)
	w := workersdk.New(firstBroker.addr, workersdk.Options{
		Concurrency: 1, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.DiscardHandler),
	})
	w.Handle("restart", func(_ context.Context, job workersdk.Job) error {
		if job.ID == first.ID {
			firstStarted <- struct{}{}
			<-releaseFirst
		}
		return nil
	})
	r := startSDKWorker(t, w)
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first handler never started")
	}
	require.NoError(t, firstBroker.stop())
	secondBroker := serveLiveBroker(t, firstBroker.addr, st, cfg)
	require.Equal(t, firstBroker.addr, secondBroker.addr)
	var waitingState string
	var waitingAttempts int32
	require.NoError(t, pool.QueryRow(ctx,
		"select state, attempts from jobs where queue = 'restart' and id <> $1", first.ID,
	).Scan(&waitingState, &waitingAttempts))
	require.Equal(t, string(store.StateAvailable), waitingState,
		"the replacement stream has no credit while the old handler owns capacity")
	require.Equal(t, int32(0), waitingAttempts)
	release()
	waitForCompleted(t, ctx, "restart", 2)
	require.NoError(t, r.stop(t))
	_, completed, attemptsOne := storedCounts(t, ctx, "restart")
	require.Equal(t, 2, completed)
	require.Equal(t, 2, attemptsOne)
}
