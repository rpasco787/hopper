package scheduler

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// barrier is the default budget for a harness wait, following the convention
// in worker/fake_test.go: long enough that a loaded machine does not trip it,
// short enough to beat go test's own timeout, so a stuck test names what it
// was waiting for instead of running out the clock.
const barrier = 10 * time.Second

// dueResponse is one scripted answer to DueSchedules.
type dueResponse struct {
	schedules []store.CronSchedule
	dbNow     time.Time
}

// advanceCall records one AdvanceSchedule invocation.
type advanceCall struct {
	name     string
	from, to time.Time
}

// fakeStore is a mutex-guarded in-memory scheduler.Store, following the
// conventions in worker/fake_test.go: nothing here calls require or FailNow
// from a non-test goroutine (Scheduler.Run drives every call from its own
// goroutine), and every accessor returns a copy of its snapshot rather than a
// reference into fake state a caller could race with.
type fakeStore struct {
	mu sync.Mutex

	// leadership
	leaderHeld     bool
	termLimit      int // 0 = unlimited terms may be won
	termsStarted   int
	tryLeaderCalls int
	tryLeaderErr   error // when set, TryLeadership always fails with this
	checkErr       error // when set, every Check fails with this
	checkFailAfter int   // when > 0, Check fails once len(dueCalls) >= this
	checks         int
	releases       int

	// events is an ordered log of "check"/"due", for tests that assert
	// interleaving (Decision 11: one DueSchedules call per Check).
	events []string

	// due schedules
	dueScript  []dueResponse // the last entry repeats once the rest are consumed
	dueCalls   []int         // limit passed on each call, in arrival order
	dueErrOnce error         // returned on the first call only, then cleared

	// advance
	advanceCalls []advanceCall
	advanceHook  func(name string, from, to time.Time) (bool, error) // default: true, nil

	// enqueue, deduped on (queue, idempotency key) like the real unique index
	enqueued   []store.EnqueueParams
	dedup      map[string]store.Job
	enqueueErr error

	// now
	now      time.Time
	nowCalls int
	nowErr   error

	// upsert / delete. existing holds the names of rows already in the table,
	// so a prune can be observed deleting the ones its keep list omits.
	upserts   []store.CronScheduleSpec
	upsertErr error
	deletes   [][]string
	existing  map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{now: time.Now(), dedup: map[string]store.Job{}, existing: map[string]bool{}}
}

func (f *fakeStore) TryLeadership(context.Context) (store.Leadership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tryLeaderCalls++
	if f.leaderHeld {
		return nil, store.ErrNotLeader
	}
	if f.tryLeaderErr != nil {
		return nil, f.tryLeaderErr
	}
	if f.termLimit > 0 && f.termsStarted >= f.termLimit {
		return nil, store.ErrNotLeader
	}
	f.leaderHeld = true
	f.termsStarted++
	return &fakeLeadership{f: f}, nil
}

func (f *fakeStore) DueSchedules(_ context.Context, limit int) ([]store.CronSchedule, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dueCalls = append(f.dueCalls, limit)
	f.events = append(f.events, "due")

	if f.dueErrOnce != nil {
		err := f.dueErrOnce
		f.dueErrOnce = nil
		return nil, time.Time{}, err
	}
	if len(f.dueScript) == 0 {
		return nil, time.Time{}, nil
	}
	resp := f.dueScript[0]
	if len(f.dueScript) > 1 {
		f.dueScript = f.dueScript[1:]
	}
	out := append([]store.CronSchedule(nil), resp.schedules...)
	return out, resp.dbNow, nil
}

func (f *fakeStore) AdvanceSchedule(_ context.Context, name string, from, to time.Time) (bool, error) {
	f.mu.Lock()
	hook := f.advanceHook
	f.advanceCalls = append(f.advanceCalls, advanceCall{name, from, to})
	f.mu.Unlock()

	if hook != nil {
		return hook(name, from, to)
	}
	return true, nil
}

func (f *fakeStore) Enqueue(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enqueued = append(f.enqueued, p)
	if f.enqueueErr != nil {
		return store.Job{}, false, f.enqueueErr
	}

	var key string
	if p.IdempotencyKey != nil {
		key = *p.IdempotencyKey
	}
	if key != "" {
		if existing, ok := f.dedup[p.Queue+"\x00"+key]; ok {
			return existing, false, nil
		}
	}

	id, err := uuid.NewV7()
	if err != nil {
		return store.Job{}, false, err
	}
	job := store.Job{
		ID: id, Queue: p.Queue, Payload: p.Payload, Priority: p.Priority,
		RunAt: p.RunAt, MaxAttempts: p.MaxAttempts, IdempotencyKey: p.IdempotencyKey,
		State: store.StateAvailable,
	}
	if key != "" {
		f.dedup[p.Queue+"\x00"+key] = job
	}
	return job, true, nil
}

func (f *fakeStore) Now(context.Context) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nowCalls++
	if f.nowErr != nil {
		return time.Time{}, f.nowErr
	}
	return f.now, nil
}

func (f *fakeStore) UpsertSchedule(_ context.Context, spec store.CronScheduleSpec) (store.CronSchedule, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upserts = append(f.upserts, spec)
	if f.upsertErr != nil {
		return store.CronSchedule{}, f.upsertErr
	}
	f.existing[spec.Name] = true
	return store.CronSchedule{
		Name: spec.Name, Queue: spec.Queue, Payload: spec.Payload,
		CronExpr: spec.CronExpr, Timezone: spec.Timezone, Enabled: spec.Enabled,
		Priority: spec.Priority, MaxAttempts: spec.MaxAttempts, NextRunAt: spec.NextRunAt,
	}, nil
}

func (f *fakeStore) DeleteSchedulesExcept(_ context.Context, keep []string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, append([]string(nil), keep...))
	kept := map[string]bool{}
	for _, name := range keep {
		kept[name] = true
	}
	removed := 0
	for name := range f.existing {
		if !kept[name] {
			delete(f.existing, name)
			removed++
		}
	}
	return removed, nil
}

// Snapshot accessors. Each returns a copy so a caller cannot race the fake's
// own goroutine (Scheduler.Run) by mutating what it reads.

func (f *fakeStore) dueCallsSnapshot() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.dueCalls...)
}

func (f *fakeStore) advanceCallsSnapshot() []advanceCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]advanceCall(nil), f.advanceCalls...)
}

func (f *fakeStore) enqueuedSnapshot() []store.EnqueueParams {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.EnqueueParams(nil), f.enqueued...)
}

func (f *fakeStore) eventsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeStore) upsertsSnapshot() []store.CronScheduleSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.CronScheduleSpec(nil), f.upserts...)
}

func (f *fakeStore) deletesSnapshot() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.deletes))
	for i, d := range f.deletes {
		out[i] = append([]string(nil), d...)
	}
	return out
}

func (f *fakeStore) existingSnapshot() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.existing))
	for k, v := range f.existing {
		out[k] = v
	}
	return out
}

func (f *fakeStore) nowCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nowCalls
}

func (f *fakeStore) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releases
}

func (f *fakeStore) termsStartedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.termsStarted
}

func (f *fakeStore) tryLeaderCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tryLeaderCalls
}

// fakeLeadership is the store.Leadership a fakeStore hands out. Check and
// Release both report back to the fakeStore they came from, so a test
// configures behavior on one shared fakeStore rather than reaching into each
// leadership instance.
type fakeLeadership struct {
	f *fakeStore

	mu       sync.Mutex
	released bool
}

func (l *fakeLeadership) Check(context.Context) error {
	l.f.mu.Lock()
	defer l.f.mu.Unlock()
	l.f.checks++
	l.f.events = append(l.f.events, "check")
	if l.f.checkErr != nil {
		return l.f.checkErr
	}
	if l.f.checkFailAfter > 0 && len(l.f.dueCalls) >= l.f.checkFailAfter {
		return store.ErrNotLeader
	}
	return nil
}

func (l *fakeLeadership) Release(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	l.released = true

	l.f.mu.Lock()
	l.f.leaderHeld = false
	l.f.releases++
	l.f.mu.Unlock()
	return nil
}

// syncBuffer is a concurrency-safe io.Writer for capturing slog output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// run is a Scheduler.Run running in the background, mirroring worker/fake_test.go's
// run/runWorker: done is close-only so cleanup and an explicit wait can both
// safely receive from it.
type run struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func runScheduler(t *testing.T, s *Scheduler) *run {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel, done: make(chan struct{})}

	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(barrier):
			t.Error("Run did not return after its context was cancelled")
		}
	})

	go func() {
		defer close(r.done)
		r.err = s.Run(ctx)
	}()
	return r
}

func (r *run) stop() { r.cancel() }

func (r *run) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(barrier):
		t.Fatal("Run did not return within the barrier")
		return nil
	}
}

func (r *run) stopAndWait(t *testing.T) error {
	t.Helper()
	r.stop()
	return r.wait(t)
}

// requireEventually is require.Eventually with this file's barrier and a
// short, consistent poll tick.
func requireEventually(t *testing.T, condition func() bool, msg string) {
	t.Helper()
	require.Eventually(t, condition, barrier, 2*time.Millisecond, msg)
}
