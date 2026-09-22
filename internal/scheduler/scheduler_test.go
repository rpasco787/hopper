package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// newTestScheduler builds a Scheduler over f with a short interval so
// polling tests converge quickly, and a logger writing into buf for tests
// that inspect log output. buf may be nil.
func newTestScheduler(f *fakeStore, buf *syncBuffer) *Scheduler {
	var logger *slog.Logger
	if buf != nil {
		logger = slog.New(slog.NewJSONHandler(buf, nil))
	} else {
		logger = slog.New(slog.NewTextHandler(devNull{}, nil))
	}
	return &Scheduler{Store: f, Interval: 5 * time.Millisecond, Logger: logger}
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

func TestFiresDueScheduleThenAdvances(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Second)
	f.now = now
	tick := now.Add(-time.Minute)
	cs := store.CronSchedule{
		Name: "nightly", Queue: "report", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, Priority: 3, MaxAttempts: 7,
		Payload: json.RawMessage(`{"x":1}`), NextRunAt: tick,
	}
	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{cs}, dbNow: now}}
	f.termLimit = 1
	f.checkFailAfter = 1 // end the term right after the one pass

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool {
		return len(f.enqueuedSnapshot()) == 1 && len(f.advanceCallsSnapshot()) == 1
	}, "expected one Enqueue and one AdvanceSchedule call")

	r.stopAndWait(t)

	enq := f.enqueuedSnapshot()
	require.Len(t, enq, 1)
	require.Equal(t, "report", enq[0].Queue)
	require.JSONEq(t, `{"x":1}`, string(enq[0].Payload))
	require.Equal(t, int16(3), enq[0].Priority)
	require.Equal(t, int32(7), enq[0].MaxAttempts)
	require.True(t, enq[0].RunAt.Equal(tick))
	require.NotNil(t, enq[0].IdempotencyKey)
	require.Equal(t, IdempotencyKey("nightly", tick), *enq[0].IdempotencyKey)

	wantSpec, err := ParseSpec("* * * * *", "UTC")
	require.NoError(t, err)
	wantNext, err := wantSpec.Next(now)
	require.NoError(t, err)

	adv := f.advanceCallsSnapshot()
	require.Len(t, adv, 1)
	require.Equal(t, "nightly", adv[0].name)
	require.True(t, adv[0].from.Equal(tick))
	require.True(t, adv[0].to.Equal(wantNext))
}

func TestDoesNothingWhileNotLeader(t *testing.T) {
	f := newFakeStore()
	f.tryLeaderErr = store.ErrNotLeader

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return f.tryLeaderCallCount() >= 3 },
		"expected several leadership retries")

	require.Empty(t, f.dueCallsSnapshot(), "a non-leader must never read due schedules")
	require.Empty(t, f.enqueuedSnapshot(), "a non-leader must never enqueue")

	err := r.stopAndWait(t)
	require.ErrorIs(t, err, context.Canceled)
}

func TestReleasesLeadershipWhenTheContextIsCancelled(t *testing.T) {
	f := newFakeStore()

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return f.termsStartedCount() >= 1 }, "expected a term to start")

	err := r.stopAndWait(t)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, f.releaseCount())
}

func TestLeadershipLossEndsTheTerm(t *testing.T) {
	f := newFakeStore()
	f.checkFailAfter = 1 // the first Check after one pass fails

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool {
		return f.releaseCount() >= 1 && f.termsStartedCount() >= 2
	}, "expected the term to end and a second term to re-contend")

	r.stopAndWait(t)

	require.Len(t, f.dueCallsSnapshot(), 1, "no further DueSchedules call must happen once leadership is lost")
}

func TestStoreErrorEndsTheTermAndRetries(t *testing.T) {
	f := newFakeStore()
	f.dueErrOnce = errors.New("boom")

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.dueCallsSnapshot()) >= 2 },
		"expected the second term to retry DueSchedules after the first term's error")

	r.stopAndWait(t)

	require.Contains(t, buf.String(), "boom", "the store error must be logged when the term ends")
	require.GreaterOrEqual(t, f.releaseCount(), 2)
}

func TestDuplicateTickIsNotReFired(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Second)
	f.now = now
	tick := now.Add(-time.Minute)
	cs := store.CronSchedule{
		Name: "dup", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: tick,
	}
	key := IdempotencyKey("dup", tick)
	existing := store.Job{Queue: "q", RunAt: tick}
	f.dedup["q\x00"+key] = existing

	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{cs}, dbNow: now}}
	f.termLimit = 1
	f.checkFailAfter = 1

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.advanceCallsSnapshot()) == 1 },
		"the schedule must still advance even though Enqueue deduplicated")

	r.stopAndWait(t)

	require.Len(t, f.enqueuedSnapshot(), 1)
	require.Contains(t, buf.String(), `"deduplicated":true`)
}

func TestStaleAdvanceIsReportedNotRetried(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Second)
	f.now = now
	tick := now.Add(-time.Minute)
	cs := store.CronSchedule{
		Name: "stale", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: tick,
	}
	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{cs}, dbNow: now}}
	f.advanceHook = func(string, time.Time, time.Time) (bool, error) { return false, nil }
	f.checkFailAfter = 3

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.advanceCallsSnapshot()) >= 3 },
		"expected the scheduler to keep retrying the advance across passes")

	err := r.stopAndWait(t)
	_ = err

	require.NotContains(t, buf.String(), `"level":"ERROR"`, "a stale advance must not end the term")
	for _, adv := range f.advanceCallsSnapshot() {
		require.True(t, adv.from.Equal(tick), "each pass must re-read the same stored tick, not trust its own copy")
	}
}

func TestBadScheduleIsSkippedAndLoggedOnce(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Second)
	f.now = now
	good := store.CronSchedule{
		Name: "good", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(-time.Minute),
	}
	bad := store.CronSchedule{
		Name: "bad", Queue: "q", CronExpr: "not-a-cron", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(-time.Minute),
	}
	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{good, bad}, dbNow: now}}
	f.checkFailAfter = 5

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.dueCallsSnapshot()) >= 5 },
		"expected five passes")

	r.stopAndWait(t)

	require.NotEmpty(t, f.enqueuedSnapshot(), "the valid schedule must still fire")
	require.Equal(t, 1, strings.Count(buf.String(), `"schedule":"bad"`),
		"the bad schedule must be logged exactly once across five passes")
}

func TestCatchUpFiresOnceAndSkipsToTheFuture(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Minute) // align to a minute boundary
	f.now = now
	staleTick := now.Add(-time.Hour)
	cs := store.CronSchedule{
		Name: "catchup", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: staleTick,
	}
	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{cs}, dbNow: now}}
	f.termLimit = 1
	f.checkFailAfter = 1

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.advanceCallsSnapshot()) == 1 }, "expected one advance")
	r.stopAndWait(t)

	wantSpec, err := ParseSpec("* * * * *", "UTC")
	require.NoError(t, err)
	wantNext, err := wantSpec.Next(now)
	require.NoError(t, err)

	adv := f.advanceCallsSnapshot()
	require.Len(t, adv, 1)
	require.True(t, adv[0].from.Equal(staleTick))
	require.True(t, adv[0].to.Equal(wantNext), "must skip to the future, not the minute after the stale tick")
	require.NotEqual(t, staleTick.Add(time.Minute), adv[0].to)

	require.Len(t, f.enqueuedSnapshot(), 1, "an hour overdue must fire once, not once per missed minute")
}

func TestNextMustBeAfterTheFiredTick(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC().Truncate(time.Second)
	f.now = now
	// A hand-edited row: next_run_at is impossibly far in the future, so the
	// real parser's Next(dbNow) cannot advance past it.
	cs := store.CronSchedule{
		Name: "broken", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(48 * time.Hour),
	}
	f.dueScript = []dueResponse{{schedules: []store.CronSchedule{cs}, dbNow: now}}
	f.termLimit = 1
	f.checkFailAfter = 1

	buf := &syncBuffer{}
	s := newTestScheduler(f, buf)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.dueCallsSnapshot()) >= 1 }, "expected at least one pass")
	// Give the (non-)fire path a moment to run before asserting silence.
	requireEventually(t, func() bool { return strings.Contains(buf.String(), `"schedule":"broken"`) },
		"expected the bad-advance case to be logged")

	r.stopAndWait(t)

	require.Empty(t, f.advanceCallsSnapshot(), "no AdvanceSchedule call for a schedule that cannot advance")
	require.Empty(t, f.enqueuedSnapshot(), "no Enqueue call for a schedule that cannot advance")
	require.Equal(t, 1, strings.Count(buf.String(), `"schedule":"broken"`))
}

func TestPassIsBoundedByBatchSize(t *testing.T) {
	f := newFakeStore()
	f.termLimit = 1
	f.checkFailAfter = 1

	s := newTestScheduler(f, nil)
	s.BatchSize = 7
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return len(f.dueCallsSnapshot()) >= 1 }, "expected a pass")
	r.stopAndWait(t)

	for _, limit := range f.dueCallsSnapshot() {
		require.Equal(t, 7, limit)
	}
}

func TestPassFiresOneBatchPerCheck(t *testing.T) {
	f := newFakeStore()
	now := time.Now().UTC()
	f.now = now
	full := make([]store.CronSchedule, 3)
	for i := range full {
		full[i] = store.CronSchedule{
			Name: string(rune('a' + i)), Queue: "q", CronExpr: "@every 1h", Timezone: "UTC",
			Enabled: true, NextRunAt: now.Add(-time.Minute),
		}
	}
	f.dueScript = []dueResponse{{schedules: full, dbNow: now}} // repeats forever
	f.termLimit = 1
	f.checkFailAfter = 2 // fail the check once two DueSchedules calls have happened

	s := newTestScheduler(f, nil)
	r := runScheduler(t, s)

	requireEventually(t, func() bool { return f.releaseCount() >= 1 }, "expected the term to end")
	r.stopAndWait(t)

	require.Equal(t, []string{"check", "due", "check", "due", "check"}, f.eventsSnapshot(),
		"a Check must never be followed by two DueSchedules calls before the next Check")
	require.Len(t, f.dueCallsSnapshot(), 2, "pass must not drain more than one batch per check")
}
