package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// seedSchedule upserts spec and fails the test on error, so tests that only
// need a row in place don't have to repeat the error check.
func seedSchedule(t *testing.T, ctx context.Context, s *Store, spec store.CronScheduleSpec) store.CronSchedule {
	t.Helper()
	cs, err := s.UpsertSchedule(ctx, spec)
	require.NoError(t, err)
	return cs
}

func TestUpsertScheduleInsertsAndUpdates(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	next1 := time.Now().Add(time.Hour)
	cs, err := s.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name:      "nightly-report",
		Queue:     "report",
		CronExpr:  "0 3 * * *",
		Timezone:  "UTC",
		Enabled:   true,
		NextRunAt: next1,
	})
	require.NoError(t, err)
	require.Equal(t, "nightly-report", cs.Name)
	require.Equal(t, "report", cs.Queue)
	require.JSONEq(t, `{}`, string(cs.Payload))
	require.Equal(t, int32(defaultMaxAttempts), cs.MaxAttempts)
	require.WithinDuration(t, next1, cs.NextRunAt, time.Millisecond)

	// A second upsert changing only queue must leave next_run_at untouched,
	// even though the spec carries a different NextRunAt.
	cs2, err := s.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name:      "nightly-report",
		Queue:     "report-v2",
		CronExpr:  "0 3 * * *",
		Timezone:  "UTC",
		Enabled:   true,
		NextRunAt: time.Now().Add(2 * time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, "report-v2", cs2.Queue)
	require.Equal(t, cs.NextRunAt.UnixMicro(), cs2.NextRunAt.UnixMicro(),
		"a queue-only change must not disturb next_run_at")

	// A third upsert changing cron_expr must adopt the spec's NextRunAt.
	next3 := time.Now().Add(3 * time.Hour)
	cs3, err := s.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name:      "nightly-report",
		Queue:     "report-v2",
		CronExpr:  "0 4 * * *",
		Timezone:  "UTC",
		Enabled:   true,
		NextRunAt: next3,
	})
	require.NoError(t, err)
	require.WithinDuration(t, next3, cs3.NextRunAt, time.Millisecond)

	// A timezone-only change must also adopt the spec's NextRunAt.
	next4 := time.Now().Add(4 * time.Hour)
	cs4, err := s.UpsertSchedule(ctx, store.CronScheduleSpec{
		Name:      "nightly-report",
		Queue:     "report-v2",
		CronExpr:  "0 4 * * *",
		Timezone:  "America/New_York",
		Enabled:   true,
		NextRunAt: next4,
	})
	require.NoError(t, err)
	require.WithinDuration(t, next4, cs4.NextRunAt, time.Millisecond)
}

func TestDueSchedulesSelectsOnlyDueEnabledRows(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	now := time.Now()
	due1 := seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "due-a", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(-2 * time.Minute),
	})
	due2 := seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "due-b", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(-1 * time.Minute),
	})
	seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "due-disabled", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: false, NextRunAt: now.Add(-1 * time.Minute),
	})
	seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "future", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: now.Add(time.Hour),
	})

	got, dbNow, err := s.DueSchedules(ctx, 10)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), dbNow, time.Second)
	require.Len(t, got, 2)
	require.Equal(t, due1.Name, got[0].Name)
	require.Equal(t, due2.Name, got[1].Name)
}

func TestDueSchedulesReturnsZeroClockWhenIdle(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	got, dbNow, err := s.DueSchedules(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, got)
	require.True(t, dbNow.IsZero())
}

func TestAdvanceScheduleIsGuarded(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	cs := seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "advance-me", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: time.Now().Add(time.Minute),
	})

	newNext := cs.NextRunAt.Add(time.Minute)
	ok, err := s.AdvanceSchedule(ctx, cs.Name, cs.NextRunAt, newNext)
	require.NoError(t, err)
	require.True(t, ok)

	rows, err := s.ListSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, newNext.UnixMicro(), rows[0].NextRunAt.UnixMicro())
	require.NotNil(t, rows[0].LastEnqueuedAt)
	updatedAt1 := rows[0].UpdatedAt

	// Repeating the same call with the now-stale `from` must match no row.
	ok2, err := s.AdvanceSchedule(ctx, cs.Name, cs.NextRunAt, newNext.Add(time.Minute))
	require.NoError(t, err)
	require.False(t, ok2)

	rows2, err := s.ListSchedules(ctx)
	require.NoError(t, err)
	require.Equal(t, newNext.UnixMicro(), rows2[0].NextRunAt.UnixMicro())
	require.Equal(t, updatedAt1.UnixMicro(), rows2[0].UpdatedAt.UnixMicro())
}

func TestAdvanceScheduleUnknownName(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	ok, err := s.AdvanceSchedule(ctx, "does-not-exist", time.Now(), time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDeleteSchedulesExceptPrunes(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	for _, name := range []string{"a", "b", "c"} {
		seedSchedule(t, ctx, s, store.CronScheduleSpec{
			Name: name, Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
			Enabled: true, NextRunAt: time.Now().Add(time.Hour),
		})
	}

	n, err := s.DeleteSchedulesExcept(ctx, []string{"a", "b"})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	rows, err := s.ListSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	_, err = s.DeleteSchedulesExcept(ctx, nil)
	require.Error(t, err)

	rows2, err := s.ListSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, rows2, 2, "an empty keep must delete nothing")
}

func TestScheduleTimestampsSurviveRoundTrip(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	next := time.Date(2031, 6, 15, 12, 0, 0, 123456789, time.UTC)
	cs := seedSchedule(t, ctx, s, store.CronScheduleSpec{
		Name: "precise", Queue: "q", CronExpr: "* * * * *", Timezone: "UTC",
		Enabled: true, NextRunAt: next,
	})

	// PostgreSQL's timestamptz has microsecond resolution: the stored value
	// carries no sub-microsecond remainder, unlike the Go time.Time we sent.
	require.Zero(t, cs.NextRunAt.Nanosecond()%1000,
		"the stored value must be truncated to microsecond resolution")

	// The idempotency key format (docs/plans decision 5) uses exactly six
	// fractional digits. Reading the schedule back must reproduce the same
	// key material UpsertSchedule already returned, so a caller that always
	// uses the stored value (never the value it sent) computes one stable key.
	const keyLayout = "2006-01-02T15:04:05.000000Z"
	rows, err := s.ListSchedules(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, cs.NextRunAt.UTC().Format(keyLayout), rows[0].NextRunAt.UTC().Format(keyLayout))
}

func TestNowReturnsTheDatabaseClock(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)

	n1, err := s.Now(ctx)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), n1, time.Second)

	n2, err := s.Now(ctx)
	require.NoError(t, err)
	require.False(t, n2.Before(n1))
}
