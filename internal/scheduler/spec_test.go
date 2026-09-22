package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseSpecAcceptsStandardExpressions(t *testing.T) {
	for _, expr := range []string{"0 3 * * *", "*/5 * * * *", "@hourly", "@every 1s"} {
		_, err := ParseSpec(expr, "UTC")
		require.NoErrorf(t, err, "expression %q", expr)
	}
}

// TestEveryHasAOneSecondFloor pins robfig/cron v3.0.1's actual @every
// behavior (docs/plans decision, Task 3): sub-second durations are raised to
// one second and fields below a second are truncated, not rounded, so
// "@every 1500ms" ticks once per second, not every 1.5s.
func TestEveryHasAOneSecondFloor(t *testing.T) {
	for _, expr := range []string{"@every 250ms", "@every 1500ms"} {
		s, err := ParseSpec(expr, "UTC")
		require.NoErrorf(t, err, "expression %q", expr)

		start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		first, err := s.Next(start)
		require.NoError(t, err)
		second, err := s.Next(first)
		require.NoError(t, err)
		require.Equalf(t, time.Second, second.Sub(first), "expression %q", expr)
	}
}

// TestEveryRoundsUpToTheNextSecondBoundary pins where @every occurrences
// land, not just how far apart they are: ConstantDelaySchedule.Next subtracts
// the instant's own sub-second remainder before adding the delay, which
// rounds up to the next whole-second boundary rather than adding the delay
// verbatim to the instant it was handed.
func TestEveryRoundsUpToTheNextSecondBoundary(t *testing.T) {
	s, err := ParseSpec("@every 250ms", "UTC")
	require.NoError(t, err)

	from := time.Date(2024, 1, 1, 12, 0, 0, 437_000_000, time.UTC)
	next, err := s.Next(from)
	require.NoError(t, err)
	require.Equal(t, time.Date(2024, 1, 1, 12, 0, 1, 0, time.UTC), next)
}

func TestParseSpecRejectsGarbage(t *testing.T) {
	for _, expr := range []string{"", "not a cron", "* * * *", "* * * * * *"} {
		_, err := ParseSpec(expr, "UTC")
		require.Errorf(t, err, "expression %q", expr)
		require.ErrorContainsf(t, err, expr, "error should name the offending expression %q", expr)
	}
}

func TestParseSpecRejectsUnknownTimezone(t *testing.T) {
	_, err := ParseSpec("* * * * *", "Mars/Olympus")
	require.Error(t, err)

	s, err := ParseSpec("* * * * *", "")
	require.NoError(t, err)
	require.Equal(t, time.UTC, s.loc)

	_, err = ParseSpec("* * * * *", "UTC")
	require.NoError(t, err)
	_, err = ParseSpec("* * * * *", "America/New_York")
	require.NoError(t, err)
}

func TestNextIsComputedInTheScheduleTimezone(t *testing.T) {
	s, err := ParseSpec("0 3 * * *", "America/New_York")
	require.NoError(t, err)

	from := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	next, err := s.Next(from)
	require.NoError(t, err)

	require.Equal(t, 3, next.Hour(), "must be 03:00 in the schedule's own timezone")
	require.NotEqual(t, 3, next.UTC().Hour(), "03:00 local is not 03:00 UTC")
}

func TestNextIsStrictlyAfterTheGivenInstant(t *testing.T) {
	s, err := ParseSpec("0 3 * * *", "UTC")
	require.NoError(t, err)

	occurrence := time.Date(2024, 6, 1, 3, 0, 0, 0, time.UTC)
	next, err := s.Next(occurrence)
	require.NoError(t, err)
	require.True(t, next.After(occurrence))
	require.Equal(t, time.Date(2024, 6, 2, 3, 0, 0, 0, time.UTC), next)
}

func TestNextReportsNoFutureOccurrence(t *testing.T) {
	s, err := ParseSpec("0 0 30 2 *", "UTC")
	require.NoError(t, err)

	_, err = s.Next(time.Now())
	require.Error(t, err)
}

// TestNextAcrossDaylightSavingTransitions is a characterization test of a
// third-party parser (robfig/cron v3.0.1), not a guarantee this project
// imposes. The instants below were read out of a deliberately wrong
// assertion's failure output (t.Logf lines from that run are reproduced in
// the comments) and are now pinned as exact UTC instants.
//
// Measured behavior, in wall-clock terms:
//   - The skipped wall-clock time (2023-03-12 02:30 America/New_York does
//     not exist: clocks jump 02:00 -> 03:00) is not resolved to some shifted
//     instant on the same day. The whole day is skipped, and the occurrence
//     lands on 2023-03-13 02:30 EDT instead.
//   - The repeated wall-clock hour on the fall-back date is 01:00-01:59, not
//     02:00-02:59 (clocks fall 02:00 -> 01:00), so 02:30 is not ambiguous
//     that day. It fires exactly once, at 2023-11-05 02:30 EST, the same as
//     any ordinary day, and the following occurrence is a normal 24h later.
//
// A future robfig/cron upgrade that changes these values is a signal to
// re-read the parser and update docs/semantics.md §13, not automatically a
// bug.
func TestNextAcrossDaylightSavingTransitions(t *testing.T) {
	s, err := ParseSpec("30 2 * * *", "America/New_York")
	require.NoError(t, err)

	beforeSpring := time.Date(2023, 3, 11, 12, 0, 0, 0, time.UTC)
	springNext, err := s.Next(beforeSpring)
	require.NoError(t, err)
	require.Equal(t, time.Date(2023, 3, 13, 6, 30, 0, 0, time.UTC).UnixMicro(), springNext.UnixMicro(),
		"the non-existent 2023-03-12 02:30 local is skipped entirely; the next occurrence is 2023-03-13 02:30 EDT")

	beforeFall := time.Date(2023, 11, 4, 12, 0, 0, 0, time.UTC)
	fallNext, err := s.Next(beforeFall)
	require.NoError(t, err)
	require.Equal(t, time.Date(2023, 11, 5, 7, 30, 0, 0, time.UTC).UnixMicro(), fallNext.UnixMicro(),
		"02:30 is outside the repeated 01:00-01:59 hour, so it fires once, at 2023-11-05 02:30 EST")

	fallNext2, err := s.Next(fallNext)
	require.NoError(t, err)
	require.Equal(t, time.Date(2023, 11, 6, 7, 30, 0, 0, time.UTC).UnixMicro(), fallNext2.UnixMicro(),
		"the day after the transition ticks a normal 24h later, confirming no double-fire")
}
