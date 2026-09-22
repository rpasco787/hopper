// Package scheduler runs hopper's cron scheduler: election by advisory lock,
// reading due schedules, enqueueing jobs and advancing next_run_at. See
// docs/semantics.md §13.
package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Spec is a parsed cron expression bound to a timezone.
type Spec struct {
	sched cron.Schedule
	loc   *time.Location
	expr  string
}

// ParseSpec parses a standard five-field cron expression, a @descriptor or an
// @every duration, in the named IANA timezone. An empty timezone is UTC.
//
// time.LoadLocation reads the system zoneinfo database, which the distroless
// runtime image does not have; cmd/broker imports time/tzdata to embed it.
func ParseSpec(expr, tz string) (Spec, error) {
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Spec{}, fmt.Errorf("scheduler: parse spec: unknown timezone %q: %w", tz, err)
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return Spec{}, fmt.Errorf("scheduler: parse spec: expression %q: %w", expr, err)
	}
	return Spec{sched: sched, loc: loc, expr: expr}, nil
}

// Next returns the first occurrence strictly after t, in the spec's timezone.
// It errors when there is none within the parser's search horizon, which is
// how an expression like "0 0 30 2 *" is reported rather than silently
// yielding the zero time.
func (s Spec) Next(t time.Time) (time.Time, error) {
	next := s.sched.Next(t.In(s.loc))
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("scheduler: expression %q has no future occurrence", s.expr)
	}
	return next, nil
}
