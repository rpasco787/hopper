package scheduler

import "time"

// IdempotencyKey is the dedup key for one tick of one schedule
// (docs/semantics.md §8). Exactly six fractional digits, because that is
// PostgreSQL's timestamptz resolution: time.RFC3339Nano trims trailing zeros,
// so the same instant would key differently depending on whether it happened
// to land on a whole second.
//
// tick must always be the NextRunAt that came back from PostgreSQL, never a
// locally computed instant: a Go time.Time carries nanoseconds the column
// cannot store, so a locally computed value and its stored counterpart can
// differ and produce two keys for one tick (docs/plans decision 5).
func IdempotencyKey(name string, tick time.Time) string {
	return name + "@" + tick.UTC().Format("2006-01-02T15:04:05.000000Z")
}
