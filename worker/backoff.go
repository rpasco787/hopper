package worker

import (
	"math/rand/v2"
	"time"
)

// Reconnect and confirmation retry pacing. These are the SDK's own; job retry
// scheduling belongs to the store (docs/semantics.md §6) and nothing here
// influences it.
const (
	backoffBase = 100 * time.Millisecond
	// backoffCap is the cap on the *nominal* delay, before jitter. Jitter can
	// take a capped delay up to 1.25×, so the longest wait this produces is
	// 6.25s, not 5s. Saying "5s cap" would be wrong by a second and a quarter.
	backoffCap = 5 * time.Second

	jitterFloor = 0.75
	jitterRange = 0.5 // so the multiplier lands in [0.75, 1.25).
)

// backoff produces exponentially growing, jittered delays. It is not safe for
// concurrent use; each retry loop owns one.
//
// The jitter is not decoration. A broker restart disconnects every worker at
// once, and undithered exponential backoff would have them all reconnect in
// the same few milliseconds, then again, and again.
type backoff struct {
	attempt int
	// jitter returns a value in [0, 1). It is a field so tests can pin the
	// multiplier to its exact extremes rather than sampling and hoping.
	jitter func() float64
}

func newBackoff() *backoff {
	return &backoff{jitter: rand.Float64}
}

// next returns the next delay and advances the sequence.
func (b *backoff) next() time.Duration {
	// The shift is bounded before it happens: 1<<63 on a Duration is negative,
	// and a negative delay fires a timer immediately, which is the opposite of
	// backing off.
	d := backoffCap
	if b.attempt < 62 {
		if shifted := backoffBase << b.attempt; shifted > 0 && shifted < backoffCap {
			d = shifted
		}
	}
	b.attempt++
	return time.Duration(float64(d) * (jitterFloor + jitterRange*b.jitter()))
}

// reset restarts the sequence. Callers reset only after an attempt did
// meaningful work, never after every attempt that merely failed differently:
// resetting on a clean immediate EOF turns backoff into a tight loop.
func (b *backoff) reset() { b.attempt = 0 }
