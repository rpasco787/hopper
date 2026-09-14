package store

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy computes the backoff before a failed job is retried. Nack and
// the lease reaper both use it, so every failure path waits the same way.
//
//	delay = min(Cap, Base * 2^(attempts-1)) * U(0.75, 1.25)
type RetryPolicy struct {
	Base time.Duration
	Cap  time.Duration
	rand func() float64 // returns [0,1); nil means math/rand/v2. Tests inject a fixed value.
}

// DefaultRetryPolicy is base 1s, cap 5m: a job that always fails is retried at
// roughly +1s, +2s, +4s, +8s and then dies at the default max_attempts of 5.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{Base: time.Second, Cap: 5 * time.Minute}
}

// Delay returns the backoff for a job that has been leased attempts times.
// attempts is 1 on the first failure, so the first retry waits about Base.
func (p RetryPolicy) Delay(attempts int32) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	// float64 so large exponents saturate at Cap instead of overflowing int64.
	raw := math.Min(float64(p.Cap), float64(p.Base)*math.Pow(2, float64(attempts-1)))

	u := p.rand
	if u == nil {
		u = rand.Float64
	}
	jitter := 0.75 + 0.5*u()

	return time.Duration(raw * jitter)
}
