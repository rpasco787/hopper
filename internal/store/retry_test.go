package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fixedRand makes jitter deterministic: u=0.5 gives a factor of exactly 1.0.
func fixedRand(u float64) func() float64 { return func() float64 { return u } }

func TestRetryPolicyDoublesFromBase(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Cap: 5 * time.Minute, rand: fixedRand(0.5)}
	require.Equal(t, 1*time.Second, p.Delay(1))
	require.Equal(t, 2*time.Second, p.Delay(2))
	require.Equal(t, 4*time.Second, p.Delay(3))
	require.Equal(t, 8*time.Second, p.Delay(4))
}

func TestRetryPolicyCaps(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Cap: 5 * time.Minute, rand: fixedRand(0.5)}
	require.Equal(t, 5*time.Minute, p.Delay(20))
	require.Equal(t, 5*time.Minute, p.Delay(100)) // no overflow at large exponents
}

func TestRetryPolicyJitterBounds(t *testing.T) {
	lo := RetryPolicy{Base: time.Second, Cap: 5 * time.Minute, rand: fixedRand(0)}
	hi := RetryPolicy{Base: time.Second, Cap: 5 * time.Minute, rand: fixedRand(1)}
	require.Equal(t, 750*time.Millisecond, lo.Delay(1))
	require.Equal(t, 1250*time.Millisecond, hi.Delay(1))
}

func TestRetryPolicyJitterAppliesAtCap(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Cap: time.Minute, rand: fixedRand(0)}
	require.Equal(t, 45*time.Second, p.Delay(50))
}

func TestRetryPolicyDefaultsAreUsable(t *testing.T) {
	d := DefaultRetryPolicy().Delay(1)
	require.GreaterOrEqual(t, d, 750*time.Millisecond)
	require.LessOrEqual(t, d, 1250*time.Millisecond)
}
