package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJobBucketsResolveABacklog pins the property the 10k burst needed. When
// every observation in a rate window lands in one bucket, histogram_quantile
// reports p99 near that bucket's upper edge, so the ratio between adjacent
// buckets bounds how far p99 can overstate. At factor 2 a 91s p99 read 119.4s.
// Above 180s the buckets still double; a backlog that deep is out of scope.
func TestJobBucketsResolveABacklog(t *testing.T) {
	require.IsIncreasing(t, JobBuckets)
	require.Equal(t, 600.0, JobBuckets[len(JobBuckets)-1], "a ten-minute backlog must not clamp")
	for i := 1; i < len(JobBuckets); i++ {
		lo, hi := JobBuckets[i-1], JobBuckets[i]
		if lo < 10 || hi > 180 {
			continue
		}
		require.LessOrEqual(t, hi/lo, 1.5, "buckets %g and %g are too far apart", lo, hi)
	}
}
