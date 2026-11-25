package sharded

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOrderRotatesAndSkipsDown(t *testing.T) {
	h := newHealth(4)
	now := time.Unix(100, 0)
	require.Equal(t, []int{2, 3, 0, 1}, h.order(2, now))
	h.down(3, now)
	require.Equal(t, []int{2, 0, 1}, h.order(2, now))
	require.Equal(t, []int{2, 3, 0, 1}, h.order(2, now.Add(downFor)), "down expires")
	h.down(3, now)
	h.up(3)
	require.Equal(t, []int{2, 3, 0, 1}, h.order(2, now), "a probe brings it back")
	for i := range 4 {
		h.down(i, now)
	}
	require.Equal(t, []int{1, 2, 3, 0}, h.order(1, now), "all down: try them all")
}
