//go:build chaos

package chaos

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Stopping after another connection succeeds must leave the independent
// probe time to observe recovery, rather than cancel its next sample.
func TestProbeStopWaitsForRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &probe{cancel: cancel, done: make(chan struct{}), recovered: make(chan struct{})}
	go func() {
		defer close(p.done)
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
			p.w.Recovered = time.Now()
			close(p.recovered)
		}
		<-ctx.Done()
	}()
	require.False(t, p.stop().Recovered.IsZero(), "probe stopped before its recovery sample")
}
