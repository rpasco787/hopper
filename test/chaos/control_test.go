//go:build chaos

package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestUndisturbedRunIsClean is the control: the same stack and accounting
// with nothing broken must show every job completing on its first lease. If
// this fails, no other result in the suite means anything.
func TestUndisturbedRunIsClean(t *testing.T) {
	s := up(t, "control")
	run := s.enqueue("control", 1000, 100)
	s.waitFinished(run, 1000, 3*time.Minute)
	rep := s.reconcile(run)
	t.Logf("run %s: %s", run, summary(rep))
	require.Empty(t, rep.ExecutionProblems())
	require.Empty(t, rep.AttemptProblems())
	require.Empty(t, rep.Extras, "an undisturbed run redelivered jobs")
	for _, w := range s.containers("worker") {
		require.Positive(t, s.logged(w, "job started"), "%s ran nothing; the run does not exercise three workers", w)
	}
}
