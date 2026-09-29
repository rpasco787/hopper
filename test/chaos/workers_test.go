//go:build chaos

package chaos

import (
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

// TestKilledWorkersLoseNothing kills random workers with SIGKILL throughout a
// run and starts each again two seconds later. At-least-once allows a job to
// run again, but only as attempt accounting explains: every job completes,
// every lease starts at most one handler, and every repeated start is a
// handler whose process was killed before it was confirmed, and before the
// job started again.
func TestKilledWorkersLoseNothing(t *testing.T) {
	jobs := intSetting(t, "HOPPER_CHAOS_JOBS", 10000)
	ms := intSetting(t, "HOPPER_CHAOS_JOB_MS", 100)
	seed := uint64(time.Now().UnixNano())
	if v := setting("HOPPER_CHAOS_SEED", ""); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		require.NoError(t, err, "HOPPER_CHAOS_SEED=%q", v)
		seed = n
	}
	t.Logf("HOPPER_CHAOS_SEED=%d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	s := up(t, "kill")
	workers := s.containers("worker")
	run := s.enqueue("kill", jobs, ms)
	s.waitCompleted(run, jobs/20, 2*time.Minute)

	// A random worker every 5–10s while a tenth of the run is still to do,
	// so every kill lands in a busy fleet, and none so late that the run
	// ends before its lost leases expire.
	kills := 0
	deadline := time.Now().Add(15 * time.Minute)
	for {
		completed, _, err := s.counts(run)
		require.NoError(t, err)
		if completed >= jobs*9/10 {
			break
		}
		require.True(t, time.Now().Before(deadline), "run %s stalled at %d completed", run, completed)
		time.Sleep(5*time.Second + time.Duration(rng.Int64N(int64(5*time.Second))))
		victim := workers[rng.IntN(len(workers))]
		s.kill(victim)
		time.Sleep(2 * time.Second)
		s.start(victim)
		kills++
	}
	require.GreaterOrEqual(t, kills, 5, "the run finished after %d kills; raise HOPPER_CHAOS_JOBS", kills)
	// A killed handler's lease expires after 30s, and its retry waits out
	// the backoff and the promoter before it runs again.
	s.waitFinished(run, jobs, 5*time.Minute)

	rep := s.reconcile(run)
	t.Logf("run %s: %d kills; %s", run, kills, summary(rep))
	for _, p := range rep.Processes {
		if p.Cut {
			t.Logf("  %s process %d: cut with %d of %d handlers unfinished", p.Pod, p.N, p.Interrupted, p.Starts)
		}
	}
	require.Empty(t, rep.AttemptProblems())
	cut, interrupted := 0, 0
	for _, p := range rep.Processes {
		if p.Cut {
			cut++
			interrupted += p.Interrupted
		}
	}
	require.Equal(t, kills, cut, "each kill must show in the logs as one process cut short")
	require.Positive(t, interrupted, "no kill caught a handler running; the run says nothing about redelivery")
	// Every handler a kill interrupted never acked, so its job ran again.
	require.GreaterOrEqual(t, len(rep.Repeats), interrupted)
	for _, r := range rep.Repeats {
		require.Equal(t, accounting.CauseKilled, r.Cause, "job %s lease %s ran again for a reason other than a kill", r.ID, r.LeaseID)
		// Accounting places the kill after the process's last line. A killed
		// delivery's job can also be leased again only once its 30s lease has
		// expired, which bounds the gap from below even for a process that was
		// quiet before it was killed.
		require.GreaterOrEqual(t, r.NextStartedAt.Sub(r.StartedAt), 25*time.Second,
			"job %s ran again %s after lease %s started; a killed lease is taken back only when it expires",
			r.ID, r.NextStartedAt.Sub(r.StartedAt), r.LeaseID)
	}
}
