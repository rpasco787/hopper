//go:build conformance

package chaos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

// TestConformanceKilledWorker: SIGKILL the busiest worker in the middle of a
// run and start it two seconds later (docs/semantics.md §11 item 1). Every job
// completes, every repeated start is a killed process's, taken back only when
// its lease ran out, and exactly one process was cut with a handler unfinished.
func TestConformanceKilledWorker(t *testing.T) {
	c := conform(t, "kill")
	run := c.enqueue("kill", 1500, 100)
	c.waitCompleted(run, 200, 2*time.Minute)

	victim := c.busiest("worker")
	require.Positive(t, c.openDeliveries(victim), "the busiest worker %s has no delivery open; the kill would cut nothing", victim)
	n := c.acceptedCount(victim)
	c.kill(victim)
	time.Sleep(2 * time.Second)
	c.start(victim)
	c.awaitAccepted(victim, n)
	c.waitFinished(run, 1500, 5*time.Minute)

	c.judge("kill", run, accounting.Expect{Scenario: "kill", Mode: accounting.ModeAttempts,
		RepeatCauses: []string{accounting.CauseKilled}, MinRepeatGap: 25 * time.Second, Kills: 1}, nil)
}

// TestConformanceBrokerGoneLongerThanTheLease: SIGKILL the broker the workers
// dial, for 45 s, under three 45 s handlers (§11 item 2). Nothing can renew
// or confirm, so each handler must be stopped at its own local deadline, leave
// its lease unconfirmed, and run again once the broker is back.
func TestConformanceBrokerGoneLongerThanTheLease(t *testing.T) {
	c := conform(t, "broker-gone")
	run := fmt.Sprintf("broker-gone-%d", time.Now().UnixNano())
	ids := c.gate(run, 3, 45000)
	time.Sleep(time.Second)

	b := c.containers("broker")[0]
	c.kill(b)
	from := c.finishedAt(b) // the Docker VM's clock, which the workers' log times are on
	time.Sleep(45 * time.Second)
	c.start(b)
	c.waitFinished(run, 3, 4*time.Minute)

	c.judge("broker-gone", run, accounting.Expect{Scenario: "broker-gone", Mode: accounting.ModeAttempts,
		RepeatCauses: []string{accounting.CauseLost}, Expired: ids, OutageStart: from, Grant: grant}, nil)
}

// TestConformanceShortPostgresOutage: PostgreSQL stopped gracefully for about
// three seconds while six gated deliveries' handlers return (§11 item 3). Their
// Acks meet the outage and are retried inside the lease: no job runs twice, and
// each gated job completes under its first lease.
func TestConformanceShortPostgresOutage(t *testing.T) {
	c := conform(t, "postgres")
	run := c.enqueue("postgres", 1500, 100)
	c.waitCompleted(run, 500, 2*time.Minute)
	gated := c.gate(run, 6, 3000)

	pg := c.containers("postgres")[0]
	p := c.startProbe()
	c.stop(pg)
	c.waitLogged("handler finished", gated, time.Minute)
	c.start(pg)
	c.waitDatabase()
	w := p.stop()
	require.False(t, w.LastOK.IsZero(), "the probe must see a healthy database before the outage")
	require.False(t, w.FirstFail.IsZero(), "the probe must see the outage")
	require.False(t, w.Recovered.IsZero(), "the probe must see the recovery")
	require.Less(t, w.Recovered.Sub(w.LastOK), 10*time.Second, "new sessions failed for too long to say the outage was short: %s", w)
	c.t.Logf("postgres outage: %s", w)
	c.waitFinished(run, 1506, 3*time.Minute)

	c.judge("postgres", run, accounting.Expect{Scenario: "postgres", Mode: accounting.ModeExecution, FirstLease: gated}, nil)
}

// TestConformanceSigtermDrainsCleanly: docker stop on the busiest worker in the
// middle of a run (§11 item 4). It drains inside its budget and exits 0, every
// delivery open at the signal ends on a confirmation, and no job runs twice.
// What the drain left over, as nacked unstarted jobs or leases a worker never
// received, is reported, not forbidden (D16).
func TestConformanceSigtermDrainsCleanly(t *testing.T) {
	c := conform(t, "sigterm")
	run := c.enqueue("sigterm", 600, 250)
	c.eventually(func() bool { return c.startedJobs(run) >= 30 },
		2*time.Minute, 250*time.Millisecond, "fewer than 30 of run %s's jobs logged %q", run, msgJobStarted)

	w := c.busiest("worker")
	require.Positive(t, c.openDeliveries(w), "the busiest worker %s has no delivery open; the drain would have nothing to wait for", w)
	c.stop(w) // no -t: the container's own grace period applies
	exit := c.exitCode(w)
	n := c.acceptedCount(w)
	c.start(w)
	c.awaitAccepted(w, n)
	c.waitFinished(run, 600, 3*time.Minute)

	c.judge("sigterm", run, accounting.Expect{Scenario: "sigterm", Mode: accounting.ModeGraceful,
		Exits: map[string]int{w: 0}, DrainedHandlers: true}, map[string]int{w: exit})
}

// TestConformanceShutdownDeadlineExceeded: one worker with a two second budget
// is stopped under two 20 s handlers (§11 item 4). It abandons both, says so on
// one line with its four counts and once on stderr, and exits 1; both jobs
// complete later, after their leases ran out.
func TestConformanceShutdownDeadlineExceeded(t *testing.T) {
	c := conform(t, "deadline", "HOPPER_CHAOS_SHUTDOWN_TIMEOUT=2s")
	c.scale("worker", 1)
	ws := c.containers("worker")
	require.Len(t, ws, 1, "the stack should hold one worker")
	w := ws[0]

	run := c.enqueue("deadline", 2, 20000)
	c.eventually(func() bool { return c.startedJobs(run) >= 2 },
		time.Minute, 100*time.Millisecond, "both of run %s's jobs never logged %q", run, msgJobStarted)

	c.stop(w)
	exit := c.exitCode(w)
	n := c.acceptedCount(w)
	c.start(w)
	c.awaitAccepted(w, n)
	c.waitFinished(run, 2, 3*time.Minute)

	c.judge("deadline", run, accounting.Expect{Scenario: "deadline", Mode: accounting.ModeAttempts,
		Exits: map[string]int{w: 1},
		Stderr: map[string]string{w: "worker: worker: shutdown deadline reached with 2 active deliveries " +
			"(2 running handlers, 0 acquiring a lease, 0 awaiting confirmation)"},
		Deadline: map[string]accounting.DeadlineCounts{w: {Deliveries: 2, Handlers: 2}},
		Problems: []string{
			"pod " + w + ": shutdown deadline reached; work was abandoned",
			"pod " + w + ": shutdown started but the worker never logged a clean stop",
		},
		RepeatCauses: []string{accounting.CauseLost, accounting.CauseKilled}}, map[string]int{w: exit})
}
