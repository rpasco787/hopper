//go:build chaos

package chaos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// agedOut is what a broker logs when it ends a lease stream for age.
const agedOut = "lease stream reached its maximum age"

// scaleWorkers runs n workers and waits until each has opened a stream.
func (s *stack) scaleWorkers(n int) {
	s.t.Helper()
	s.compose(upLimit, "up", "-d", "--no-build", "--no-recreate", "--wait",
		"--scale", fmt.Sprintf("worker=%d", n), "worker")
	for _, w := range s.containers("worker") {
		s.eventually(func() bool { return s.logged(w, "lease stream opened") > 0 },
			time.Minute, 200*time.Millisecond, "%s never opened a lease stream", w)
	}
}

// openStreams is how many lease streams broker container c serves now, read
// from its log: streams opened minus streams closed since its current process
// started. hopper_lease_streams_open says the same thing, but the suite reads
// logs, and the brokers' metrics ports are not published here.
func (s *stack) openStreams(c string) int {
	es, _ := s.entries(c)
	start := 0
	for i, e := range es {
		if e.Msg == "broker listening" {
			start = i
		}
	}
	n := 0
	for _, e := range es[start:] {
		switch e.Msg {
		case "lease stream opened":
			n++
		case "lease stream closed":
			n--
		}
	}
	return n
}

// loggedBetween counts container c's log lines with message msg whose time
// falls in [from, to).
func (s *stack) loggedBetween(c, msg string, from, to time.Time) int {
	es, _ := s.entries(c)
	n := 0
	for _, e := range es {
		if e.Msg == msg && !e.Time.Before(from) && e.Time.Before(to) {
			n++
		}
	}
	return n
}

// TestLeaseStreamAgeOutsStrandNothing is the control run with one change:
// workers dial both brokers by one name, and every lease stream and client
// connection ages out about every 5s. Nothing is broken, so as in
// TestUndisturbedRunIsClean every job must complete on its first lease. A
// stream that aged out with a row leased but unsent would show here as an
// extra lease. ExecutionProblems alone allows those (NeverReceived), which is
// why Extras is checked as well.
func TestLeaseStreamAgeOutsStrandNothing(t *testing.T) {
	const (
		age  = 5 * time.Second
		jobs = 6000
	)
	s := up(t, "ageouts", "HOPPER_CHAOS_WORKER_BROKER=brokers:9090", "HOPPER_CHAOS_STREAM_AGE="+age.String())
	brokers := []string{s.containers("broker")[0], s.containers("broker-standby")[0]}

	from := time.Now()
	run := s.enqueue("ageouts", jobs, 100)
	s.waitFinished(run, jobs, 5*time.Minute)
	to := time.Now()

	rep := s.reconcile(run)
	t.Logf("run %s: %s", run, summary(rep))
	require.Empty(t, rep.ExecutionProblems())
	require.Empty(t, rep.AttemptProblems())
	require.Empty(t, rep.Extras, "an age-out stranded a lease, or something else redelivered a job")
	aged := 0
	for _, b := range brokers {
		aged += s.loggedBetween(b, agedOut, from, to)
	}
	t.Logf("  %d lease streams aged out while jobs ran", aged)
	require.GreaterOrEqual(t, aged, 10, "too few age-outs during the run to say anything about them")
}

// TestStreamsSpreadBackAfterABrokerReturns: eight workers dial both brokers
// by one name. One broker is stopped, and the test waits until the survivor
// serves every stream: that is the state recovery is measured from. The
// broker is then started again. Streams return to it only as the survivor's
// streams age out (about every 20s) and round robin places them, and only
// after the clients have resolved the name again, which the survivor's
// connection age triggers. Placement is random per worker, so the test waits
// for what round robin can promise, every broker serving at least half an
// even share, and reports the time rather than bounding it tightly.
//
// Stopping a broker drains it, and a drain may leave rows leased but unsent
// (§11). So this test checks ExecutionProblems only; the strict accounting is
// in TestLeaseStreamAgeOutsStrandNothing.
func TestStreamsSpreadBackAfterABrokerReturns(t *testing.T) {
	const (
		workers = 8
		age     = 20 * time.Second
		jobs    = 40000
	)
	s := up(t, "rebalance", "HOPPER_CHAOS_WORKER_BROKER=brokers:9090", "HOPPER_CHAOS_STREAM_AGE="+age.String())
	s.scaleWorkers(workers)
	returning, survivor := s.containers("broker")[0], s.containers("broker-standby")[0]

	run := s.enqueue("rebalance", jobs, 100)
	s.waitCompleted(run, jobs/20, 2*time.Minute)

	s.stop(returning)
	s.eventually(func() bool { return s.openStreams(survivor) == workers },
		time.Minute, time.Second, "the survivor never served every stream")

	s.start(returning)
	back := time.Now()
	fair := workers / 4 // half of an even share across two brokers
	s.eventually(func() bool {
		a, b := s.openStreams(returning), s.openStreams(survivor)
		return a+b == workers && a >= fair && b >= fair
	}, 5*time.Minute, time.Second, "streams never spread back to the returned broker")
	settled := time.Now()
	t.Logf("streams per broker %d/%d, %s after the broker came back (stream and connection age %s)",
		s.openStreams(returning), s.openStreams(survivor), settled.Sub(back).Round(time.Second), age)
	require.Positive(t, s.loggedBetween(survivor, agedOut, back, settled),
		"streams reached the returned broker without any of the survivor's ageing out")

	s.waitFinished(run, jobs, 8*time.Minute)
	rep := s.reconcile(run)
	t.Logf("run %s: %s", run, summary(rep))
	require.Empty(t, rep.ExecutionProblems(), "no job lost and no handler started twice")
}
