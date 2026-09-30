//go:build chaos

package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

// storeFailures are the broker lines that mean a store call failed.
var storeFailures = map[string]bool{
	"rpc unavailable": true, "rpc failed": true, "queue depth sample failed": true, "sweep failed": true,
}

// brokerView is what the brokers logged about a database outage.
type brokerView struct {
	unavailable, failed map[string]int  // answers by RPC: UNAVAILABLE, which the worker retries, or INTERNAL, which it gives up on
	ackUnavailable      map[string]bool // job ids whose Ack some broker answered UNAVAILABLE
	first, last         time.Time       // the first and last store failure any broker logged
}

func (s *stack) brokerView(brokers []string) brokerView {
	s.t.Helper()
	v := brokerView{unavailable: map[string]int{}, failed: map[string]int{}, ackUnavailable: map[string]bool{}}
	for _, b := range brokers {
		es, err := s.entries(b)
		require.NoError(s.t, err)
		for _, e := range es {
			if !storeFailures[e.Msg] {
				continue
			}
			if v.first.IsZero() || e.Time.Before(v.first) {
				v.first = e.Time
			}
			if e.Time.After(v.last) {
				v.last = e.Time
			}
			switch e.Msg {
			case "rpc unavailable":
				v.unavailable[e.RPC]++
				if e.RPC == "Ack" {
					v.ackUnavailable[e.JobID] = true
				}
			case "rpc failed":
				v.failed[e.RPC]++
			}
		}
	}
	return v
}

// TestPostgresRestartLosesNoAck takes PostgreSQL down in the middle of a run,
// gracefully (the image's SIGINT, a fast shutdown) or by SIGKILL (the next
// start runs crash recovery), and brings it back once six gated deliveries,
// started before it went down, have returned from their handlers. Each gated
// delivery's Ack therefore meets the outage. The brokers must ride it out
// without restarting and answer every Ack, Nack and Heartbeat that met it
// UNAVAILABLE, so the workers retry rather than give up: no job is lost and no
// handler starts twice. That holds while each delivery still has lease left
// when the database returns (docs/semantics.md §11.3);
// TestAnOutageLongerThanTheLeaseReexecutes is the other side of that line.
func TestPostgresRestartLosesNoAck(t *testing.T) {
	for _, tc := range []struct {
		name string
		down func(s *stack, pg string)
	}{
		{"graceful", func(s *stack, pg string) { s.stop(pg) }},
		{"crash", func(s *stack, pg string) { s.kill(pg) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := up(t, "postgres-"+tc.name)
			const jobs, gates = 3000, 6
			run := s.enqueue("postgres-"+tc.name, jobs, 100)
			s.waitCompleted(run, jobs/3, 2*time.Minute)
			brokers := append(s.containers("broker"), s.containers("broker-standby")...)
			started := map[string]string{}
			for _, b := range brokers {
				started[b] = s.startedAt(b)
			}
			gated := s.gate(run, gates, 3000)

			pg := s.containers("postgres")[0]
			p := s.startProbe()
			tc.down(s, pg)
			s.waitLogged("handler finished", gated, time.Minute)
			s.start(pg)
			s.waitDatabase()
			window := p.stop()
			require.False(t, window.LastOK.IsZero(), "probe must observe a healthy database before the outage")
			require.False(t, window.FirstFail.IsZero(), "probe must observe the outage")
			require.False(t, window.Recovered.IsZero(), "probe must observe recovery before stopping")
			s.waitFinished(run, jobs+gates, 3*time.Minute)

			rep := s.reconcile(run)
			for _, b := range brokers {
				require.Equal(t, started[b], s.startedAt(b), "%s restarted", b)
				require.Equal(t, 1, s.logged(b, "broker listening"), "%s restarted", b)
			}
			v := s.brokerView(brokers)
			t.Logf("run %s: %s; brokers logged store failures for %s; answered UNAVAILABLE %v, INTERNAL %v; %s",
				run, window, v.last.Sub(v.first).Round(10*time.Millisecond), v.unavailable, v.failed, summary(rep))
			require.Empty(t, rep.ExecutionProblems(), "no job lost and no handler started twice")
			for _, rpc := range []string{"Ack", "Nack", "Heartbeat"} {
				require.Zero(t, v.failed[rpc], "brokers answered %d %s calls INTERNAL; every one that met the outage must be UNAVAILABLE", v.failed[rpc], rpc)
			}
			for _, id := range gated {
				require.True(t, v.ackUnavailable[id], "gated job %s's Ack never met the outage; the run says nothing about acks", id)
			}
		})
	}
}

// TestAnOutageLongerThanTheLeaseReexecutes is the window §11.3 documents.
// PostgreSQL stays down for longer than any lease, so every gated delivery's
// Ack is retried until its lease runs out, the worker gives the delivery up
// and says so, and the job runs again once the database is back. Every job
// still completes, and every repeated start is one its worker logged giving
// up.
func TestAnOutageLongerThanTheLeaseReexecutes(t *testing.T) {
	down := durationSetting(t, "HOPPER_CHAOS_LONG_OUTAGE", 40*time.Second)
	s := up(t, "long-outage")
	const jobs, gates = 2000, 6
	run := s.enqueue("long-outage", jobs, 100)
	s.waitCompleted(run, jobs/4, 2*time.Minute)
	gated := s.gate(run, gates, 3000)

	pg := s.containers("postgres")[0]
	p := s.startProbe()
	stopped := time.Now()
	s.stop(pg)
	s.waitLogged("handler finished", gated, time.Minute)
	time.Sleep(time.Until(stopped.Add(down)))
	s.start(pg)
	s.waitDatabase()
	window := p.stop()
	require.False(t, window.LastOK.IsZero(), "probe must observe a healthy database before the outage")
	require.False(t, window.FirstFail.IsZero(), "probe must observe the outage")
	require.False(t, window.Recovered.IsZero(), "probe must observe recovery before stopping")
	s.waitFinished(run, jobs+gates, 5*time.Minute)

	rep := s.reconcile(run)
	t.Logf("run %s: PostgreSQL stopped for %s; %s; %s", run, down, window, summary(rep))
	require.Empty(t, rep.AttemptProblems(), "every repeated start must be explained")
	repeated := map[string]bool{}
	for _, r := range rep.Repeats {
		require.Equal(t, accounting.CauseLost, r.Cause, "job %s lease %s ran again for a reason other than a lease the worker gave up", r.ID, r.LeaseID)
		repeated[r.ID.String()] = true
	}
	for _, id := range gated {
		require.True(t, repeated[id], "gated job %s did not run again; an outage of %s should outlast its lease", id, down)
	}
}
