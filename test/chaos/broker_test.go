//go:build chaos

package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// streamBroke is what a worker logs when its lease stream ends under it.
var streamBroke = map[string]bool{
	"lease stream failed; reconnecting": true,
	"lease stream closed; reconnecting": true,
}

// rodeOut counts deliveries a worker started before its lease stream first
// broke and confirmed after: handlers running through the disruption whose
// Ack or Nack still got through.
func rodeOut(es []entry) int {
	broke := -1
	for i, e := range es {
		if streamBroke[e.Msg] {
			broke = i
			break
		}
	}
	if broke < 0 {
		return 0
	}
	before := map[string]bool{}
	for _, e := range es[:broke] {
		if e.Msg == "job started" {
			before[e.LeaseID] = true
		}
	}
	n := 0
	for _, e := range es[broke:] {
		if e.Msg == "job confirmed" && before[e.LeaseID] {
			n++
		}
	}
	return n
}

// TestBrokerRestartLosesNothing restarts the broker every worker is connected
// to, in the middle of a run: gracefully, as a deploy does, and by SIGKILL, as
// a crash does. Brokers hold no job state, so nothing may be lost and no
// handler may start twice; workers must reconnect and go on taking work. A
// delivery the broker had sent but the worker never received is redelivered
// after its lease, which the report counts but the claim allows (§11.5).
func TestBrokerRestartLosesNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		disrupt func(s *stack, broker string)
	}{
		{"graceful", func(s *stack, b string) { s.restart(b) }},
		{"crash", func(s *stack, b string) { s.kill(b); s.start(b) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := up(t, "broker-"+tc.name)
			const jobs = 3000
			run := s.enqueue("broker-"+tc.name, jobs, 100)
			s.waitCompleted(run, jobs/3, 2*time.Minute)
			broker := s.containers("broker")[0]
			tc.disrupt(s, broker)
			s.waitFinished(run, jobs, 3*time.Minute)

			rep := s.reconcile(run)
			t.Logf("run %s: %s", run, summary(rep))
			require.Empty(t, rep.ExecutionProblems(), "no job lost and no handler started twice")
			require.Equal(t, 2, s.logged(broker, "broker listening"), "%s did not restart", broker)
			rode := 0
			for _, w := range s.containers("worker") {
				es, err := s.entries(w)
				require.NoError(t, err)
				opened, startedAfter := 0, false
				for _, e := range es {
					switch e.Msg {
					case "lease stream opened":
						opened++
						startedAfter = false
					case "job started":
						startedAfter = true
					}
				}
				require.GreaterOrEqual(t, opened, 2, "%s never reconnected", w)
				require.True(t, startedAfter, "%s took no work after reconnecting", w)
				rode += rodeOut(es)
			}
			t.Logf("  %d deliveries started before their stream broke were confirmed after it", rode)
			require.Positive(t, rode, "no handler was running when the broker went away; the run says nothing about riding it out")
		})
	}
}
