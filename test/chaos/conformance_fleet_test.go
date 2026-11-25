//go:build conformance

package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

// TestConformanceMixedFleet: two Java workers and two Go workers on the same
// queues (D20). Both SDKs lease from the same brokers, write to the same rows
// and are judged together, so a contract one reads differently from the other
// shows here. It runs only in the pass that names a worker image.
func TestConformanceMixedFleet(t *testing.T) {
	if workerImage == "" {
		t.Skip("the mixed fleet runs in the pass with HOPPER_CONFORMANCE_WORKER set, Go beside that image")
	}
	c := conform(t, "mixed")
	c.scale("worker", 2)
	c.addGoWorkers(2)

	c.step(t, "Burst", func(t *testing.T) { // §1, over all four workers
		run := c.enqueue("mixed-burst", 1000, 100)
		c.waitFinished(run, 1000, 3*time.Minute)
		c.judge("mixed-burst", run, accounting.Expect{Scenario: "mixed-burst", Mode: accounting.ModeAttempts,
			NoExtras: true, EveryWorkerRan: true}, nil)
	})

	c.step(t, "Kill", func(t *testing.T) { // §11 item 1: one process of each SDK
		run := c.enqueue("mixed-kill", 2000, 100)
		c.waitCompleted(run, 200, 2*time.Minute)
		for _, service := range []string{"worker", "worker-go"} {
			victim := c.busiest(service)
			require.Positive(t, c.openDeliveries(victim), "the busiest %s %s has no delivery open; the kill would cut nothing", service, victim)
			n := c.acceptedCount(victim)
			c.kill(victim)
			time.Sleep(2 * time.Second)
			c.start(victim)
			c.awaitAccepted(victim, n)
		}
		c.waitFinished(run, 2000, 5*time.Minute)
		c.judge("mixed-kill", run, accounting.Expect{Scenario: "mixed-kill", Mode: accounting.ModeAttempts,
			RepeatCauses: []string{accounting.CauseKilled}, MinRepeatGap: 25 * time.Second, Kills: 2}, nil)
	})
}
