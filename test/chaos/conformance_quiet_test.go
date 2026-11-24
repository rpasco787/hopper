//go:build conformance

package chaos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/test/accounting"
)

const (
	msgJobStarted    = "job started"
	msgLeaseRanOut   = "job left unconfirmed; its lease ran out and it will be redelivered"
	msgConfirmRefuse = "job confirmation refused; someone else may have it now"
	msgNotConfirmed  = "lease not confirmed; leaving the job to be redelivered"
)

// TestConformanceQuietRun: one stack, nothing broken, one contract after
// another (docs/semantics.md §1, §4, §6, §7, §10). Every step is judged by
// accounting.Judge on the evidence it saves, so a copy of that evidence is
// judged again by the same rules (go test -tags evidence).
func TestConformanceQuietRun(t *testing.T) {
	c := conform(t, "quiet")
	var burst string

	c.step(t, "Burst", func(t *testing.T) { // §1
		burst = c.enqueue("burst", 1000, 100)
		c.waitFinished(burst, 1000, 3*time.Minute)
		c.judge("burst", burst, accounting.Expect{Scenario: "burst", Mode: accounting.ModeAttempts,
			NoExtras: true, EveryWorkerRan: true}, nil)
	})

	c.step(t, "ConcurrencyBound", func(t *testing.T) { // §4
		require.NotEmpty(t, burst, "the burst ran first")
		c.judge("concurrency", burst, accounting.Expect{Scenario: "concurrency", Mode: accounting.ModeAttempts,
			Saturated: true}, nil)
	})

	c.step(t, "FailBelowMaxAttempts", func(t *testing.T) { // §6
		run := c.newRun("failbelow")
		payloads := make([]string, 20)
		for i := range payloads {
			payloads[i] = `{"fail":2}`
		}
		ids := c.enqueueJobs(run, "fail_n_times", payloads, 5, 0)
		c.waitFinished(run, len(ids), 2*time.Minute)
		c.judge("fail-below-max", run, accounting.Expect{Scenario: "fail-below-max", Mode: accounting.ModeAttempts,
			RepeatCauses: []string{accounting.CauseNacked},
			Rows: wantRows(ids, accounting.RowWant{State: "completed", Attempts: 3, Starts: ptr(3),
				LastError: ptr("intentional failure on attempt 2 of 2")})}, nil)
	})

	c.step(t, "FailAtMaxAttempts", func(t *testing.T) { // §7
		run := c.newRun("failat")
		payloads := make([]string, 20)
		for i := range payloads {
			payloads[i] = `{"fail":3}`
		}
		ids := c.enqueueJobs(run, "fail_n_times", payloads, 3, 0)
		c.waitFinished(run, len(ids), 2*time.Minute)
		c.judge("fail-at-max", run, accounting.Expect{Scenario: "fail-at-max", Mode: accounting.ModeAttempts,
			RepeatCauses: []string{accounting.CauseNacked},
			Rows: wantRows(ids, accounting.RowWant{State: "dead", Attempts: 3, Starts: ptr(3),
				LastError: ptr("intentional failure on attempt 3 of 3")})}, nil)
	})

	c.step(t, "CancelWhileRunning", func(t *testing.T) { // §10: stopped at its first renewal
		run := c.newRun("cancelrun")
		id := c.enqueueJobs(run, "sleep", []string{`{"ms":20000}`}, 5, 10)[0]
		c.awaitLine(id, []string{msgJobStarted}, time.Minute)
		c.cancel(id)
		c.awaitLine(id, []string{msgLeaseRanOut}, 30*time.Second)
		c.judge("cancel-running", run, accounting.Expect{Scenario: "cancel-running", Mode: accounting.ModeAttempts,
			Grant: grant, Cancels: map[string]string{id.String(): accounting.CancelStoppedAtRenewal},
			Rows: map[string]accounting.RowWant{id.String(): {State: "cancelled", Attempts: 1, Starts: ptr(1)}}}, nil)
	})

	c.step(t, "CancelAfterAShortHandler", func(t *testing.T) { // §10: its Ack refused
		run := c.newRun("cancelshort")
		id := c.enqueueJobs(run, "sleep", []string{`{"ms":3000}`}, 5, 10)[0]
		c.awaitLine(id, []string{msgJobStarted}, time.Minute)
		c.cancel(id)
		c.awaitLine(id, []string{msgConfirmRefuse}, 30*time.Second)
		c.judge("cancel-short", run, accounting.Expect{Scenario: "cancel-short", Mode: accounting.ModeAttempts,
			Grant: grant, Cancels: map[string]string{id.String(): accounting.CancelAckRefused},
			Rows: map[string]accounting.RowWant{id.String(): {State: "cancelled", Attempts: 1, Starts: ptr(1)}}}, nil)
	})

	c.step(t, "CancelAtLeaseTime", func(t *testing.T) { // §10: renewal stop, or never started
		run := c.newRun("cancellease")
		id := c.enqueueJobs(run, "sleep", []string{`{"ms":20000}`}, 5, 10)[0]
		c.awaitLeased(id, time.Minute)
		c.cancel(id)
		line, _ := c.awaitLine(id, []string{msgNotConfirmed, msgLeaseRanOut}, 30*time.Second)
		t.Logf("cancel at lease time: the first terminal evidence was %q", line)
		c.judge("cancel-lease-time", run, accounting.Expect{Scenario: "cancel-lease-time", Mode: accounting.ModeAttempts,
			Grant: grant, Cancels: map[string]string{id.String(): accounting.CancelRenewalOrNeverStarted},
			Rows: map[string]accounting.RowWant{id.String(): {State: "cancelled", Attempts: 1}}}, nil)
	})
}

func ptr[T any](v T) *T { return &v }

// wantRows gives every id the same wanted ending.
func wantRows[ID fmt.Stringer](ids []ID, w accounting.RowWant) map[string]accounting.RowWant {
	m := make(map[string]accounting.RowWant, len(ids))
	for _, id := range ids {
		m[id.String()] = w
	}
	return m
}
