//go:build k8s

package k8s

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/scheduler"
	"github.com/rpasco787/hopper/test/accounting"
)

// failoverTick is the cron schedule deploy/kind/replicated.yaml declares, one
// tick a second.
const failoverTick = "failover-tick"

// failoverResult is what one primary failover did, as failover.json records
// it. Times are seconds from the crash; negative is before it.
type failoverResult struct {
	Run                  string         `json:"run"`
	Image                string         `json:"broker_image"`
	SynchronousNames     string         `json:"synchronous_standby_names"`
	ReplicationPausedFor string         `json:"replication_paused_for"`
	OldPrimary           string         `json:"old_primary"`
	NewPrimary           string         `json:"new_primary"`
	PromotedAfter        float64        `json:"promoted_after_s"`
	RoutedAfter          float64        `json:"routed_after_s"`
	FirstFailedAfter     float64        `json:"first_failed_enqueue_after_s"`
	RecoveredAfter       float64        `json:"first_acknowledged_enqueue_after_s"`
	AckedWhilePaused     int            `json:"acknowledged_while_paused"`
	DedupedWhilePaused   int            `json:"of_which_deduplicated"`
	Acknowledged         int            `json:"acknowledged"`
	Lost                 int            `json:"lost_acknowledged"`
	LostDedupOnly        int            `json:"lost_answered_only_as_deduplicated"`
	LostFrom             float64        `json:"lost_answered_from_s,omitempty"`
	LostTo               float64        `json:"lost_answered_to_s,omitempty"`
	Deduplicated         int            `json:"answers_that_found_their_job"`
	FailedEnqueues       map[string]int `json:"failed_enqueues_by_code"`
	Jobs                 int            `json:"jobs"`
	Completed            int            `json:"completed"`
	Repeats              map[string]int `json:"repeated_starts_by_cause"`
	AttemptProblems      int            `json:"attempt_problems"`
	Unavailable          map[string]int `json:"broker_unavailable_by_rpc"`
	Internal             map[string]int `json:"broker_internal_by_rpc"`
	Brokers              int            `json:"brokers"`
	ListenersReconnected int            `json:"listeners_reconnected"`
	SchedulerTermsWon    int            `json:"scheduler_terms_won"`
	PendingTickJobs      int            `json:"pending_tick_jobs"`
	LaterTicks           int            `json:"later_ticks"`
	ShutdownLogged       bool           `json:"old_primary_logged_a_shutdown"`
	CancelledSyncWaits   int            `json:"old_primary_cancelled_sync_waits"`

	NewPrimaryCancelledSyncWaits int `json:"new_primary_cancelled_sync_waits"`
	// After the promotion: whether a sample caught the new primary with no
	// synchronous standby, when one first streamed to it, the
	// synchronous_standby_names it reported meanwhile, and what was answered
	// before (standbys.json; docs/semantics.md §11 item 7).
	WithoutStandbySeen      bool    `json:"new_primary_seen_without_standby"`
	FirstStandbyAfter       float64 `json:"first_standby_after_s"`
	WithoutStandbyNames     string  `json:"new_primary_synchronous_standby_names"`
	AnsweredWithoutStandby  int     `json:"answered_without_standby"`
	DedupedWithoutStandby   int     `json:"of_which_deduplicated_without_standby"`
	ConfirmedWithoutStandby int     `json:"confirmed_without_standby"`
}

// TestPrimaryFailoverLosesNothing crashes the primary of a database that
// confirms every commit on a synchronous standby, mid-run, and holds hopper to
// docs/semantics.md §1 across the promotion. Every job Enqueue acknowledged
// exists afterwards and completes. Every repeated handler start is one
// attempt accounting explains, and none follows an acked lease. No broker
// answers INTERNAL. With HOPPER_FAILOVER_LAG the standbys are paused first,
// so the crash meets commits waiting for them; then nothing may be
// acknowledged during the pause, not even a retry that found its row (§11
// item 7). Nothing may be answered, to a producer or a worker, before the new
// primary has a synchronous standby. failover also requires what replication
// doesn't decide: a real crash, no restarts, every wakeup listener back, a new
// scheduler term, and the tick pending at the crash fired once.
func TestPrimaryFailoverLosesNothing(t *testing.T) {
	res, dur, rep := failover(t, true)
	if res.ReplicationPausedFor != "0s" {
		require.Zero(t, res.AckedWhilePaused-res.DedupedWhilePaused,
			"a new job was acknowledged while no standby could confirm it: synchronous replication was not in force")
		require.Zero(t, res.DedupedWhilePaused,
			"a retry was answered deduplicated while no standby could confirm the row it found (docs/semantics.md §11 item 7)")
	}
	require.Empty(t, dur.Lost, "acknowledged jobs missing after the failover; %d were answered only as deduplicated", res.LostDedupOnly)
	require.Empty(t, rep.AttemptProblems(), "every repeated start must be explained, and no acked lease may be followed by another")
	require.Empty(t, res.Internal, "brokers answered INTERNAL; a failover must be answered UNAVAILABLE")
	require.Positive(t, total(res.Unavailable), "no broker call met the failover; the run says nothing")
	require.Zero(t, res.AnsweredWithoutStandby,
		"the new primary answered %d enqueues (%d deduplicated) before any synchronous standby streamed to it",
		res.AnsweredWithoutStandby, res.DedupedWithoutStandby)
	require.Zero(t, res.ConfirmedWithoutStandby,
		"workers were told %d confirmations before the new primary had a synchronous standby", res.ConfirmedWithoutStandby)
}

// TestAsynchronousFailoverCountsWhatItLoses is the contrast: the same crash
// with standbys that confirm nothing. It records what was lost (acknowledged
// jobs missing, and acked deliveries that ran again) and requires only what
// replication doesn't decide. With replication paused first, everything
// acknowledged during the pause should be lost, so it requires that some was.
func TestAsynchronousFailoverCountsWhatItLoses(t *testing.T) {
	res, _, _ := failover(t, false)
	if res.ReplicationPausedFor != "0s" {
		require.Positive(t, res.Lost, "replication was paused for %s and nothing acknowledged was lost: the pause did not hold",
			res.ReplicationPausedFor)
	}
	t.Logf("asynchronous: lost %d of %d acknowledged jobs; %d acked deliveries ran again",
		res.Lost, res.Acknowledged, res.Repeats[accounting.CauseAcked])
}

// failover crashes the replicated database's primary under load and returns
// what happened, the durability check and the attempt accounting. wantSync
// says which replication the release must run.
func failover(t *testing.T, wantSync bool) (failoverResult, accounting.Durability, accounting.Report) {
	c := newCluster(t)
	db := replicatedDatabase(c)
	rate := intSetting(t, "HOPPER_FAILOVER_RATE", 200)
	ms := intSetting(t, "HOPPER_FAILOVER_JOB_MS", 50)
	lag, err := time.ParseDuration(setting("HOPPER_FAILOVER_LAG", "0s"))
	require.NoError(t, err, "HOPPER_FAILOVER_LAG")
	begun := time.Now()
	run := fmt.Sprintf("f2-%d", begun.UnixNano())
	dir := filepath.Join(setting("HOPPER_PROOF_DIR", t.TempDir()), run)
	logs := filepath.Join(dir, "logs")
	workerLogs, brokerLogs := filepath.Join(logs, "workers"), filepath.Join(logs, "brokers")
	for _, d := range []string{workerLogs, brokerLogs} {
		require.NoError(t, os.MkdirAll(d, 0o755))
	}
	bg := context.Background()
	install := "scripts/kind-up.sh -f deploy/kind/replicated.yaml"
	if !wantSync {
		install += " -f deploy/kind/asynchronous.yaml"
	}

	st := db.awaitHealthy(10 * time.Minute)
	names := db.synchronousNames(st.Primary)
	require.Equal(t, wantSync, names != "", "synchronous_standby_names is %q; this proof needs: %s", names, install)
	res := failoverResult{Run: run, SynchronousNames: names, ReplicationPausedFor: lag.String(), OldPrimary: st.Primary,
		Image: c.run("get", "deployment", c.release+"-broker", "-o", "jsonpath={.spec.template.spec.containers[0].image}")}
	// The instances that survive the crash, one of which is promoted. They are
	// watched from the crash on: a new primary serves before any standby
	// streams to it.
	survivors := slices.DeleteFunc(db.instances(), func(pod string) bool { return pod == st.Primary })

	workers := "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=worker"
	brokers := "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=broker"
	for _, d := range []string{"worker", "broker"} {
		c.run("rollout", "status", "deployment/"+c.release+"-"+d, "--timeout=2m")
	}
	wpods, bpods := c.livePods(workers), c.livePods(brokers)
	res.Brokers = len(bpods)
	before := restarts(c, workers) + restarts(c, brokers)
	wcap, bcap := startCapture(c, workers, workerLogs), startCapture(c, brokers, brokerLogs)
	t.Cleanup(func() { _ = wcap.stop(); _ = bcap.stop() })
	require.Eventually(t, func() bool { return wcap.attached(wpods) && bcap.attached(bpods) },
		time.Minute, 200*time.Millisecond, "the workers' and brokers' logs never attached")

	pool := c.database()
	cl, err := client.Dial(c.forward("svc/"+c.release+"-broker", "9090"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })
	p := startProducer(cl, run, rate, ms)
	t.Cleanup(p.stop)
	require.Eventually(t, func() bool { return p.ackedCount() >= 15*rate }, 2*time.Minute, 200*time.Millisecond,
		"the producer never reached 15s of load")

	oldLog := db.follow(st.Primary, filepath.Join(logs, st.Primary+".log"))
	// The tick pending now is one §13 promises fires once, whoever leads. It is
	// read before any pause: a tick fired during the pause advances the
	// schedule on the old primary alone, and under asynchronous replication
	// that advance dies with it, so the new leader rightly never fires the
	// tick the old primary had moved on to.
	var pending time.Time
	require.NoError(t, pool.QueryRow(bg, `select next_run_at from cron_schedules where name = $1`, failoverTick).Scan(&pending),
		"the release must declare the %s schedule: %s", failoverTick, install)
	var paused time.Time
	if lag > 0 {
		t.Cleanup(db.pauseReplication(st.Primary, st.Instances-1))
		paused = time.Now()
		time.Sleep(lag)
	}
	require.True(t, oldLog.live(), "the primary's log stream ended before the crash, so it proves nothing about it")
	crashed := db.crash(st.Primary)
	var standbys func() []standbySample
	if wantSync {
		standbys = db.watchStandbys(survivors, 250*time.Millisecond, 3*time.Minute)
	}
	newPrimary, promoted, routed := db.awaitPromotion(st.Primary, 3*time.Minute)
	firstFailure, recovered, ok := p.awaitRecovery(crashed, 3*time.Minute)
	require.True(t, ok, "producers were never answered again after the crash (first failure %v)", firstFailure)
	time.Sleep(20 * time.Second) // load on the new primary
	p.stop()
	old := oldLog.ended(t)
	newLog := c.run("logs", "pod/"+newPrimary, "-c", "postgres")
	require.NoError(t, os.WriteFile(filepath.Join(logs, newPrimary+".log"), []byte(newLog+"\n"), 0o644))

	pool = c.database() // the old forward went with the old primary
	require.Eventually(t, func() bool {
		var open, all int
		err := pool.QueryRow(bg, `select count(*) filter (where state not in ('completed', 'dead', 'cancelled')), count(*)
		                          from jobs where idempotency_key like $1 || '-%'`, run).Scan(&open, &all)
		return err == nil && open == 0 && all > 0
	}, 10*time.Minute, 2*time.Second, "run %s never finished", run)
	require.NoError(t, pool.QueryRow(bg, `select count(*) from jobs where idempotency_key = $1`,
		scheduler.IdempotencyKey(failoverTick, pending)).Scan(&res.PendingTickJobs))
	require.NoError(t, pool.QueryRow(bg, `select count(*) from jobs where idempotency_key like $1 and run_at > $2`,
		failoverTick+"@%", pending).Scan(&res.LaterTicks))

	require.NoError(t, wcap.stop(), "worker log capture")
	require.NoError(t, bcap.stop(), "broker log capture")
	wcap.snapshot(c.livePods(workers))
	bcap.snapshot(c.livePods(brokers))
	after := restarts(c, workers) + restarts(c, brokers)

	rows := loadRows(t, pool, run)
	prod := p.results(paused, crashed)
	dur := accounting.CheckDurability(prod.acked, rows)
	wlogs := readLogs(t, workerLogs)
	rep := accounting.Reconcile(rows, wlogs)
	bv := readBrokerView(t, brokerLogs, begun)

	since := func(at time.Time) float64 { return at.Sub(crashed).Seconds() }
	res.NewPrimary = newPrimary
	res.PromotedAfter, res.RoutedAfter = since(promoted), since(routed)
	res.FirstFailedAfter, res.RecoveredAfter = since(firstFailure), since(recovered)
	res.AckedWhilePaused, res.DedupedWhilePaused = prod.window, prod.windowDups
	res.Acknowledged, res.Lost = dur.Acknowledged, len(dur.Lost)
	for _, l := range dur.Lost {
		if !prod.fresh[l.ID] {
			res.LostDedupOnly++
		}
	}
	if n := len(dur.Lost); n > 0 {
		res.LostFrom, res.LostTo = since(dur.Lost[0].At), since(dur.Lost[n-1].At)
	}
	res.Deduplicated, res.FailedEnqueues = prod.deduped, prod.failed
	res.Jobs, res.Completed = rep.Jobs, rep.Completed
	res.Repeats = map[string]int{}
	for _, r := range rep.Repeats {
		res.Repeats[cmp.Or(r.Cause, "unexplained")]++
	}
	res.AttemptProblems = len(rep.AttemptProblems())
	res.Unavailable, res.Internal = bv.unavailable, bv.internal
	res.ListenersReconnected, res.SchedulerTermsWon = len(bv.listeners), bv.terms
	res.ShutdownLogged = countSince(old, begun, "shutdown request") > 0
	res.CancelledSyncWaits = countSince(old, begun, "canceling wait for synchronous replication")
	res.NewPrimaryCancelledSyncWaits = countSince(newLog, crashed, "canceling wait for synchronous replication")
	if standbys != nil {
		samples := standbys()
		writeJSON(t, filepath.Join(dir, "standbys.json"), samples)
		if at, ok := firstStandby(samples); ok {
			res.FirstStandbyAfter = since(at)
		}
		// The old primary's last answers are in flight just after the kill,
		// and a promotion takes seconds, so whatever was answered from a
		// second after the crash to the window's end came from the new
		// primary while it had no synchronous standby. Worker lines carry the
		// kind node's clock, so their span ends half a second early.
		edge := crashed.Add(time.Second)
		if names, to, ok := unconfirmedWindow(samples); ok {
			res.WithoutStandbySeen, res.WithoutStandbyNames = true, names
			res.AnsweredWithoutStandby, res.DedupedWithoutStandby = p.answeredBetween(edge, to)
			res.ConfirmedWithoutStandby = confirmedBetween(wlogs, edge, to.Add(-500*time.Millisecond))
		}
	}

	// Evidence first, so a failed requirement below still leaves it.
	writeJSON(t, filepath.Join(dir, "failover.json"), res)
	writeJSON(t, filepath.Join(logs, "report.json"), rep)
	if len(dur.Lost) > 0 {
		writeJSON(t, filepath.Join(dir, "lost.json"), dur.Lost)
	}
	t.Logf("run %s (synchronous_standby_names %q, replication paused %s): crashed %s; %s promoted after %.1fs, routed after %.1fs; "+
		"enqueues failing from %.1fs, answered again at %.1fs; %d acknowledged, %d lost (%d answered only as deduplicated); "+
		"%d jobs, %d completed, repeats %v, %d attempt problems; brokers UNAVAILABLE %v, INTERNAL %v; "+
		"%d/%d listeners reconnected, %d terms won; pending tick %d job(s), %d later ticks; evidence in %s",
		run, names, lag, st.Primary, newPrimary, res.PromotedAfter, res.RoutedAfter, res.FirstFailedAfter, res.RecoveredAfter,
		res.Acknowledged, res.Lost, res.LostDedupOnly, res.Jobs, res.Completed, res.Repeats, res.AttemptProblems,
		res.Unavailable, res.Internal, res.ListenersReconnected, res.Brokers, res.SchedulerTermsWon,
		res.PendingTickJobs, res.LaterTicks, dir)
	t.Logf("after the promotion: first synchronous standby at %.1fs; seen without one: %v (names %q); answered without one %d (%d deduplicated), "+
		"confirmed %d; the new primary cancelled %d waits",
		res.FirstStandbyAfter, res.WithoutStandbySeen, res.WithoutStandbyNames, res.AnsweredWithoutStandby, res.DedupedWithoutStandby,
		res.ConfirmedWithoutStandby, res.NewPrimaryCancelledSyncWaits)

	// What replication doesn't decide.
	require.False(t, res.ShutdownLogged, "the old primary logged a shutdown request: it was stopped, not crashed")
	require.Equal(t, before, after, "a worker or broker restarted")
	require.Equal(t, res.Brokers, res.ListenersReconnected, "every broker's wakeup listener must reconnect to the new primary")
	require.Positive(t, res.SchedulerTermsWon, "no broker won a scheduler term on the new primary")
	require.Equal(t, 1, res.PendingTickJobs, "the tick pending at the crash (%s) must fire exactly once", pending)
	require.Positive(t, res.LaterTicks, "the schedule never advanced past the tick pending at the crash")
	return res, dur, rep
}

// TestCountSinceSkipsEarlierRuns needs no cluster: the old primary's log runs
// from its pod's start, which can be a run ago, when it was promoted.
func TestCountSinceSkipsEarlierRuns(t *testing.T) {
	since := time.Date(2026, 11, 23, 6, 41, 15, 0, time.UTC)
	log := `{"ts":"2026-11-23T06:40:52.882696758Z","record":{"message":"canceling wait for synchronous replication due to user request"}}
{"ts":"2026-11-23T06:41:30.000000001Z","record":{"message":"canceling wait for synchronous replication due to user request"}}
{"ts":"2026-11-23T06:41:31Z","record":{"message":"checkpoint starting"}}
not json, but canceling wait for synchronous replication
`
	require.Equal(t, 2, countSince(log, since, "canceling wait for synchronous replication"),
		"the line before since is an earlier run's; a line with no time can't be placed, so it counts")
}

// brokerView is what the brokers logged from a time on.
type brokerView struct {
	unavailable, internal map[string]int  // answers by RPC: "rpc unavailable" and "rpc failed"
	listeners             map[string]bool // pods that logged "wakeup listener reconnected"
	terms                 int             // "scheduler term won" lines
}

// readBrokerView reads every captured broker log in dir and counts the lines
// logged at or after since. Brokers outlive runs, so their logs hold earlier
// runs' lines too, and since must be this run's start. The pods' clock is the
// Docker VM's, which tracks the host's.
func readBrokerView(t *testing.T, dir string, since time.Time) brokerView {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	v := brokerView{unavailable: map[string]int{}, internal: map[string]int{}, listeners: map[string]bool{}}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		pod := strings.TrimSuffix(filepath.Base(path), ".log")
		for _, line := range bytes.Split(raw, []byte("\n")) {
			var e struct {
				Time time.Time `json:"time"`
				Msg  string    `json:"msg"`
				RPC  string    `json:"rpc"`
			}
			if json.Unmarshal(bytes.TrimSpace(line), &e) != nil || e.Time.Before(since) {
				continue
			}
			switch e.Msg {
			case "rpc unavailable":
				v.unavailable[e.RPC]++
			case "rpc failed":
				v.internal[e.RPC]++
			case "wakeup listener reconnected":
				v.listeners[pod] = true
			case "scheduler term won":
				v.terms++
			}
		}
	}
	return v
}

// countSince counts the lines of an instance's log that hold s and were logged
// at or after since. The old primary's log runs from its pod's start, and the
// pod may have been promoted in an earlier run, so its lines from before since
// are that run's. A line with no time can't be placed, so it counts.
func countSince(log string, since time.Time, s string) int {
	n := 0
	for line := range strings.Lines(log) {
		if !strings.Contains(line, s) {
			continue
		}
		var e struct {
			TS time.Time `json:"ts"`
		}
		if json.Unmarshal([]byte(line), &e) != nil || !e.TS.Before(since) {
			n++
		}
	}
	return n
}

// restarts sums the restart counts of every container in the pods matching
// selector.
func restarts(c *cluster, selector string) int {
	c.t.Helper()
	n := 0
	for _, f := range strings.Fields(c.run("get", "pods", "-l", selector, "-o",
		"jsonpath={.items[*].status.containerStatuses[*].restartCount}")) {
		v, err := strconv.Atoi(f)
		require.NoError(c.t, err)
		n += v
	}
	return n
}

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// TestUnconfirmedWindowEndsAtTheFirstStandby needs no cluster.
func TestUnconfirmedWindowEndsAtTheFirstStandby(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(int64(1000+s), 0) }
	samples := []standbySample{
		{At: at(1), Pod: "db-2"},                                    // still a standby
		{At: at(2), Pod: "db-3", Err: "exec failed"},                // no answer
		{At: at(3), Pod: "db-2", Primary: true, Names: "ANY 1 (a)"}, // promoted, nothing streaming
		{At: at(4), Pod: "db-2", Primary: true, Names: "ANY 1 (b)"},
		{At: at(5), Pod: "db-2", Primary: true, Sync: 1}, // a standby streams
		{At: at(6), Pod: "db-2", Primary: true},          // a later gap is not the window
	}
	names, to, ok := unconfirmedWindow(samples)
	require.True(t, ok)
	require.Equal(t, at(4), to)
	require.Equal(t, "ANY 1 (b)", names)
	first, ok := firstStandby(samples)
	require.True(t, ok)
	require.Equal(t, at(5), first)

	_, _, ok = unconfirmedWindow(samples[4:])
	require.False(t, ok, "a standby streaming at the first primary sample leaves no window seen")
}

// TestConfirmedBetweenCountsOnlyConfirmations needs no cluster.
func TestConfirmedBetweenCountsOnlyConfirmations(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(int64(1000+s), 0) }
	logs := []accounting.Log{{Pod: "w", Lines: []accounting.Line{
		{Time: at(1), Msg: "job confirmed"},
		{Time: at(2), Msg: "job confirmed"},
		{Time: at(2), Msg: "handler finished"},
		{Time: at(3), Msg: "job confirmed"},
		{Time: at(4), Msg: "job confirmed"},
	}}}
	require.Equal(t, 2, confirmedBetween(logs, at(1), at(3)), "(from, to]: the line at from is out, the line at to is in")
}

// unconfirmedWindow finds, in samples sorted by time, the last sample that
// found the new primary with no synchronous standby streaming to it before
// any sample found one, and the synchronous_standby_names it reported. ok is
// false when no sample caught the new primary without one.
func unconfirmedWindow(samples []standbySample) (names string, to time.Time, ok bool) {
	for _, s := range samples {
		if s.Err != "" || !s.Primary {
			continue
		}
		if s.Sync > 0 {
			break
		}
		names, to, ok = s.Names, s.At, true
	}
	return names, to, ok
}

// firstStandby is when a sample first found a primary with a synchronous
// standby streaming to it.
func firstStandby(samples []standbySample) (time.Time, bool) {
	for _, s := range samples {
		if s.Err == "" && s.Primary && s.Sync > 0 {
			return s.At, true
		}
	}
	return time.Time{}, false
}

// confirmedBetween counts the "job confirmed" lines the workers logged in
// (from, to]. The SDK logs one only after the broker answered the Ack or
// Nack.
func confirmedBetween(logs []accounting.Log, from, to time.Time) int {
	n := 0
	for _, l := range logs {
		for _, line := range l.Lines {
			if line.Msg == "job confirmed" && line.Time.After(from) && !line.Time.After(to) {
				n++
			}
		}
	}
	return n
}
