//go:build conformance

package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

// grant is the lease every conformance stack grants: Compose's
// HOPPER_LEASE_DURATION (docker-compose.yml), docs/design/conformance.md.
const grant = 30 * time.Second

// conf is a chaos stack with the helpers the conformance scenarios share. Its
// methods that an eventually condition may call never fail the test; the rest
// fail it on s.t, which step points at the running subtest.
type conf struct {
	*stack
	// services are the Compose services whose containers are the workers under
	// test: worker, and worker-go once a mixed fleet adds it.
	services []string
	// hostnames caches each container's hostname, which a worker's id is built on.
	hostnames map[string]string
}

// conform brings up a fresh stack, and waits until a broker has accepted each
// worker's subscription, not merely until the worker says it opened one: the
// Java SDK logs that line when it hands the subscription to grpc-java, which
// can be before any connection exists (docs/design/conformance.md).
func conform(t *testing.T, name string, env ...string) *conf {
	t.Helper()
	c := &conf{stack: up(t, name, env...), services: []string{"worker"}, hostnames: map[string]string{}}
	for _, w := range c.workers() {
		c.awaitAccepted(w, 0)
	}
	return c
}

// step runs f as a subtest with the stack's t pointed at the subtest's, and
// restores the parent's after. The stack's methods call require on s.t, and a
// parent's FailNow from a subtest's goroutine is illegal. Subtests never run in
// parallel, so nothing else reads s.t meanwhile.
func (c *conf) step(t *testing.T, name string, f func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		parent := c.t
		c.t = t
		defer func() { c.t = parent }()
		f(t)
	})
}

// scale runs n containers of service and waits until a broker has accepted
// each one.
func (c *conf) scale(service string, n int) {
	c.t.Helper()
	c.compose(upLimit, "up", "-d", "--no-build", "--no-recreate", "--wait",
		"--scale", fmt.Sprintf("%s=%d", service, n), service)
	for _, w := range c.containers(service) {
		c.awaitAccepted(w, 0)
	}
}

// addGoWorkers adds n Go workers beside the worker service, for the mixed
// fleet. worker-go exists only when HOPPER_CONFORMANCE_WORKER names an image.
func (c *conf) addGoWorkers(n int) {
	c.t.Helper()
	require.NotEmpty(c.t, workerImage, "worker-go exists only beside a named worker image")
	if !slices.Contains(c.services, "worker-go") {
		c.services = append(c.services, "worker-go")
	}
	c.scale("worker-go", n)
}

// workers are the containers of every worker service, sorted.
func (c *conf) workers() []string {
	c.t.Helper()
	var all []string
	for _, svc := range c.services {
		all = append(all, c.containers(svc)...)
	}
	slices.Sort(all)
	return all
}

// names lists a service's containers without failing the test.
func (c *conf) names(service string) []string {
	ctx, cancel := c.within(quickLimit)
	defer cancel()
	out, err := c.output(ctx, "ps", "-a", "--filter", "label=com.docker.compose.project="+project,
		"--filter", "label=com.docker.compose.service="+service, "--format", "{{.Names}}")
	if err != nil {
		return nil
	}
	names := strings.Fields(out)
	slices.Sort(names)
	return names
}

// streams is `docker logs` with stdout and stderr kept apart. It never fails
// the test.
func (c *conf) streams(ctx context.Context, name string) (stdout, stderr []byte, err error) {
	cmd := exec.CommandContext(ctx, "docker", "logs", name)
	cmd.WaitDelay = 5 * time.Second
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	err = cmd.Run()
	return o.Bytes(), e.Bytes(), err
}

// workerID is the id a container's worker subscribes with: both images run the
// worker as PID 1, so <hostname>-1. "" when it cannot be read.
func (c *conf) workerID(name string) string {
	if h, ok := c.hostnames[name]; ok {
		return h + "-1"
	}
	ctx, cancel := c.within(quickLimit)
	defer cancel()
	h, err := c.output(ctx, "inspect", "--format", "{{.Config.Hostname}}", name)
	if err != nil || h == "" {
		return ""
	}
	c.hostnames[name] = h
	return h + "-1"
}

// acceptedCount is how many times a broker logged "lease stream opened" for
// the container's worker id. It never fails the test.
func (c *conf) acceptedCount(name string) int {
	id := c.workerID(name)
	if id == "" {
		return 0
	}
	n := 0
	for _, svc := range []string{"broker", "broker-standby"} {
		for _, b := range c.names(svc) {
			ctx, cancel := c.within(logsLimit)
			raw, err := c.rawLog(ctx, b)
			cancel()
			if err != nil {
				continue
			}
			for _, line := range bytes.Split(raw, []byte("\n")) {
				var e struct {
					Msg      string `json:"msg"`
					WorkerID string `json:"worker_id"`
				}
				if json.Unmarshal(bytes.TrimSpace(line), &e) == nil && e.Msg == "lease stream opened" && e.WorkerID == id {
					n++
				}
			}
		}
	}
	return n
}

// awaitAccepted waits until a broker has logged more "lease stream opened"
// lines for the container's worker than before (D10).
func (c *conf) awaitAccepted(name string, before int) {
	c.t.Helper()
	c.eventually(func() bool { return c.acceptedCount(name) > before },
		time.Minute, 250*time.Millisecond, "no broker accepted %s's subscription (it had %d before)", name, before)
}

// newRun names a run, unique within the stack. Its jobs' idempotency keys are
// <run>-<i>, which is what rows selects them by.
func (c *conf) newRun(name string) string {
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
}

// enqueueJobs adds one job per payload to the run, in one EnqueueBatch, keyed
// <run>-<i>, and returns their ids in order.
func (c *conf) enqueueJobs(run, queue string, payloads []string, maxAttempts int32, priority int16) []uuid.UUID {
	c.t.Helper()
	jobs := make([]client.Job, len(payloads))
	for i, p := range payloads {
		jobs[i] = client.Job{Queue: queue, Payload: json.RawMessage(p), Priority: priority, MaxAttempts: maxAttempts,
			IdempotencyKey: fmt.Sprintf("%s-%d", run, i)}
	}
	ctx, cancel := c.within(rpcLimit)
	defer cancel()
	results, err := c.client(c.brokerAddr).EnqueueBatch(ctx, jobs)
	require.NoError(c.t, err)
	ids := make([]uuid.UUID, len(results))
	for i, r := range results {
		require.False(c.t, r.Deduplicated, "run %s is not fresh", run)
		ids[i] = r.ID
	}
	return ids
}

// cancel cancels a job through the Go client and requires it to report the
// job cancelled.
func (c *conf) cancel(id uuid.UUID) {
	c.t.Helper()
	ctx, cancel := c.within(rpcLimit)
	defer cancel()
	res, err := c.client(c.brokerAddr).Cancel(ctx, id)
	require.NoError(c.t, err)
	require.Equal(c.t, "JOB_STATE_CANCELLED", res.State, "job %s", id)
}

// awaitLeased polls the job's row every 2 ms until its state is leased, so a
// caller can cancel at the instant a broker took the lease (§10). It never
// misses the state for long: the worker's ownership check and start follow it.
func (c *conf) awaitLeased(id uuid.UUID, timeout time.Duration) {
	c.t.Helper()
	c.eventually(func() bool {
		ctx, cancel := c.within(dbLimit)
		defer cancel()
		var state string
		if err := c.db.QueryRow(ctx, `select state::text from jobs where id = $1`, id).Scan(&state); err != nil {
			return false
		}
		return state == "leased"
	}, timeout, 2*time.Millisecond, "job %s never became leased", id)
}

// awaitLine waits until some worker's stdout has a line about the job with one
// of msgs, and returns which message it was and when it was logged (the Docker
// VM's clock).
func (c *conf) awaitLine(id uuid.UUID, msgs []string, timeout time.Duration) (string, time.Time) {
	c.t.Helper()
	var msg string
	var at time.Time
	c.eventually(func() bool {
		for _, w := range c.workerNames() {
			ctx, cancel := c.within(logsLimit)
			out, _, err := c.streams(ctx, w)
			cancel()
			if err != nil {
				continue
			}
			l, _ := accounting.Parse(w, bytes.NewReader(out))
			for _, line := range l.Lines {
				if line.JobID == id.String() && slices.Contains(msgs, line.Msg) {
					msg, at = line.Msg, line.Time
					return true
				}
			}
		}
		return false
	}, timeout, 100*time.Millisecond, "no worker logged any of %q about job %s", msgs, id)
	return msg, at
}

// workerNames is every worker container, without failing the test.
func (c *conf) workerNames() []string {
	var all []string
	for _, svc := range c.services {
		all = append(all, c.names(svc)...)
	}
	slices.Sort(all)
	return all
}

// rows reads the run's jobs as the judges read them.
func (c *conf) rows(run string) []accounting.Row {
	c.t.Helper()
	ctx, cancel := c.within(reportLimit)
	defer cancel()
	rs, err := c.db.Query(ctx, `
		select id, state::text, attempts, lease_id, coalesce(last_error, '')
		from jobs where idempotency_key like $1 || '-%'`, run)
	require.NoError(c.t, err)
	defer rs.Close()
	var got []accounting.Row
	for rs.Next() {
		var r accounting.Row
		var lease *uuid.UUID
		require.NoError(c.t, rs.Scan(&r.ID, &r.State, &r.Attempts, &lease, &r.LastError))
		if lease != nil {
			r.LeaseID = *lease
		}
		got = append(got, r)
	}
	require.NoError(c.t, rs.Err())
	return got
}

// exitCode is the container's last exit code.
func (c *conf) exitCode(name string) int {
	c.t.Helper()
	out := c.run(quickLimit, "inspect", "--format", "{{.State.ExitCode}}", name)
	n, err := strconv.Atoi(out)
	require.NoError(c.t, err, "exit code of %s: %q", name, out)
	return n
}

// finishedAt is when the container's last process ended, on the Docker VM's
// clock (never compare it with the host's).
func (c *conf) finishedAt(name string) time.Time {
	c.t.Helper()
	out := c.run(quickLimit, "inspect", "--format", "{{.State.FinishedAt}}", name)
	at, err := time.Parse(time.RFC3339Nano, out)
	require.NoError(c.t, err, "FinishedAt of %s: %q", name, out)
	return at
}

// evidence captures the run's rows and every worker's two streams.
func (c *conf) evidence(run string, exits map[string]int) accounting.Evidence {
	c.t.Helper()
	ev := accounting.Evidence{Rows: c.rows(run), Stdout: map[string][]byte{}, Stderr: map[string][]byte{}, Exits: exits}
	for _, w := range c.workers() {
		ctx, cancel := c.within(logsLimit)
		out, errOut, err := c.streams(ctx, w)
		cancel()
		require.NoError(c.t, err, "logs of %s", w)
		ev.Stdout[w], ev.Stderr[w] = out, errOut
	}
	return ev
}

// judge captures the run's evidence, judges it against e, and keeps both in
// the scenario's evidence directory: the files accounting.WriteEvidence writes,
// and judgement.txt. A confirmation's log line is written just after its row
// commits, so while problems remain it captures again, once a second for up to
// 15 s. It then requires there to be none, and logs one summary line.
func (c *conf) judge(name, run string, e accounting.Expect, exits map[string]int) {
	c.t.Helper()
	var ev accounting.Evidence
	var problems []string
	for attempt := 0; ; attempt++ {
		ev = c.evidence(run, exits)
		problems = accounting.Judge(e, ev)
		if len(problems) == 0 || attempt >= 15 {
			break
		}
		time.Sleep(time.Second)
	}
	dir := filepath.Join(c.dir, name)
	require.NoError(c.t, accounting.WriteEvidence(dir, e, ev))
	verdict := "judged: no problems\n"
	if len(problems) > 0 {
		verdict = "judged: " + strconv.Itoa(len(problems)) + " problems\n  " + strings.Join(problems, "\n  ") + "\n"
	}
	require.NoError(c.t, os.WriteFile(filepath.Join(dir, "judgement.txt"), []byte(verdict), 0o644))
	c.t.Logf("%s %s: %s (evidence %s)", name, strings.TrimSpace(strings.SplitN(verdict, "\n", 2)[0]), c.summarize(e, ev), dir)
	require.Empty(c.t, problems, "scenario %s: evidence in %s", name, dir)
}

// summarize is the line judge logs: the run's counts, causes and extras, each
// process's peak, and which outcome each cancelled job saw.
func (c *conf) summarize(e accounting.Expect, ev accounting.Evidence) string {
	var logs []accounting.Log
	for _, w := range slices.Sorted(maps.Keys(ev.Stdout)) {
		l, _ := accounting.Parse(w, bytes.NewReader(ev.Stdout[w]))
		logs = append(logs, l)
	}
	s := summary(accounting.Reconcile(ev.Rows, logs))
	peaks, _ := accounting.ConcurrencyProblems(logs)
	var ps []string
	for _, p := range peaks {
		ps = append(ps, fmt.Sprintf("%s#%d %d/%d", p.Pod, p.Process, p.Max, p.Concurrency))
	}
	s += "; peaks " + strings.Join(ps, " ")
	for _, id := range slices.Sorted(maps.Keys(e.Cancels)) {
		outcome, _ := accounting.CancelOutcome(logs, id, e.Grant)
		s += fmt.Sprintf("; cancel outcome of %s: %q", id, outcome)
	}
	return s
}

// openDeliveries is how many deliveries the container's current process has
// open by its log.
func (c *conf) openDeliveries(name string) int {
	c.t.Helper()
	ctx, cancel := c.within(logsLimit)
	defer cancel()
	out, _, err := c.streams(ctx, name)
	require.NoError(c.t, err, "logs of %s", name)
	l, err := accounting.Parse(name, bytes.NewReader(out))
	require.NoError(c.t, err)
	return accounting.OpenDeliveries(l)
}

// startedJobs is how many of the run's jobs some worker has logged "job
// started" for. It never fails the test, so an eventually condition can call it.
func (c *conf) startedJobs(run string) int {
	ctx, cancel := c.within(dbLimit)
	rs, err := c.db.Query(ctx, `select id from jobs where idempotency_key like $1 || '-%'`, run)
	if err != nil {
		cancel()
		return 0
	}
	ids := map[string]bool{}
	for rs.Next() {
		var id uuid.UUID
		if rs.Scan(&id) == nil {
			ids[id.String()] = true
		}
	}
	rs.Close()
	cancel()
	started := map[string]bool{}
	for _, w := range c.workerNames() {
		lctx, lcancel := c.within(logsLimit)
		out, _, err := c.streams(lctx, w)
		lcancel()
		if err != nil {
			continue
		}
		l, _ := accounting.Parse(w, bytes.NewReader(out))
		for _, line := range l.Lines {
			if line.Msg == msgJobStarted && ids[line.JobID] {
				started[line.JobID] = true
			}
		}
	}
	return len(started)
}

// busiest is the container of service with the most deliveries open now.
func (c *conf) busiest(service string) string {
	c.t.Helper()
	best, open := "", -1
	for _, w := range c.containers(service) {
		ctx, cancel := c.within(logsLimit)
		out, _, err := c.streams(ctx, w)
		cancel()
		require.NoError(c.t, err, "logs of %s", w)
		l, err := accounting.Parse(w, bytes.NewReader(out))
		require.NoError(c.t, err)
		if n := accounting.OpenDeliveries(l); n > open {
			best, open = w, n
		}
	}
	return best
}
