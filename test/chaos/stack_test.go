//go:build chaos

package chaos

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

// project is the Compose project every test runs in, one test at a time.
const project = "hopper-chaos"

// composeFiles are docker-compose.yml with the suite's changes merged over it.
var composeFiles = []string{"-f", "../../docker-compose.yml", "-f", "compose.yaml"}

// Limits on every external call. Each is generous for its operation. Together
// they make every call return, so a failure reaches the test's cleanup, which
// saves the evidence and removes the stack, instead of the test binary's
// -timeout, which kills the process without running any cleanup.
const (
	quickLimit  = 30 * time.Second // docker ps, inspect, kill
	startLimit  = time.Minute      // docker start
	stopLimit   = 2 * time.Minute  // docker stop and restart; the broker's grace period is 40s
	logsLimit   = time.Minute      // docker logs
	upLimit     = 5 * time.Minute  // docker compose up --wait
	downLimit   = 2 * time.Minute  // docker compose down
	buildLimit  = 20 * time.Minute // docker compose build, once
	dbLimit     = 5 * time.Second  // one query, ping or connection attempt
	reportLimit = time.Minute      // reading a run's rows
	rpcLimit    = 30 * time.Second // one enqueue call
	// cleanupReserve is held back from the test binary's deadline, and is
	// cleanup's own budget for saving logs and downing the stack.
	cleanupReserve = 5 * time.Minute
)

// TestMain builds the broker and worker images from the working tree once, so
// every test runs the code under test rather than an image left over from
// another build.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), buildLimit)
	cmd := exec.CommandContext(ctx, "docker", append(append([]string{"compose", "-p", project}, composeFiles...),
		"build", "broker", "broker-standby", "worker")...)
	// compose.yaml requires its port variables even to build.
	cmd.Env = append(os.Environ(), portEnv("1", "2", "3")...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.WaitDelay = 5 * time.Second
	err := cmd.Run()
	cancel()
	if err != nil {
		log.Fatalf("docker compose build: %v", err)
	}
	os.Exit(m.Run())
}

func portEnv(pg, broker, standby string) []string {
	return []string{"HOPPER_CHAOS_PG_PORT=" + pg, "HOPPER_CHAOS_BROKER_PORT=" + broker, "HOPPER_CHAOS_STANDBY_PORT=" + standby}
}

// stack is one test's Compose stack: PostgreSQL, two brokers and three
// workers, published on loopback ports chosen for it.
type stack struct {
	t *testing.T
	// ctx bounds everything the test does. It ends cleanupReserve before the
	// test binary's deadline, so an operation that runs out of time fails the
	// test while cleanup still has time to run.
	ctx context.Context
	dir string // this test's evidence
	env []string

	dsn                     string
	brokerAddr, standbyAddr string
	db                      *pgxpool.Pool

	mu     sync.Mutex
	events *os.File // every action taken, with its time
}

// up starts a fresh stack for one test and removes it when the test ends,
// saving every container's log into the evidence directory first, pass or
// fail.
func up(t *testing.T, name string) *stack {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Hour)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline.Add(-cleanupReserve))
	t.Cleanup(cancel)
	dir := filepath.Join(setting("HOPPER_CHAOS_DIR", t.TempDir()), fmt.Sprintf("%s-%d", name, time.Now().UnixNano()))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	events, err := os.Create(filepath.Join(dir, "events.csv"))
	require.NoError(t, err)
	fmt.Fprintln(events, "time,action,container")
	pg, broker, standby := freePort(t), freePort(t), freePort(t)
	s := &stack{t: t, ctx: ctx, dir: dir, env: portEnv(pg, broker, standby), events: events,
		dsn:        fmt.Sprintf("postgres://hopper:hopper@127.0.0.1:%s/hopper?sslmode=disable&connect_timeout=5", pg),
		brokerAddr: "127.0.0.1:" + broker, standbyAddr: "127.0.0.1:" + standby}

	// A previous run that was interrupted may have left the project behind.
	s.compose(downLimit, "down", "-v", "--remove-orphans")
	t.Cleanup(func() {
		// Its own budget: the test's may already be spent.
		ctx, cancel := context.WithTimeout(context.Background(), cleanupReserve)
		defer cancel()
		s.saveLogs(ctx)
		if _, err := s.composeOutput(ctx, "down", "-v", "--remove-orphans", "-t", "10"); err != nil {
			t.Errorf("docker compose down: %v", err)
		}
		_ = events.Close()
		t.Logf("evidence in %s", dir)
	})
	s.compose(upLimit, "up", "-d", "--no-build", "--wait", "--scale", "worker=3",
		"postgres", "broker", "broker-standby", "worker")

	s.db, err = pgxpool.New(ctx, s.dsn)
	require.NoError(t, err)
	t.Cleanup(s.db.Close)
	for _, addr := range []string{s.brokerAddr, s.standbyAddr} {
		cl := s.client(addr)
		s.eventually(func() bool {
			ctx, cancel := s.within(2 * time.Second)
			defer cancel()
			_, err := cl.GetJob(ctx, uuid.New())
			return errors.Is(err, client.ErrNotFound)
		}, time.Minute, 200*time.Millisecond, "broker %s never served", addr)
	}
	for _, w := range s.containers("worker") {
		s.eventually(func() bool { return s.logged(w, "lease stream opened") > 0 },
			time.Minute, 200*time.Millisecond, "%s never opened a lease stream", w)
	}
	return s
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// within bounds one operation by limit and by the test's budget.
func (s *stack) within(limit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.ctx, limit)
}

// eventually is require.Eventually with its timeout cut to what is left of
// the test's budget. Every condition must itself be bounded: Eventually stops
// waiting at its timeout, but cannot stop a condition that never returns.
func (s *stack) eventually(cond func() bool, timeout, tick time.Duration, msg string, args ...any) {
	s.t.Helper()
	if d, ok := s.ctx.Deadline(); ok {
		timeout = max(min(timeout, time.Until(d)), tick)
	}
	require.Eventually(s.t, cond, timeout, tick, append([]any{msg}, args...)...)
}

// output runs the docker CLI under ctx with the stack's port variables and
// returns its trimmed stdout. It never fails the test, so conditions and
// cleanup can call it.
func (s *stack) output(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = append(os.Environ(), s.env...)
	cmd.WaitDelay = 5 * time.Second
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		err = fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(stderr.Bytes()))
	}
	return strings.TrimSpace(string(out)), err
}

// run is output within limit, failing the test on an error.
func (s *stack) run(limit time.Duration, args ...string) string {
	s.t.Helper()
	ctx, cancel := s.within(limit)
	defer cancel()
	out, err := s.output(ctx, args...)
	require.NoError(s.t, err)
	return out
}

func (s *stack) composeOutput(ctx context.Context, args ...string) (string, error) {
	return s.output(ctx, append(append([]string{"compose", "-p", project}, composeFiles...), args...)...)
}

func (s *stack) compose(limit time.Duration, args ...string) string {
	s.t.Helper()
	ctx, cancel := s.within(limit)
	defer cancel()
	out, err := s.composeOutput(ctx, args...)
	require.NoError(s.t, err)
	return out
}

// containers lists a service's containers by name, sorted, stopped ones
// included.
func (s *stack) containers(service string) []string {
	s.t.Helper()
	out := s.run(quickLimit, "ps", "-a", "--filter", "label=com.docker.compose.project="+project,
		"--filter", "label=com.docker.compose.service="+service, "--format", "{{.Names}}")
	names := strings.Fields(out)
	slices.Sort(names)
	require.NotEmpty(s.t, names, "no %s containers", service)
	return names
}

// act runs one disruptive docker command and records it in events.csv.
func (s *stack) act(limit time.Duration, action, container string, args ...string) {
	s.t.Helper()
	s.run(limit, args...)
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.events, "%s,%s,%s\n", time.Now().UTC().Format(time.RFC3339Nano), action, container)
}

// kill ends a container's process with SIGKILL: no drain, no clean stop.
func (s *stack) kill(c string) { s.act(quickLimit, "kill", c, "kill", "--signal", "KILL", c) }

// start starts a stopped container again in place: same name, same log.
func (s *stack) start(c string) { s.act(startLimit, "start", c, "start", c) }

// stop stops a container gracefully: its image's stop signal, then SIGKILL
// after its grace period.
func (s *stack) stop(c string) { s.act(stopLimit, "stop", c, "stop", c) }

// restart is stop, then start.
func (s *stack) restart(c string) { s.act(stopLimit, "restart", c, "restart", c) }

// startedAt is when the container's current process started.
func (s *stack) startedAt(c string) string {
	s.t.Helper()
	return s.run(quickLimit, "inspect", "--format", "{{.State.StartedAt}}", c)
}

// rawLog is a container's whole log, stdout and stderr, across every process
// it has run.
func (s *stack) rawLog(ctx context.Context, c string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", "logs", c)
	cmd.WaitDelay = 5 * time.Second
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.Bytes(), err
}

// entry is the part of a hopper log line the suite reads outside accounting.
type entry struct {
	Time    time.Time `json:"time"`
	Msg     string    `json:"msg"`
	RPC     string    `json:"rpc"`
	JobID   string    `json:"job_id"`
	LeaseID string    `json:"lease_id"`
}

// entries parses a container's log, skipping anything that is not a JSON
// record. It never fails the test, so conditions can call it.
func (s *stack) entries(c string) ([]entry, error) {
	ctx, cancel := s.within(logsLimit)
	defer cancel()
	raw, err := s.rawLog(ctx, c)
	if err != nil {
		return nil, err
	}
	var out []entry
	for _, line := range bytes.Split(raw, []byte("\n")) {
		var e entry
		if json.Unmarshal(bytes.TrimSpace(line), &e) == nil && e.Msg != "" {
			out = append(out, e)
		}
	}
	return out, nil
}

// logged counts a container's log lines with message msg.
func (s *stack) logged(c, msg string) int {
	es, _ := s.entries(c)
	n := 0
	for _, e := range es {
		if e.Msg == msg {
			n++
		}
	}
	return n
}

// jobsLogged is the set of job ids any of workers logged msg about. It never
// fails the test.
func (s *stack) jobsLogged(workers []string, msg string) map[string]bool {
	seen := map[string]bool{}
	for _, w := range workers {
		es, _ := s.entries(w)
		for _, e := range es {
			if e.Msg == msg {
				seen[e.JobID] = true
			}
		}
	}
	return seen
}

// waitLogged waits until some worker has logged msg about every job in ids.
func (s *stack) waitLogged(msg string, ids []string, timeout time.Duration) {
	s.t.Helper()
	workers := s.containers("worker")
	s.eventually(func() bool {
		seen := s.jobsLogged(workers, msg)
		for _, id := range ids {
			if !seen[id] {
				return false
			}
		}
		return true
	}, timeout, 100*time.Millisecond, "not every gated job logged %q", msg)
}

// saveLogs writes every container's log into the evidence directory.
func (s *stack) saveLogs(ctx context.Context) {
	out, err := s.output(ctx, "ps", "-a", "--filter", "label=com.docker.compose.project="+project, "--format", "{{.Names}}")
	if err != nil {
		s.t.Logf("saving logs: %v", err)
		return
	}
	for _, c := range strings.Fields(out) {
		raw, err := s.rawLog(ctx, c)
		if err != nil {
			s.t.Logf("saving %s's log: %v", c, err)
		}
		_ = os.WriteFile(filepath.Join(s.dir, c+".log"), raw, 0o644)
	}
}

func (s *stack) client(addr string) *client.Client {
	s.t.Helper()
	cl, err := client.Dial(addr)
	require.NoError(s.t, err)
	s.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// enqueue adds a run of n sleep jobs of ms each, keyed <run>-<i>, and returns
// the run's name. max_attempts is high on purpose: what a chaos test measures
// is whether each job completes and how often its handler ran, and a job that
// several kills happened to catch must not dead-letter out of the run.
func (s *stack) enqueue(name string, n, ms int) string {
	s.t.Helper()
	run := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	cl := s.client(s.brokerAddr)
	payload := json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms))
	for start := 0; start < n; start += 500 {
		batch := make([]client.Job, 0, 500)
		for i := start; i < min(start+500, n); i++ {
			batch = append(batch, client.Job{Queue: "sleep", Payload: payload, MaxAttempts: 20,
				IdempotencyKey: fmt.Sprintf("%s-%d", run, i)})
		}
		ctx, cancel := s.within(rpcLimit)
		results, err := cl.EnqueueBatch(ctx, batch)
		cancel()
		require.NoError(s.t, err)
		for _, r := range results {
			require.False(s.t, r.Deduplicated, "run %s is not fresh", run)
		}
	}
	return run
}

// gate enqueues n jobs of ms each into the run, at a priority that puts them
// ahead of its backlog, and waits until every one has started: deliveries sure
// to be running when the test next breaks something. It returns their ids.
func (s *stack) gate(run string, n, ms int) []string {
	s.t.Helper()
	jobs := make([]client.Job, n)
	for i := range jobs {
		jobs[i] = client.Job{Queue: "sleep", Payload: json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms)),
			Priority: 10, MaxAttempts: 20, IdempotencyKey: fmt.Sprintf("%s-gate-%d", run, i)}
	}
	ctx, cancel := s.within(rpcLimit)
	results, err := s.client(s.brokerAddr).EnqueueBatch(ctx, jobs)
	cancel()
	require.NoError(s.t, err)
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID.String()
	}
	s.waitLogged("job started", ids, time.Minute)
	return ids
}

// counts reads how many of the run's jobs completed and how many finished in
// any terminal state. It never fails the test: the database may be down.
func (s *stack) counts(run string) (completed, finished int, err error) {
	ctx, cancel := s.within(dbLimit)
	defer cancel()
	err = s.db.QueryRow(ctx, `
		select count(*) filter (where state = 'completed'),
		       count(*) filter (where state in ('completed', 'dead', 'cancelled'))
		from jobs where idempotency_key like $1 || '-%'`, run).Scan(&completed, &finished)
	return completed, finished, err
}

func (s *stack) waitCompleted(run string, n int, timeout time.Duration) {
	s.t.Helper()
	s.eventually(func() bool {
		completed, _, err := s.counts(run)
		return err == nil && completed >= n
	}, timeout, 500*time.Millisecond, "run %s never reached %d completed jobs", run, n)
}

func (s *stack) waitFinished(run string, n int, timeout time.Duration) {
	s.t.Helper()
	s.eventually(func() bool {
		_, finished, err := s.counts(run)
		return err == nil && finished == n
	}, timeout, time.Second, "run %s never finished all %d jobs", run, n)
}

// waitDatabase waits until the suite's own pool reaches PostgreSQL again.
func (s *stack) waitDatabase() {
	s.t.Helper()
	s.eventually(func() bool {
		ctx, cancel := s.within(dbLimit)
		defer cancel()
		return s.db.Ping(ctx) == nil
	}, 2*time.Minute, 100*time.Millisecond, "PostgreSQL never came back")
}

// probe opens a new PostgreSQL session every 100ms, as a broker reconnecting
// after an outage must, and records when that stopped and started working.
// It measures unreachability from the host's published port, which is close
// to, but not the same as, what the brokers see on the Compose network.
type probe struct {
	recovered chan struct{} // closed after the first successful post-outage sample
	cancel    context.CancelFunc
	done      chan struct{}
	w         probeWindow // written by the probing goroutine; read after done
}

// probeWindow brackets how long new sessions failed. The true interval lies
// between LastFail-FirstFail and Recovered-LastOK; each probe takes up to its
// 1s connect limit on top of the 100ms period.
type probeWindow struct{ LastOK, FirstFail, LastFail, Recovered time.Time }

func (w probeWindow) String() string {
	switch {
	case w.FirstFail.IsZero():
		return "no probe failed"
	case w.Recovered.IsZero():
		return fmt.Sprintf("new sessions failing since %s, no recovery seen", w.FirstFail.Format(time.RFC3339Nano))
	}
	return fmt.Sprintf("new sessions failed for between %s and %s (probed every 100ms)",
		w.LastFail.Sub(w.FirstFail).Round(10*time.Millisecond), w.Recovered.Sub(w.LastOK).Round(10*time.Millisecond))
}

func (s *stack) startProbe() *probe {
	ctx, cancel := context.WithCancel(s.ctx)
	p := &probe{cancel: cancel, done: make(chan struct{}), recovered: make(chan struct{})}
	go func() {
		defer close(p.done)
		signalled := false
		for {
			actx, acancel := context.WithTimeout(ctx, time.Second)
			conn, err := pgx.Connect(actx, s.dsn)
			if err == nil {
				err = conn.Ping(actx)
				_ = conn.Close(actx)
			}
			acancel()
			if ctx.Err() != nil {
				return
			}
			now := time.Now()
			switch {
			case err != nil:
				if p.w.FirstFail.IsZero() {
					p.w.FirstFail = now
				}
				p.w.LastFail, p.w.Recovered = now, time.Time{} // a relapse is the same outage
			case p.w.FirstFail.IsZero():
				p.w.LastOK = now
			case p.w.Recovered.IsZero():
				p.w.Recovered = now
			}
			if !p.w.Recovered.IsZero() && !signalled {
				signalled = true
				close(p.recovered)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return p
}

// stop ends the probe. A probe that saw the outage is given a few seconds to
// take its first successful sample first, since the suite's own pool can
// reconnect before the probe's next tick; one that saw nothing waits the same.
func (p *probe) stop() probeWindow {
	select {
	case <-p.recovered:
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
	p.cancel()
	<-p.done
	return p.w
}

// reconcile matches the run's rows against every worker's whole log, and
// keeps the report with the evidence.
func (s *stack) reconcile(run string) accounting.Report {
	s.t.Helper()
	ctx, cancel := s.within(reportLimit)
	defer cancel()
	rows, err := s.db.Query(ctx,
		`select id, state::text, attempts, lease_id from jobs where idempotency_key like $1 || '-%'`, run)
	require.NoError(s.t, err)
	var got []accounting.Row
	for rows.Next() {
		var r accounting.Row
		var lease *uuid.UUID
		require.NoError(s.t, rows.Scan(&r.ID, &r.State, &r.Attempts, &lease))
		if lease != nil {
			r.LeaseID = *lease
		}
		got = append(got, r)
	}
	require.NoError(s.t, rows.Err())
	var logs []accounting.Log
	for _, w := range s.containers("worker") {
		lctx, lcancel := s.within(logsLimit)
		raw, err := s.rawLog(lctx, w)
		lcancel()
		require.NoError(s.t, err)
		l, err := accounting.Parse(w, bytes.NewReader(raw))
		require.NoError(s.t, err)
		logs = append(logs, l)
	}
	rep := accounting.Reconcile(got, logs)
	b, err := json.MarshalIndent(rep, "", "  ")
	require.NoError(s.t, err)
	require.NoError(s.t, os.WriteFile(filepath.Join(s.dir, "report.json"), append(b, '\n'), 0o644))
	return rep
}

// setting reads an environment variable, falling back to def.
func setting(name, def string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return def
}

func intSetting(t *testing.T, name string, def int) int {
	t.Helper()
	v := setting(name, strconv.Itoa(def))
	n, err := strconv.Atoi(v)
	require.NoError(t, err, "%s=%q", name, v)
	require.Positive(t, n, name)
	return n
}

func durationSetting(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	v := setting(name, def.String())
	d, err := time.ParseDuration(v)
	require.NoError(t, err, "%s=%q", name, v)
	require.Positive(t, d, name)
	return d
}

// summary is the line each test logs about its run.
func summary(rep accounting.Report) string {
	cut, interrupted := 0, 0
	for _, p := range rep.Processes {
		if p.Cut {
			cut++
			interrupted += p.Interrupted
		}
	}
	causes := map[string]int{}
	for _, r := range rep.Repeats {
		causes[cmp.Or(r.Cause, "unexplained")]++
	}
	return fmt.Sprintf("%d jobs, %d completed; %d handler starts repeated %v across %d jobs; %d processes cut, %d handlers interrupted; "+
		"%d redelivered jobs, %d lost leases (%d restarted, %d unstarted nacks, %d never received)",
		rep.Jobs, rep.Completed, len(rep.Repeats), causes, len(rep.Duplicated), cut, interrupted,
		len(rep.Extras), rep.ExtraLeases, rep.Restarted, rep.UnstartedNacks, rep.NeverReceived)
}
