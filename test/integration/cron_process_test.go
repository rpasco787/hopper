package integration

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cronProcessFile declares one frequent schedule for the broker-process
// tests. @every 1s is the fastest the parser allows.
const cronProcessFile = `{"schedules": [
	{"name": "proc-tick", "queue": "cron_proc", "cron": "@every 1s", "payload": {"from": "file"}}
]}`

// buildBrokerProcess builds cmd/broker with -race, so a data race in the
// process fails the test through its exit status.
func buildBrokerProcess(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "broker")
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/broker")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build broker process:\n%s", out)
	return binary
}

// dsnWithApplicationName returns the shared database's DSN with every session
// tagged as name, which is how these tests tell which process holds the
// scheduler lock without any hook in production code.
func dsnWithApplicationName(t *testing.T, name string) string {
	t.Helper()
	u, err := url.Parse(pool.Config().ConnConfig.ConnString())
	require.NoError(t, err)
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// startBrokerProcess runs the broker binary with the scheduler on a fast
// interval against cronFile. startWorkerProcess is a generic harness for a
// process that logs JSON to stdout, so the broker reuses it.
func startBrokerProcess(t *testing.T, binary, appName, cronFile string) *workerProcess {
	t.Helper()
	p := startWorkerProcess(t, binary,
		"-dsn", dsnWithApplicationName(t, appName), "-listen", "127.0.0.1:0",
		"-cron-file", cronFile, "-scheduler-interval", "50ms",
		"-shutdown-timeout", "2s", "-log-level", "info")
	p.waitForLog(t, 30*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	return p
}

func writeProcessCronFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schedules.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// cronProcJobAt reports whether a cron_proc job exists for the tick runAt.
func cronProcJobAt(ctx context.Context, runAt time.Time) bool {
	var found bool
	err := pool.QueryRow(ctx,
		"select exists (select 1 from jobs where queue = 'cron_proc' and run_at = $1)", runAt).Scan(&found)
	return err == nil && found
}

// firstCronProcTick returns the earliest cron_proc job's run_at, which is the
// first next_run_at reconciliation wrote, or the zero time if none exists.
func firstCronProcTick(ctx context.Context) time.Time {
	var first *time.Time
	if err := pool.QueryRow(ctx, "select min(run_at) from jobs where queue = 'cron_proc'").Scan(&first); err != nil || first == nil {
		return time.Time{}
	}
	return *first
}

func procNextRunAt(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	var next time.Time
	require.NoError(t, pool.QueryRow(ctx,
		"select next_run_at from cron_schedules where name = 'proc-tick'").Scan(&next))
	return next
}

// TestBrokerProcessRunsCronFromAFile proves the wiring rather than the loop:
// the real binary loads the file, elects itself, reconciles, fires and
// advances, and still shuts down cleanly on SIGTERM.
func TestBrokerProcessRunsCronFromAFile(t *testing.T) {
	binary := buildBrokerProcess(t)
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	p := startBrokerProcess(t, binary, "hopper-broker-solo", writeProcessCronFile(t, cronProcessFile))

	var first time.Time
	require.Eventually(t, func() bool {
		first = firstCronProcTick(ctx)
		return !first.IsZero()
	}, 10*time.Second, 20*time.Millisecond, "the broker must fire the file's schedule\n%s", p.output())

	// Reconciliation computed the first tick from Store.Now through
	// ParseSpec, so an @every 1s schedule lands on the whole-second lattice
	// by construction — a free check that the database clock is wired in.
	require.Zero(t, first.UnixMicro()%1_000_000, "first tick %s is off the whole-second lattice", first)
	require.Eventually(t, func() bool { return procNextRunAt(t, ctx).After(first) },
		5*time.Second, 20*time.Millisecond, "next_run_at must advance past the fired seed tick")

	var payload string
	require.NoError(t, pool.QueryRow(ctx,
		"select payload::text from jobs where queue = 'cron_proc' and run_at = $1", first).Scan(&payload))
	require.JSONEq(t, `{"from":"file"}`, payload)

	p.signal(t, syscall.SIGTERM)
	require.NoError(t, p.wait(t, 10*time.Second), "a clean shutdown exits 0\n%s", p.output())
	holders, err := lockHolders(ctx)
	require.NoError(t, err)
	require.Empty(t, holders, "a clean shutdown releases the scheduler lock")
}

// TestBrokerProcessCronFailsOverOnKill: a SIGKILLed process cannot be
// resurrected, so takeover is unambiguous here in a way it is not in
// TestSchedulerFailover. The assertion is still "the tick pending at the kill
// was fired", not "no occurrence was skipped": the catch-up policy permits a
// slow failover to skip, so the latter is a claim about the machine's speed.
// One round only — TestSchedulerFailover owns the repetition.
func TestBrokerProcessCronFailsOverOnKill(t *testing.T) {
	binary := buildBrokerProcess(t)
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	file := writeProcessCronFile(t, cronProcessFile)
	procs := map[string]*workerProcess{
		"hopper-broker-a": startBrokerProcess(t, binary, "hopper-broker-a", file),
		"hopper-broker-b": startBrokerProcess(t, binary, "hopper-broker-b", file),
	}

	require.Eventually(t, func() bool { return !firstCronProcTick(ctx).IsZero() },
		10*time.Second, 20*time.Millisecond, "one of the brokers must fire the schedule")

	leader := requireLeader(t, ctx).Name
	require.Contains(t, procs, leader)
	survivor := "hopper-broker-a"
	if leader == survivor {
		survivor = "hopper-broker-b"
	}
	pending := procNextRunAt(t, ctx)

	procs[leader].signal(t, syscall.SIGKILL)
	killErr := procs[leader].wait(t, 10*time.Second)
	require.Error(t, killErr, "a SIGKILLed process cannot exit cleanly")
	killedAt := time.Now()

	require.Eventually(t, func() bool {
		return heldBy(ctx, []string{survivor}) && cronProcJobAt(ctx, pending)
	}, 2*failoverBudget, 20*time.Millisecond,
		"%s must take the lock and fire the tick pending at the kill (%s)\n%s",
		survivor, pending, procs[survivor].output())
	t.Logf("killed %s; %s fired tick %s %s after the kill",
		leader, survivor, pending.Format(time.TimeOnly), time.Since(killedAt).Round(time.Millisecond))

	procs[survivor].signal(t, syscall.SIGTERM)
	require.NoError(t, procs[survivor].wait(t, 10*time.Second), "survivor exits cleanly\n%s", procs[survivor].output())
}

// TestBrokerRejectsABadCronFileAtStartup is the CrashLoopBackOff behavior: a
// malformed file kills the process at once, naming the schedule, before it
// ever opens a pool or a listener.
func TestBrokerRejectsABadCronFileAtStartup(t *testing.T) {
	binary := buildBrokerProcess(t)
	bad := writeProcessCronFile(t, `{"schedules": [{"name": "broken-sched", "queue": "q", "cron": "not a cron"}]}`)

	p := startWorkerProcess(t, binary,
		"-dsn", pool.Config().ConnConfig.ConnString(), "-listen", "127.0.0.1:0", "-cron-file", bad)
	err := p.wait(t, 10*time.Second)

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "a bad cron file must exit non-zero\n%s", p.output())
	require.NotZero(t, exitErr.ExitCode())
	require.Contains(t, p.output(), `"broken-sched"`, "the error names the offending schedule")
	for _, e := range p.logSnapshot() {
		require.NotEqual(t, "broker listening", e.Message, "the broker must never listen with a bad cron file")
	}
}
