//go:build k8s

package k8s

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

type cluster struct {
	t                           *testing.T
	context, namespace, release string
}

func (c *cluster) cmd(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, "kubectl", append([]string{"--context", c.context, "-n", c.namespace}, args...)...)
}

// run runs kubectl to completion and returns its trimmed stdout.
func (c *cluster) run(args ...string) string {
	c.t.Helper()
	out, err := c.cmd(context.Background(), args...).Output()
	var stderr []byte
	if ee, ok := err.(*exec.ExitError); ok {
		stderr = ee.Stderr
	}
	require.NoError(c.t, err, "kubectl %s: %s", strings.Join(args, " "), stderr)
	return strings.TrimSpace(string(out))
}

var forwarding = regexp.MustCompile(`^Forwarding from 127\.0\.0\.1:(\d+)`)

// forward port-forwards a free local port to target's remote port for the
// rest of the test, and returns the local address.
func (c *cluster) forward(target, remotePort string) string {
	c.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := c.cmd(ctx, "port-forward", target, ":"+remotePort)
	stdout, err := cmd.StdoutPipe()
	require.NoError(c.t, err)
	require.NoError(c.t, cmd.Start())
	c.t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	lines := bufio.NewScanner(stdout)
	for lines.Scan() {
		if m := forwarding.FindStringSubmatch(lines.Text()); m != nil {
			// kubectl prints a line per connection; keep reading so it never
			// blocks on a full pipe.
			go func() {
				for lines.Scan() {
				}
			}()
			return "127.0.0.1:" + m[1]
		}
	}
	c.t.Fatalf("port-forward %s printed no local address: %v", target, lines.Err())
	return ""
}

// database connects through a port-forward, using the broker's own DSN with
// the password from the chart's Secret.
func (c *cluster) database() *pgxpool.Pool {
	c.t.Helper()
	dsn := c.run("get", "configmap", c.release+"-broker", "-o", "jsonpath={.data.HOPPER_DSN}")
	encoded := c.run("get", "secret", c.release+"-postgres", "-o", "jsonpath={.data.password}")
	password, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(c.t, err)
	u, err := url.Parse(dsn)
	require.NoError(c.t, err)
	u.User = url.UserPassword(u.User.Username(), string(password))
	u.Host = c.forward("svc/"+c.release+"-postgres", "5432")
	pool, err := pgxpool.New(context.Background(), u.String())
	require.NoError(c.t, err)
	c.t.Cleanup(pool.Close)
	require.NoError(c.t, pool.Ping(context.Background()))
	return pool
}

// capture follows the log of every pod matching selector, including pods that
// appear after it starts, one file per pod. A rollout deletes each old pod as
// soon as its container exits, and the log goes with it, so what old workers
// did can only be read while they run.
type capture struct {
	c             *cluster
	selector, dir string

	cancel  context.CancelFunc
	polling chan struct{}  // closed when poll returns; after that nothing calls follow
	wg      sync.WaitGroup // one per follower

	mu   sync.Mutex
	done map[string]chan struct{} // closed when that pod's follower returns
	errs []error                  // followers that gave up before their pod's log ended
}

func startCapture(c *cluster, selector, dir string) *capture {
	ctx, cancel := context.WithCancel(context.Background())
	cp := &capture{c: c, selector: selector, dir: dir, cancel: cancel,
		polling: make(chan struct{}), done: map[string]chan struct{}{}}
	go func() {
		defer close(cp.polling)
		cp.poll(ctx)
	}()
	return cp
}

// stop ends the capture and reports every follower that gave up before its
// pod's log ended. The poller exits before stop waits on the followers, so no
// follower can be added during the wait. A second call is harmless.
func (cp *capture) stop() error {
	cp.cancel()
	<-cp.polling
	cp.wg.Wait()
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return errors.Join(cp.errs...)
}

// poll starts a follower for each matching pod it has not seen. A failed
// listing is retried on the next tick. The test proves the old pods attached
// before any work existed, and snapshot re-reads the new ones whole.
func (cp *capture) poll(ctx context.Context) {
	for {
		if out, err := cp.c.cmd(ctx, "get", "pods", "-l", cp.selector,
			"-o", "jsonpath={.items[*].metadata.name}").Output(); err == nil {
			for _, pod := range strings.Fields(string(out)) {
				cp.follow(ctx, pod)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// follow is called only from poll, which is how stop can know that no Add
// races its Wait.
func (cp *capture) follow(ctx context.Context, pod string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if _, ok := cp.done[pod]; ok {
		return
	}
	done := make(chan struct{})
	cp.done[pod] = done
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		defer close(done)
		if err := cp.followToEnd(ctx, pod); err != nil && ctx.Err() == nil {
			cp.mu.Lock()
			cp.errs = append(cp.errs, fmt.Errorf("pod %s: %w", pod, err))
			cp.mu.Unlock()
		}
	}()
}

// followToEnd streams pod's log into its file until the log has ended: the
// container has exited, or the pod is gone after a stream that ran until the
// container exited. Anything else is retried: a pod still starting, a broken
// stream, a failed status check, or a stream that closed before the pod's
// status caught up with its exit. A pod deleted while its stream was broken
// is an error, because the lines after the break are gone.
func (cp *capture) followToEnd(ctx context.Context, pod string) error {
	path := filepath.Join(cp.dir, pod+".log")
	clean := false // some attach streamed until the container exited
	for {
		w := &lazyFile{path: path}
		cmd := cp.c.cmd(ctx, "logs", "--follow", "pod/"+pod)
		cmd.Stdout = w
		streamErr := cmd.Run()
		if err := w.Close(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if streamErr == nil {
			clean = true
		}
		// Prints nothing once the pod is gone, its name while the container
		// runs, and its name and exit code once the container has exited.
		out, err := cp.c.cmd(ctx, "get", "pod/"+pod, "--ignore-not-found", "-o",
			"jsonpath={.metadata.name} {.status.containerStatuses[0].state.terminated.exitCode}").Output()
		if err == nil {
			fields := strings.Fields(string(out))
			gone, exited := len(fields) == 0, len(fields) == 2
			switch {
			case clean && (gone || exited):
				return nil
			case gone:
				return fmt.Errorf("deleted before its log was read to the end (last stream error: %v)", streamErr)
			}
			// Still starting or running, or exited after a broken stream, in
			// which case the next attach re-reads the whole log.
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// lazyFile creates, and so truncates, its file on the first write. Every
// attach replays the log from the start, so an attach that streams anything
// rewrites the whole capture, and one that fails before streaming anything
// (the pod is gone, or has not started) leaves the previous capture intact.
type lazyFile struct {
	path string
	f    *os.File
	err  error
}

func (l *lazyFile) Write(p []byte) (int, error) {
	if l.f == nil && l.err == nil {
		l.f, l.err = os.Create(l.path)
	}
	if l.err != nil {
		return 0, l.err
	}
	n, err := l.f.Write(p)
	if err != nil {
		l.err = err
	}
	return n, err
}

// Close reports the first error the file saw.
func (l *lazyFile) Close() error {
	if l.f != nil {
		if err := l.f.Close(); l.err == nil {
			l.err = err
		}
	}
	return l.err
}

// attached reports whether every one of pods has a follower whose stream has
// delivered output. Each worker logs "worker starting" as it starts, so an
// empty file means kubectl has not attached yet.
func (cp *capture) attached(pods []string) bool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for _, pod := range pods {
		if _, ok := cp.done[pod]; !ok {
			return false
		}
		if fi, err := os.Stat(filepath.Join(cp.dir, pod+".log")); err != nil || fi.Size() == 0 {
			return false
		}
	}
	return true
}

// ended waits until the followers of pods have returned, which happens when
// their containers exit. kubectl rollout status can return before that. A
// follower that gave up returns too, and stop reports why.
func (cp *capture) ended(pods []string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for _, pod := range pods {
		cp.mu.Lock()
		done, ok := cp.done[pod]
		cp.mu.Unlock()
		if !ok {
			return fmt.Errorf("pod %s was never followed", pod)
		}
		select {
		case <-done:
		case <-deadline:
			return fmt.Errorf("pod %s is still running", pod)
		}
	}
	return nil
}

// snapshot re-reads the whole log of pods that are still running, so a line
// still in flight on a follow stream when the capture stopped is not lost.
func (cp *capture) snapshot(pods []string) {
	for _, pod := range pods {
		out := cp.c.run("logs", "pod/"+pod)
		require.NoError(cp.c.t, os.WriteFile(filepath.Join(cp.dir, pod+".log"), []byte(out+"\n"), 0o644))
	}
}

func TestRolloutRestartDuplicatesNothing(t *testing.T) {
	c := &cluster{t: t,
		context:   setting("HOPPER_K8S_CONTEXT", "kind-hopper"),
		namespace: setting("HOPPER_K8S_NAMESPACE", "hopper"),
		release:   setting("HOPPER_K8S_RELEASE", "hopper")}
	workers := intSetting(t, "HOPPER_PROOF_WORKERS", 3)
	jobs := intSetting(t, "HOPPER_PROOF_JOBS", 1200)
	ms := intSetting(t, "HOPPER_PROOF_JOB_MS", 3000)
	run := fmt.Sprintf("s8-%d", time.Now().UnixNano())
	dir := filepath.Join(setting("HOPPER_PROOF_DIR", t.TempDir()), run)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	deployment := "deployment/" + c.release + "-worker"
	selector := "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=worker"
	// pods lists the worker pods not being deleted. rollout status can return
	// while a previous run's replaced pods are still terminating.
	pods := func() []string {
		var live []string
		out := c.run("get", "pods", "-l", selector, "-o",
			`jsonpath={range .items[*]}{.metadata.name} {.metadata.deletionTimestamp}{"\n"}{end}`)
		for _, line := range strings.Split(out, "\n") {
			if f := strings.Fields(line); len(f) == 1 {
				live = append(live, f[0])
			}
		}
		return live
	}
	bg := context.Background()

	// Start settled, so every pod listed now is one the restart replaces.
	c.run("rollout", "status", deployment, "--timeout=2m")
	oldPods := pods()
	require.Len(t, oldPods, workers, "the proof needs %d workers: scripts/kind-up.sh --set worker.replicas=%d", workers, workers)
	concurrency := c.run("get", "configmap", c.release+"-worker", "-o", "jsonpath={.data.HOPPER_CONCURRENCY}")

	cp := startCapture(c, selector, dir)
	t.Cleanup(func() { _ = cp.stop() })
	// Every old pod must be streaming before any work exists, or the proof
	// could miss what it did. An attach replays the log from the start.
	require.Eventually(t, func() bool { return cp.attached(oldPods) }, time.Minute, 200*time.Millisecond,
		"the old workers' logs never attached")

	pool := c.database()
	counts := func() (completed, finished int) {
		err := pool.QueryRow(bg, `select count(*) filter (where state = 'completed'),
		                                 count(*) filter (where state in ('completed','dead','cancelled'))
		                          from jobs where idempotency_key like $1 || '-%'`, run).Scan(&completed, &finished)
		if err != nil {
			t.Logf("count: %v", err)
		}
		return completed, finished
	}

	cl, err := client.Dial(c.forward("svc/"+c.release+"-broker", "9090"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })
	payload := json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms))
	for start := 0; start < jobs; start += 500 {
		batch := make([]client.Job, 0, 500)
		for i := start; i < min(start+500, jobs); i++ {
			batch = append(batch, client.Job{Queue: "sleep", Payload: payload,
				IdempotencyKey: fmt.Sprintf("%s-%d", run, i)})
		}
		results, err := cl.EnqueueBatch(bg, batch)
		require.NoError(t, err)
		for _, r := range results {
			require.False(t, r.Deduplicated, "run %s is not fresh", run)
		}
	}

	// Restart once the workers are saturated, so SIGTERM lands while handlers run.
	require.Eventually(t, func() bool { done, _ := counts(); return done >= 50 }, 2*time.Minute, 500*time.Millisecond)
	c.run("rollout", "restart", deployment)
	c.run("rollout", "status", deployment, "--timeout=5m")

	// An attempt lost to shutdown returns after a Nack's backoff or, if its
	// delivery was never received, after the lease expires and the reaper
	// takes it back.
	require.Eventually(t, func() bool { _, done := counts(); return done == jobs }, 10*time.Minute, 2*time.Second)
	require.NoError(t, cp.ended(oldPods, 3*time.Minute))
	require.NoError(t, cp.stop(), "log capture")
	newPods := pods()
	require.Len(t, newPods, workers, "live workers after the rollout")
	for _, pod := range newPods {
		require.NotContains(t, oldPods, pod, "pod %s survived the rollout", pod)
	}
	cp.snapshot(newPods)

	rows := loadRows(t, pool, run)
	logs := readLogs(t, dir)
	rep := accounting.Reconcile(rows, logs)
	writeJSON(t, filepath.Join(dir, "report.json"), rep)

	for _, pod := range oldPods {
		s, ok := shutdownOf(rep, pod)
		require.True(t, ok, "replaced pod %s never logged a shutdown", pod)
		require.Positive(t, s.RunningHandlers, "replaced pod %s was idle at SIGTERM; the proof would be vacuous", pod)
	}
	for _, n := range strings.Fields(c.run("get", "pods", "-l", selector,
		"-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}")) {
		require.Equal(t, "0", n, "a new worker restarted")
	}
	t.Logf("run %s: %d jobs of %dms on %d workers × concurrency %s, %d completed; "+
		"%d jobs redelivered, %d redeliveries (%d unstarted nacks, %d never received, inferred); "+
		"replaced %v with %v; logs in %s",
		run, rep.Jobs, ms, workers, concurrency, rep.Completed,
		len(rep.Extras), rep.ExtraLeases, rep.UnstartedNacks, rep.NeverReceived,
		oldPods, newPods, dir)
	for _, s := range rep.Shutdowns {
		t.Logf("  %s: running_handlers=%d stopped=%v", s.Pod, s.RunningHandlers, s.Stopped)
	}
	require.Empty(t, rep.Problems())
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

// loadRows reads the run's jobs as the database left them.
func loadRows(t *testing.T, pool *pgxpool.Pool, run string) []accounting.Row {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`select id, state::text, attempts from jobs where idempotency_key like $1 || '-%'`, run)
	require.NoError(t, err)
	defer rows.Close()
	var out []accounting.Row
	for rows.Next() {
		var r accounting.Row
		require.NoError(t, rows.Scan(&r.ID, &r.State, &r.Attempts))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// readLogs parses every captured pod log in dir, named after its pod.
func readLogs(t *testing.T, dir string) []accounting.Log {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	var logs []accounting.Log
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)
		l, err := accounting.Parse(strings.TrimSuffix(filepath.Base(path), ".log"), f)
		require.NoError(t, errors.Join(err, f.Close()), path)
		logs = append(logs, l)
	}
	return logs
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(b, '\n'), 0o644))
}

// shutdownOf finds what pod logged about its own shutdown.
func shutdownOf(rep accounting.Report, pod string) (accounting.Shutdown, bool) {
	for _, s := range rep.Shutdowns {
		if s.Pod == pod {
			return s, true
		}
	}
	return accounting.Shutdown{}, false
}
