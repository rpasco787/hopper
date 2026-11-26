//go:build k8s

package k8s

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

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

// database connects through a port-forward to the Service the broker's DSN
// names, with the password from the chart's Secret. kubectl port-forward to a
// Service picks one of its pods and stays on it, so a pool from here dies with
// that pod: after a failover, call database again for one that reaches the new
// primary.
func (c *cluster) database() *pgxpool.Pool {
	c.t.Helper()
	dsn := c.run("get", "configmap", c.release+"-broker", "-o", "jsonpath={.data.HOPPER_DSN}")
	encoded := c.run("get", "secret", c.release+"-postgres", "-o", "jsonpath={.data.password}")
	password, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(c.t, err)
	u, err := url.Parse(dsn)
	require.NoError(c.t, err)
	u.User = url.UserPassword(u.User.Username(), string(password))
	u.Host = c.forward("svc/"+u.Hostname(), cmp.Or(u.Port(), "5432"))
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

	// restarts: follow each pod's container across restarts in place, as a
	// SIGKILLed container is restarted, appending every container's log to
	// the pod's file, until the pod is gone (startRestartCapture).
	restarts bool

	cancel  context.CancelFunc
	polling chan struct{}  // closed when poll returns; after that nothing calls follow
	wg      sync.WaitGroup // one per follower

	mu   sync.Mutex
	done map[string]chan struct{} // closed when that pod's follower returns
	errs []error                  // followers that gave up before their pod's log ended
}

func startCapture(c *cluster, selector, dir string) *capture {
	return newCapture(c, selector, dir, false)
}

// startRestartCapture is startCapture for pods whose containers are killed
// and restarted in place. A plain capture stops following a pod once its
// container has exited, and a re-attach rewrites the file, so a restarted
// container would either go unread or overwrite what the killed one logged.
// This one appends each attach's lines that the file lacks, and follows a
// pod until it is gone.
func startRestartCapture(c *cluster, selector, dir string) *capture {
	return newCapture(c, selector, dir, true)
}

func newCapture(c *cluster, selector, dir string, restarts bool) *capture {
	ctx, cancel := context.WithCancel(context.Background())
	cp := &capture{c: c, selector: selector, dir: dir, cancel: cancel, restarts: restarts,
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
		var w io.WriteCloser = &lazyFile{path: path}
		if cp.restarts {
			af, err := openAppend(path)
			if err != nil {
				return err
			}
			w = af
		}
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
			if cp.restarts {
				// An exited container is restarted in place: keep following
				// until the pod itself is gone.
				exited = false
			}
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

// appendFile appends one attach's output to a capture that earlier attaches
// to the same pod wrote. Every attach replays its container's current log
// from the start: a re-attach to the same container repeats lines the file
// has, and an attach to a restarted container brings only new ones. Every log
// line carries a nanosecond time, so equal lines are the same line, and lines
// arrive about in time order: a line older than the window before the file's
// last line is one the file already holds, and a newer one is skipped only if
// the file holds it.
type appendFile struct {
	f       *os.File
	partial []byte          // an incoming line not yet ended
	last    time.Time       // the newest time in the file when this attach began
	recent  map[string]bool // the file's lines from the window before last
	err     error
}

// appendWindow is how far out of time order two lines of one log may be.
// Concurrent goroutines stamp a line before they write it.
const appendWindow = 5 * time.Second

// appendTail is how much of the file's end openAppend reads: far more than
// any worker logs in appendWindow.
const appendTail = 64 << 20

func openAppend(path string) (*appendFile, error) {
	a := &appendFile{recent: map[string]bool{}}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*appendFile, error) { _ = f.Close(); return nil, err }
	fi, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	from := max(fi.Size()-appendTail, 0)
	tail := make([]byte, fi.Size()-from)
	if _, err := f.ReadAt(tail, from); err != nil && !errors.Is(err, io.EOF) {
		return fail(err)
	}
	end := bytes.LastIndexByte(tail, '\n') + 1 // a cut-off last line: the replay holds it whole
	if err := f.Truncate(from + int64(end)); err != nil {
		return fail(err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	tail = tail[:end]
	if from > 0 { // the first line read may be cut off at the front
		tail = tail[bytes.IndexByte(tail, '\n')+1:]
	}
	var lines [][]byte
	for line := range bytes.Lines(tail) {
		lines = append(lines, line)
		if at, ok := lineTime(line); ok && at.After(a.last) {
			a.last = at
		}
	}
	for _, line := range lines {
		if at, ok := lineTime(line); !ok || !at.Before(a.last.Add(-appendWindow)) {
			a.recent[string(bytes.TrimRight(line, "\n"))] = true
		}
	}
	a.f = f
	return a, nil
}

// lineTime reads a log line's "time".
func lineTime(line []byte) (time.Time, bool) {
	var e struct {
		Time time.Time `json:"time"`
	}
	if json.Unmarshal(bytes.TrimSpace(line), &e) != nil || e.Time.IsZero() {
		return time.Time{}, false
	}
	return e.Time, true
}

func (a *appendFile) Write(p []byte) (int, error) {
	if a.err != nil {
		return 0, a.err
	}
	a.partial = append(a.partial, p...)
	for {
		i := bytes.IndexByte(a.partial, '\n')
		if i < 0 {
			break
		}
		line := a.partial[:i+1]
		if a.keep(line) {
			if _, err := a.f.Write(line); err != nil {
				a.err = err
				return 0, err
			}
		}
		a.partial = a.partial[i+1:]
	}
	a.partial = slices.Clone(a.partial)
	return len(p), nil
}

// keep reports whether line is new to the file.
func (a *appendFile) keep(line []byte) bool {
	if len(bytes.TrimSpace(line)) == 0 {
		return false
	}
	if at, ok := lineTime(line); ok && at.Before(a.last.Add(-appendWindow)) {
		return false
	}
	return !a.recent[string(bytes.TrimRight(line, "\n"))]
}

// Close writes a last line that never ended, as lazyFile would, and reports
// the first error the file saw.
func (a *appendFile) Close() error {
	if len(a.partial) > 0 && a.err == nil && a.keep(a.partial) {
		_, a.err = a.f.Write(a.partial)
	}
	if err := a.f.Close(); a.err == nil {
		a.err = err
	}
	return a.err
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

// snapshot re-reads the log of pods that are still running, so a line still
// in flight on a follow stream when the capture stopped is not lost. The
// kubelet serves only a container's current log file, so once it has rotated
// the file (10 MiB on kind) the re-read starts mid-log. It is merged into the
// followed capture, never written over it.
func (cp *capture) snapshot(pods []string) {
	for _, pod := range pods {
		path := filepath.Join(cp.dir, pod+".log")
		followed, err := os.ReadFile(path)
		if !errors.Is(err, os.ErrNotExist) {
			require.NoError(cp.c.t, err)
		}
		out := cp.c.run("logs", "pod/"+pod)
		require.NoError(cp.c.t, os.WriteFile(path, mergeLog(followed, []byte(out+"\n")), 0o644))
	}
}

// snapshotAppend is snapshot for a restart capture: it re-reads the current
// container's log of pods still running and appends the lines the file lacks,
// as a re-attach does (appendFile), streaming, so memory stays bounded however
// long the log. snapshot holds a whole log and a set of its lines.
func (cp *capture) snapshotAppend(pods []string) {
	for _, pod := range pods {
		a, err := openAppend(filepath.Join(cp.dir, pod+".log"))
		require.NoError(cp.c.t, err)
		cmd := cp.c.cmd(context.Background(), "logs", "pod/"+pod)
		cmd.Stdout = a
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		runErr := cmd.Run()
		require.NoError(cp.c.t, errors.Join(runErr, a.Close()), "kubectl logs pod/%s: %s", pod, stderr.String())
	}
}

// mergeLog returns followed's complete lines, then every line of reread that
// followed lacks, in reread's order. An unterminated last line in followed was
// cut off mid-stream, and reread holds it whole. Every log line carries a
// nanosecond timestamp, so equal lines are the same line.
func mergeLog(followed, reread []byte) []byte {
	followed = followed[:bytes.LastIndexByte(followed, '\n')+1]
	seen := map[string]bool{}
	for line := range bytes.Lines(followed) {
		seen[string(line)] = true
	}
	out := bytes.Clone(followed)
	for line := range bytes.Lines(reread) {
		if len(bytes.TrimSpace(line)) > 0 && !seen[string(line)] {
			out = append(out, line...)
		}
	}
	return out
}

// TestMergeLogKeepsWhatARotationHid needs no cluster: a re-read that starts
// mid-log, because the kubelet rotated the file, must not drop what the
// follower captured before the rotation, and nothing may appear twice.
func TestMergeLogKeepsWhatARotationHid(t *testing.T) {
	for _, tc := range []struct{ name, followed, reread, want string }{
		{"no rotation, a line in flight", "a\nb\n", "a\nb\nc\n", "a\nb\nc\n"},
		{"rotated after b", "a\nb\nc\n", "c\nd\n", "a\nb\nc\nd\n"},
		{"rotated, follow cut mid-line", "a\nb\nc", "c-whole\nd\n", "a\nb\nc-whole\nd\n"},
		{"nothing followed", "", "a\n", "a\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, string(mergeLog([]byte(tc.followed), []byte(tc.reread))))
		})
	}
}

// TestAppendFileFollowsARestartedContainer needs no cluster: each attach to a
// worker replays its container's log, so a re-attach to the same container
// adds only what the file lacks, and an attach to the container the kubelet
// restarted after a kill adds everything it logged, after what the killed one
// did.
func TestAppendFileFollowsARestartedContainer(t *testing.T) {
	line := func(s float64, msg string) string {
		at := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC).Add(time.Duration(s * float64(time.Second)))
		return fmt.Sprintf(`{"time":%q,"msg":%q}`, at.Format(time.RFC3339Nano), msg) + "\n"
	}
	path := filepath.Join(t.TempDir(), "w.log")
	attach := func(chunks ...string) {
		t.Helper()
		a, err := openAppend(path)
		require.NoError(t, err)
		for _, c := range chunks {
			_, err := a.Write([]byte(c))
			require.NoError(t, err)
		}
		require.NoError(t, a.Close())
	}
	read := func() string {
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		return string(b)
	}

	// The first container: its stream breaks mid-line.
	full := line(1, "a") + line(2, "b")
	attach(full + line(3, "c")[:10])
	// A re-attach to the same container replays it, the cut line whole.
	attach(full[:7], full[7:]+line(3, "c"), line(4, "d"))
	want := line(1, "a") + line(2, "b") + line(3, "c") + line(4, "d")
	require.Equal(t, want, read())
	// Killed, then restarted in place: the new container's log is new.
	attach(line(20, "worker starting") + line(21, "e"))
	want += line(20, "worker starting") + line(21, "e")
	require.Equal(t, want, read())
	// A line a little out of time order is still placed; one far older than
	// the file's end is a replay of what the file holds.
	attach(line(20, "worker starting") + line(21, "e") + line(20.5, "late") + line(22, "f") + line(1, "old"))
	want += line(20.5, "late") + line(22, "f")
	require.Equal(t, want, read())
}

// newCluster reads which cluster, namespace and release the proofs act on.
func newCluster(t *testing.T) *cluster {
	return &cluster{t: t,
		context:   setting("HOPPER_K8S_CONTEXT", "kind-hopper"),
		namespace: setting("HOPPER_K8S_NAMESPACE", "hopper"),
		release:   setting("HOPPER_K8S_RELEASE", "hopper")}
}

// output runs kubectl to completion and returns its trimmed stdout. Unlike run
// it never fails the test, so require.Eventually conditions and background
// samplers, which run on other goroutines, can call it.
func (c *cluster) output(ctx context.Context, args ...string) (string, error) {
	out, err := c.cmd(ctx, args...).Output()
	if ee, ok := err.(*exec.ExitError); ok {
		err = fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(ee.Stderr))
	}
	return strings.TrimSpace(string(out)), err
}

// livePods lists the pods matching selector that are not being deleted.
func (c *cluster) livePods(selector string) []string {
	c.t.Helper()
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

// followed lists every pod the capture has followed, sorted.
func (cp *capture) followed() []string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	pods := make([]string, 0, len(cp.done))
	for pod := range cp.done {
		pods = append(pods, pod)
	}
	slices.Sort(pods)
	return pods
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
		`select id, state::text, attempts, lease_id from jobs where idempotency_key like $1 || '-%'`, run)
	require.NoError(t, err)
	defer rows.Close()
	var out []accounting.Row
	for rows.Next() {
		var r accounting.Row
		var lease *uuid.UUID
		require.NoError(t, rows.Scan(&r.ID, &r.State, &r.Attempts, &lease))
		if lease != nil {
			r.LeaseID = *lease
		}
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
