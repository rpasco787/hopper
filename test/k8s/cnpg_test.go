//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// healthyPhase is what CloudNativePG reports when every instance is up and the
// primary is the one it wants.
const healthyPhase = "Cluster in healthy state"

// replicatedDB is the release's CloudNativePG Cluster (postgres.mode=
// cloudnativepg), driven through kubectl and, to crash it, through the kind
// node's container runtime.
type replicatedDB struct {
	c    *cluster
	name string // Cluster/<release>-db; its read-write Service is <name>-rw
	node string // the kind node's Docker container
}

func replicatedDatabase(c *cluster) *replicatedDB {
	c.t.Helper()
	name := c.release + "-db"
	_, err := c.output(context.Background(), "get", "clusters.postgresql.cnpg.io", name)
	require.NoError(c.t, err, "the proof needs the replicated database: scripts/kind-cnpg.sh, then scripts/kind-up.sh -f deploy/kind/replicated.yaml")
	return &replicatedDB{c: c, name: name, node: setting("HOPPER_K8S_NODE", "hopper-control-plane")}
}

// dbStatus is one reading of the Cluster's status.
type dbStatus struct {
	Phase           string
	Primary, Target string // status.currentPrimary and status.targetPrimary
	Instances       int
	Ready           int
}

func (s dbStatus) healthy() bool {
	return s.Phase == healthyPhase && s.Instances > 1 && s.Ready == s.Instances && s.Primary != "" && s.Primary == s.Target
}

// status never fails the test, so polling loops can call it.
func (r *replicatedDB) status(ctx context.Context) (dbStatus, error) {
	out, err := r.c.output(ctx, "get", "clusters.postgresql.cnpg.io", r.name, "-o", "json")
	if err != nil {
		return dbStatus{}, err
	}
	var cl struct {
		Status struct {
			Phase          string `json:"phase"`
			CurrentPrimary string `json:"currentPrimary"`
			TargetPrimary  string `json:"targetPrimary"`
			Instances      int    `json:"instances"`
			ReadyInstances int    `json:"readyInstances"`
		} `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &cl); err != nil {
		return dbStatus{}, err
	}
	s := cl.Status
	return dbStatus{Phase: s.Phase, Primary: s.CurrentPrimary, Target: s.TargetPrimary,
		Instances: s.Instances, Ready: s.ReadyInstances}, nil
}

// awaitHealthy waits until every instance is ready and the primary settled.
// A previous run's crashed primary has to rejoin as a standby first.
func (r *replicatedDB) awaitHealthy(timeout time.Duration) dbStatus {
	r.c.t.Helper()
	require.Eventually(r.c.t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		s, err := r.status(ctx)
		return err == nil && s.healthy()
	}, timeout, 2*time.Second, "Cluster/%s never became healthy", r.name)
	s, err := r.status(context.Background())
	require.NoError(r.c.t, err)
	require.True(r.c.t, s.healthy(), "Cluster/%s: %+v", r.name, s)
	return s
}

// psql runs sql on an instance as the postgres superuser over the pod's local
// socket, and returns its unaligned output. It never fails the test.
func (r *replicatedDB) psql(ctx context.Context, pod, sql string) (string, error) {
	return r.c.output(ctx, "exec", pod, "-c", "postgres", "--", "psql", "-XAtc", sql)
}

// synchronousNames is the primary's synchronous_standby_names: empty means
// asynchronous replication.
func (r *replicatedDB) synchronousNames(primary string) string {
	r.c.t.Helper()
	out, err := r.psql(context.Background(), primary, "show synchronous_standby_names")
	require.NoError(r.c.t, err)
	return out
}

// pauseReplication stops the primary's WAL senders with SIGSTOP, so no commit
// from now on reaches a standby. An asynchronous primary goes on
// acknowledging commits; a synchronous one makes them wait. It stands in for
// standbys that have fallen behind when the primary dies. The returned
// function resumes them. The crash that follows makes that moot, but cleanup
// calls it in case the test fails first.
func (r *replicatedDB) pauseReplication(primary string, standbys int) (resume func()) {
	r.c.t.Helper()
	pids, err := r.psql(context.Background(), primary, "select string_agg(pid::text, ' ') from pg_stat_replication")
	require.NoError(r.c.t, err)
	require.Len(r.c.t, strings.Fields(pids), standbys, "WAL senders on %s", primary)
	signal := func(sig string) error {
		_, err := r.c.output(context.Background(), "exec", primary, "-c", "postgres", "--", "sh", "-c", "kill -"+sig+" "+pids)
		return err
	}
	require.NoError(r.c.t, signal("STOP"))
	return func() { _ = signal("CONT") }
}

// crash ends the primary as a machine failure would. It SIGKILLs the pod's
// postgres container through the kind node's container runtime, so PostgreSQL
// has no chance to shut down, then force-deletes the pod so the operator
// fails over instead of restarting the instance in place. Deleting the pod
// alone is a shutdown, and a shutdown sends the standbys the WAL they lack
// (docs/failover/kill-modes.txt). It returns the time just before the kill.
func (r *replicatedDB) crash(pod string) time.Time {
	r.c.t.Helper()
	id := r.onNode("crictl", "ps", "-q", "--name", "^postgres$",
		"--label", "io.kubernetes.pod.name="+pod, "--label", "io.kubernetes.pod.namespace="+r.c.namespace)
	require.Len(r.c.t, strings.Fields(id), 1, "pod %s's running postgres container", pod)
	at := time.Now()
	r.onNode("crictl", "stop", "--timeout", "0", id)
	r.c.run("delete", "pod", pod, "--grace-period=0", "--force", "--wait=false")
	return at
}

// onNode runs a command in the kind node's container.
func (r *replicatedDB) onNode(args ...string) string {
	r.c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", append([]string{"exec", r.node}, args...)...).Output()
	var stderr []byte
	if ee, ok := err.(*exec.ExitError); ok {
		stderr = ee.Stderr
	}
	require.NoError(r.c.t, err, "docker exec %s %s: %s", r.node, strings.Join(args, " "), stderr)
	return strings.TrimSpace(string(out))
}

// awaitPromotion waits until an instance other than old is primary and the
// read-write Service routes to it alone. It returns that instance and when
// each was first seen.
func (r *replicatedDB) awaitPromotion(old string, timeout time.Duration) (primary string, promoted, routed time.Time) {
	r.c.t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if promoted.IsZero() {
			if s, err := r.status(ctx); err == nil && s.Primary != "" && s.Primary != old && s.Primary == s.Target {
				primary, promoted = s.Primary, time.Now()
			}
		}
		if !promoted.IsZero() {
			if pods, err := r.routed(ctx); err == nil && slices.Equal(pods, []string{primary}) {
				cancel()
				return primary, promoted, time.Now()
			}
		}
		cancel()
	}
	r.c.t.Fatalf("no instance replaced %s as the routed primary within %s", old, timeout)
	return "", time.Time{}, time.Time{}
}

// routed lists the pods the read-write Service sends connections to: the
// ready endpoints of its EndpointSlices, sorted.
func (r *replicatedDB) routed(ctx context.Context) ([]string, error) {
	out, err := r.c.output(ctx, "get", "endpointslices", "-l", "kubernetes.io/service-name="+r.name+"-rw", "-o",
		`jsonpath={range .items[*].endpoints[*]}{.targetRef.name} {.conditions.ready}{"\n"}{end}`)
	if err != nil {
		return nil, err
	}
	var pods []string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[1] == "true" {
			pods = append(pods, f[0])
		}
	}
	slices.Sort(pods)
	return pods, nil
}

// follow streams one instance's postgres log into path until its container
// exits. Unlike capture it never re-attaches: the operator recreates a lost
// instance under the same pod name, and a re-attach would overwrite the dead
// instance's log with the new one's.
type follow struct {
	pod, path string
	done      chan struct{} // closed when the stream ends
}

func (r *replicatedDB) follow(pod, path string) *follow {
	r.c.t.Helper()
	f, err := os.Create(path)
	require.NoError(r.c.t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	cmd := r.c.cmd(ctx, "logs", "--follow", "pod/"+pod, "-c", "postgres")
	cmd.Stdout = f
	require.NoError(r.c.t, cmd.Start())
	fl := &follow{pod: pod, path: path, done: make(chan struct{})}
	go func() {
		defer close(fl.done)
		defer cancel()
		_ = cmd.Wait()
		_ = f.Close()
	}()
	r.c.t.Cleanup(func() { cancel(); <-fl.done })
	// An attach replays the log from the start, so it is never empty for long.
	require.Eventually(r.c.t, func() bool {
		fi, err := os.Stat(path)
		return err == nil && fi.Size() > 0
	}, 30*time.Second, 100*time.Millisecond, "pod %s's log never attached", pod)
	return fl
}

// live reports whether the stream is still open.
func (fl *follow) live() bool {
	select {
	case <-fl.done:
		return false
	default:
		return true
	}
}

// ended waits for the stream to end with its container, and returns the log.
func (fl *follow) ended(t *testing.T) string {
	t.Helper()
	select {
	case <-fl.done:
	case <-time.After(time.Minute):
		t.Fatalf("pod %s's log never ended", fl.pod)
	}
	b, err := os.ReadFile(fl.path)
	require.NoError(t, err)
	return string(b)
}

// instances lists the Cluster's instance pods, as its status names them.
func (r *replicatedDB) instances() []string {
	r.c.t.Helper()
	names := strings.Fields(r.c.run("get", "clusters.postgresql.cnpg.io", r.name, "-o", "jsonpath={.status.instanceNames[*]}"))
	require.NotEmpty(r.c.t, names, "Cluster/%s lists no status.instanceNames", r.name)
	return names
}

// standbySample is one reading of an instance after the crash.
type standbySample struct {
	At      time.Time `json:"at"` // when the query was sent
	Pod     string    `json:"pod"`
	Primary bool      `json:"primary"` // not in recovery
	Sync    int       `json:"sync"`    // standbys streaming to it as sync or quorum
	Names   string    `json:"synchronous_standby_names"`
	Err     string    `json:"err,omitempty"`
}

// standbyQuery reads what a standbySample holds.
const standbyQuery = `select not pg_is_in_recovery(),
  (select count(*) from pg_stat_replication where state = 'streaming' and sync_state in ('sync', 'quorum')),
  current_setting('synchronous_standby_names')`

// watchStandbys samples pods every interval, all at once, until a sample
// finds a primary with a synchronous standby streaming to it, or timeout. A
// new primary serves before any standby streams to it, and what it answers
// then no standby has (docs/semantics.md §11 item 7). The function it
// returns waits for the watch to end and returns the samples by time. It
// never fails the test: it runs on its own goroutine.
func (r *replicatedDB) watchStandbys(pods []string, interval, timeout time.Duration) func() []standbySample {
	var (
		mu      sync.Mutex
		samples []standbySample
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(interval) {
			var wg sync.WaitGroup
			for _, pod := range pods {
				wg.Go(func() {
					s := r.sampleStandby(pod)
					mu.Lock()
					samples = append(samples, s)
					mu.Unlock()
				})
			}
			wg.Wait()
			mu.Lock()
			_, found := firstStandby(samples)
			mu.Unlock()
			if found {
				return
			}
		}
	}()
	return func() []standbySample {
		<-done
		mu.Lock()
		defer mu.Unlock()
		out := slices.Clone(samples)
		slices.SortFunc(out, func(a, b standbySample) int { return a.At.Compare(b.At) })
		return out
	}
}

// sampleStandby reads one instance. An instance being recreated, or not yet
// answering, gives a sample with Err set.
func (r *replicatedDB) sampleStandby(pod string) standbySample {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s := standbySample{At: time.Now(), Pod: pod}
	out, err := r.psql(ctx, pod, standbyQuery)
	if err != nil {
		s.Err = err.Error()
		return s
	}
	f := strings.SplitN(out, "|", 3)
	if len(f) != 3 {
		s.Err = "unexpected psql output: " + out
		return s
	}
	n, err := strconv.Atoi(f[1])
	if err != nil {
		s.Err = err.Error()
		return s
	}
	s.Primary, s.Sync, s.Names = f[0] == "t", n, f[2]
	return s
}
