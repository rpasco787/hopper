//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

// autoscaler is the release's worker ScaledObject as the cluster runs it, the
// HPA KEDA made from it, and the pods they scale.
type autoscaler struct {
	c                  *cluster
	name, hpa          string
	selector           string
	query              string
	promPath           string  // KEDA's Prometheus query API, through the API server's service proxy
	per                float64 // the trigger's threshold: available jobs per worker
	min, max, fallback int
	fallbackBehavior   string
}

// scaledWorkers reads the worker ScaledObject, and fails when autoscaling is off.
func scaledWorkers(c *cluster) *autoscaler {
	c.t.Helper()
	name := c.release + "-worker"
	raw, err := c.output(context.Background(), "get", "scaledobject", name, "-o", "json")
	require.NoError(c.t, err, "the proof needs autoscaling: scripts/kind-addons.sh, then scripts/kind-up.sh -f deploy/kind/autoscaling.yaml")
	var so struct {
		Spec struct {
			MinReplicaCount, MaxReplicaCount int
			Fallback                         struct {
				Replicas int
				Behavior string
			}
			Triggers []struct{ Metadata map[string]string }
		}
	}
	require.NoError(c.t, json.Unmarshal([]byte(raw), &so))
	require.Len(c.t, so.Spec.Triggers, 1, "ScaledObject/%s triggers", name)
	md := so.Spec.Triggers[0].Metadata
	// http://<service>.<namespace>.svc:<port>, which the API server's service
	// proxy reaches without a port-forward.
	server, err := url.Parse(md["serverAddress"])
	require.NoError(c.t, err)
	host := strings.Split(server.Hostname(), ".")
	require.GreaterOrEqual(c.t, len(host), 2, "serverAddress %q is not a Service DNS name", md["serverAddress"])
	per, err := strconv.ParseFloat(md["threshold"], 64)
	require.NoError(c.t, err, "ScaledObject/%s threshold", name)
	return &autoscaler{c: c, name: name, hpa: "keda-hpa-" + name,
		selector:         "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=worker",
		query:            md["query"],
		promPath:         fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%s/proxy/api/v1/query", host[1], host[0], server.Port()),
		per:              per,
		min:              so.Spec.MinReplicaCount,
		max:              so.Spec.MaxReplicaCount,
		fallback:         so.Spec.Fallback.Replicas,
		fallbackBehavior: so.Spec.Fallback.Behavior,
	}
}

// scaleState is one reading of the autoscaler and the pods it scales.
type scaleState struct {
	depth                         float64 // the ScaledObject's own query
	known                         bool    // false: the query returned no sample
	desired                       int     // the HPA's status.desiredReplicas
	running, pending, terminating int     // worker pods; running and pending exclude pods being deleted
	fallback                      string  // the ScaledObject's Fallback condition: True, False or empty
}

// prom runs an instant query against the Prometheus KEDA reads, and returns
// each series' value. It never fails the test.
func (a *autoscaler) prom(ctx context.Context, query string) ([]float64, error) {
	raw, err := a.c.output(ctx, "get", "--raw", a.promPath+"?query="+url.QueryEscape(query))
	if err != nil {
		return nil, err
	}
	var resp struct {
		Status string
		Data   struct{ Result []struct{ Value [2]any } }
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil || resp.Status != "success" {
		return nil, fmt.Errorf("prometheus answered %q: %v", resp.Status, err)
	}
	values := make([]float64, 0, len(resp.Data.Result))
	for _, r := range resp.Data.Result {
		v, _ := r.Value[1].(string)
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: value %q: %w", query, v, err)
		}
		values = append(values, f)
	}
	return values, nil
}

// brokerAges returns how old each of the release's brokers' last depth sample
// is, in seconds: one value per broker Prometheus still scrapes.
func (a *autoscaler) brokerAges(ctx context.Context) ([]float64, error) {
	return a.prom(ctx, fmt.Sprintf(`time() - hopper_queue_depth_last_sample_timestamp_seconds{namespace=%q, job=%q}`,
		a.c.namespace, a.c.release+"-broker"))
}

// read never fails the test, so conditions and the timeline can call it.
func (a *autoscaler) read(ctx context.Context) (scaleState, error) {
	var s scaleState
	depth, err := a.prom(ctx, a.query)
	if err != nil {
		return s, err
	}
	switch n := len(depth); {
	case n > 1:
		return s, fmt.Errorf("the query returned %d series; KEDA needs one", n)
	case n == 1:
		s.depth, s.known = depth[0], true
	}
	out, err := a.c.output(ctx, "get", "hpa", a.hpa, "-o", "jsonpath={.status.desiredReplicas}")
	if err != nil {
		return s, err
	}
	if s.desired, err = strconv.Atoi(out); err != nil {
		return s, fmt.Errorf("HorizontalPodAutoscaler/%s desiredReplicas %q: %w", a.hpa, out, err)
	}
	out, err = a.c.output(ctx, "get", "pods", "-l", a.selector, "-o",
		`jsonpath={range .items[*]}{.status.phase} {.metadata.deletionTimestamp}{"\n"}{end}`)
	if err != nil {
		return s, err
	}
	for _, line := range strings.Split(out, "\n") {
		switch f := strings.Fields(line); {
		case len(f) == 2:
			s.terminating++
		case len(f) == 1 && f[0] == "Running":
			s.running++
		case len(f) == 1:
			s.pending++
		}
	}
	s.fallback, err = a.c.output(ctx, "get", "scaledobject", a.name, "-o",
		`jsonpath={.status.conditions[?(@.type=="Fallback")].status}`)
	return s, err
}

// assertSettled checks that s rests at n idle workers: a known depth of zero,
// no fallback, n wanted and running, and none pending or terminating.
func assertSettled(t assert.TestingT, s scaleState, n int) {
	assert.True(t, s.known, "depth unknown")
	assert.Zero(t, s.depth, "depth")
	assert.NotEqual(t, "True", s.fallback, "fallback")
	assert.Equal(t, n, s.desired, "desired")
	assert.Equal(t, n, s.running, "running")
	assert.Zero(t, s.pending+s.terminating, "pending or terminating")
}

// settle waits until the autoscaler rests at its minimum, reading a known
// depth of zero, so every pod a run sees beyond those was made for the run.
func (a *autoscaler) settle(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		s, err := a.read(context.Background())
		if assert.NoError(ct, err) {
			assertSettled(ct, s, a.min)
		}
	}, 5*time.Minute, 2*time.Second, "ScaledObject/%s never settled at %d idle workers", a.name, a.min)
}

// timeline samples the autoscaler every 2s into a CSV file until stop, and
// keeps what the proof asserts about it.
type timeline struct {
	cancel context.CancelFunc
	done   chan struct{} // closed when sampling has ended
	err    error         // from closing the file; read only after done

	mu sync.Mutex
	timelineSummary
}

// timelineSummary is what a timeline saw. A read cut short by stop is not a
// failure.
type timelineSummary struct {
	samples, failures int
	firstErr          error
	peakRunning       int
	peakDepth         float64
	last              scaleState
}

// startTimeline takes a.read, or a stand-in in TestTimelineStopsTwice.
func startTimeline(read func(context.Context) (scaleState, error), path string) (*timeline, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	tl := &timeline{cancel: cancel, done: make(chan struct{})}
	began := time.Now()
	go func() {
		defer close(tl.done)
		fmt.Fprintln(f, "seconds,depth,desired,running,pending,terminating,fallback")
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			s, err := read(ctx)
			tl.mu.Lock()
			switch {
			case ctx.Err() != nil:
			case err != nil:
				tl.failures++
				if tl.firstErr == nil {
					tl.firstErr = err
				}
			default:
				depth := ""
				if s.known {
					depth = strconv.FormatFloat(s.depth, 'f', -1, 64)
					tl.peakDepth = max(tl.peakDepth, s.depth)
				}
				tl.samples++
				tl.peakRunning = max(tl.peakRunning, s.running)
				tl.last = s
				fmt.Fprintf(f, "%.0f,%s,%d,%d,%d,%d,%s\n", time.Since(began).Seconds(), depth,
					s.desired, s.running, s.pending, s.terminating, s.fallback)
			}
			tl.mu.Unlock()
			select {
			case <-ctx.Done():
				tl.err = f.Close()
				return
			case <-tick.C:
			}
		}
	}()
	return tl, nil
}

// stop ends the sampling and reports whether the file was written. It can be
// called again: the proof stops the timeline, and its cleanup stops it again.
func (tl *timeline) stop() error {
	tl.cancel()
	<-tl.done
	return tl.err
}

func (tl *timeline) summary() timelineSummary {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.timelineSummary
}

// Needs no cluster: go test -tags k8s -run TestTimelineStopsTwice ./test/k8s
func TestTimelineStopsTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timeline.csv")
	// A read that only ends when stop cancels it.
	tl, err := startTimeline(func(ctx context.Context) (scaleState, error) {
		<-ctx.Done()
		return scaleState{}, ctx.Err()
	}, path)
	require.NoError(t, err)
	stopped := make(chan error, 2)
	go func() { stopped <- tl.stop(); stopped <- tl.stop() }()
	for range 2 {
		select {
		case err := <-stopped:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("timeline.stop blocked")
		}
	}
	s := tl.summary()
	require.Zero(t, s.failures, "a read cut short by stop counted as failed: %v", s.firstErr)
	require.Zero(t, s.samples)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "seconds,depth,desired,running,pending,terminating,fallback\n", string(raw))
}

func TestAutoscalingRedeliversNothing(t *testing.T) {
	c := newCluster(t)
	a := scaledWorkers(c)
	require.Greater(t, a.max, a.min, "ScaledObject/%s has nothing to scale", a.name)
	jobs := intSetting(t, "HOPPER_SCALE_JOBS", 20000)
	ms := intSetting(t, "HOPPER_SCALE_JOB_MS", 2000)
	run := fmt.Sprintf("s9-%d", time.Now().UnixNano())
	dir := filepath.Join(setting("HOPPER_PROOF_DIR", t.TempDir()), run)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	bg := context.Background()

	a.settle(t)
	first := c.livePods(a.selector)
	cp := startCapture(c, a.selector, dir)
	t.Cleanup(func() { _ = cp.stop() })
	// The workers already running must be streaming before any work exists;
	// the capture picks up every pod the scale-up adds as it appears, and an
	// attach replays a log from its start.
	require.Eventually(t, func() bool { return cp.attached(first) }, time.Minute, 200*time.Millisecond,
		"the first workers' logs never attached")

	pool := c.database()
	finished := func() int {
		var n int
		if err := pool.QueryRow(bg, `select count(*) from jobs
		                             where idempotency_key like $1 || '-%' and state in ('completed','dead','cancelled')`,
			run).Scan(&n); err != nil {
			t.Logf("count: %v", err)
		}
		return n
	}
	tl, err := startTimeline(a.read, filepath.Join(dir, "timeline.csv"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = tl.stop() })

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
	enqueued := time.Now()

	// Up: every replica the ScaledObject allows must be running, so the peak
	// is capacity, not a number on paper.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		s, err := a.read(bg)
		if assert.NoError(ct, err) {
			assert.Equal(ct, a.max, s.running, "running workers")
		}
	}, 5*time.Minute, 2*time.Second, "the workers never reached %d", a.max)
	atMax := time.Since(enqueued)

	require.Eventually(t, func() bool { return finished() == jobs }, 20*time.Minute, 2*time.Second,
		"%d jobs never finished", jobs)
	drained := time.Since(enqueued)

	// Down: back to the minimum once the backlog has stayed gone for the
	// scale-down stabilization window, and settled as at the start: the
	// removed pods gone, the depth known to be 0, no fallback.
	a.settle(t)
	// settle's read and the timeline's 2-second sampling are independent. Wait
	// until the evidence itself has captured the settled state before using its
	// final row as the proof's record.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assertSettled(ct, tl.summary().last, a.min)
	}, 10*time.Second, 200*time.Millisecond, "the timeline never recorded the settled state")
	atMin := time.Since(enqueued)

	survivors := c.livePods(a.selector)
	var removed []string
	for _, pod := range cp.followed() {
		if !slices.Contains(survivors, pod) {
			removed = append(removed, pod)
		}
	}
	require.GreaterOrEqual(t, len(removed), a.max-a.min, "scale-down removed %v", removed)
	require.NoError(t, cp.ended(removed, 3*time.Minute))
	require.NoError(t, cp.stop(), "log capture")
	require.NoError(t, tl.stop(), "timeline")
	cp.snapshot(survivors)

	rows := loadRows(t, pool, run)
	rep := accounting.Reconcile(rows, readLogs(t, dir))
	writeJSON(t, filepath.Join(dir, "report.json"), rep)

	busy := 0
	for _, pod := range removed {
		s, ok := shutdownOf(rep, pod)
		require.True(t, ok, "removed pod %s never logged a shutdown", pod)
		if s.RunningHandlers > 0 {
			busy++
		}
	}
	for _, n := range strings.Fields(c.run("get", "pods", "-l", a.selector,
		"-o", "jsonpath={.items[*].status.containerStatuses[*].restartCount}")) {
		require.Equal(t, "0", n, "a surviving worker restarted")
	}
	// The timeline is the evidence the Outcome quotes, so it must hold the
	// whole cycle: every read answered, the peak it saw is the one asserted
	// above, the depth it read demanded every worker, and it ends settled.
	tls := tl.summary()
	require.Zero(t, tls.failures, "timeline reads failed; the first: %v", tls.firstErr)
	require.Equal(t, a.max, tls.peakRunning, "the timeline's peak of running workers")
	require.Greater(t, tls.peakDepth, float64(a.max-1)*a.per,
		"the timeline never read a backlog that demands %d workers", a.max)
	assertSettled(t, tls.last, a.min)
	t.Logf("run %s: %d jobs of %dms, %d completed; %d running workers at +%s (peak depth read %.0f); "+
		"drained at +%s; back to %d at +%s; %d workers removed, %d of them busy at SIGTERM; "+
		"%d jobs redelivered, %d redeliveries (%d unstarted nacks, %d unexplained by nacks); "+
		"%d timeline samples; logs in %s",
		run, rep.Jobs, ms, rep.Completed, tls.peakRunning, atMax.Round(time.Second), tls.peakDepth,
		drained.Round(time.Second), a.min, atMin.Round(time.Second), len(removed), busy,
		len(rep.Extras), rep.ExtraLeases, rep.UnstartedNacks, rep.NeverReceived, tls.samples, dir)
	for _, s := range rep.Shutdowns {
		t.Logf("  %s: running_handlers=%d stopped=%v", s.Pod, s.RunningHandlers, s.Stopped)
	}
	require.Empty(t, rep.ExecutionProblems())
	require.Empty(t, rep.Extras, "redelivered jobs: each must complete on its first lease")
}
