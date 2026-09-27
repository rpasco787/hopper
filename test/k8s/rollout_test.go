//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

func TestRolloutRestartDuplicatesNothing(t *testing.T) {
	c := newCluster(t)
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
		return c.livePods(selector)
	}
	bg := context.Background()

	// A fixed worker count: an autoscaler would resize the workers under the
	// proof and replace pods the rollout did not.
	scaled := c.run("get", "hpa", "-o", `jsonpath={range .items[*]}{.spec.scaleTargetRef.name}{"\n"}{end}`)
	require.NotContains(t, strings.Fields(scaled), c.release+"-worker",
		"autoscaling is on; this proof needs a fixed count: scripts/kind-up.sh --set worker.replicas=%d --force-conflicts", workers)

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
