//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/client"
	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/metrics"
)

func TestUnknownDepthHoldsTheFallback(t *testing.T) {
	c := newCluster(t)
	a := scaledWorkers(c)
	require.Equal(t, "currentReplicasIfHigher", a.fallbackBehavior)
	require.Greater(t, a.fallback, a.min, "a floor at the minimum could not show the fallback acting")
	bg := context.Background()
	a.settle(t)

	// The depth must become unknown because the brokers cannot sample, not
	// because they went away: a crashed, restarted or replaced broker's series
	// vanish too. The same pods must run, unrestarted, from start to end.
	brokerPods := func() string {
		return c.run("get", "pods", "-l", "app.kubernetes.io/instance="+c.release+",app.kubernetes.io/component=broker", "-o",
			`jsonpath={range .items[*]}{.metadata.uid} {.status.containerStatuses[*].restartCount}{"\n"}{end}`)
	}
	pods := brokerPods()
	brokers := len(strings.Split(pods, "\n"))
	fresh := metrics.DepthFreshness.Seconds()
	// frozen reports whether every broker is still scraped and its sample
	// timestamp has stopped for at least the freshness window.
	frozen := func(ct assert.TestingT) bool {
		ages, err := a.brokerAges(bg)
		if !assert.NoError(ct, err) || !assert.Len(ct, ages, brokers, "brokers Prometheus still scrapes") {
			return false
		}
		ok := true
		for _, age := range ages {
			ok = assert.GreaterOrEqual(ct, age, fresh, "a broker's depth sample is still advancing") && ok
		}
		return ok
	}

	// The database refuses the brokers, as in an outage: new connections are
	// rejected and the open ones terminated. Only the brokers' database
	// changes; the psql session runs in the maintenance database postgres.
	dsn, err := url.Parse(c.run("get", "configmap", c.release+"-broker", "-o", "jsonpath={.data.HOPPER_DSN}"))
	require.NoError(t, err)
	user, db := dsn.User.Username(), strings.TrimPrefix(dsn.Path, "/")
	pod := c.release + "-postgres-0"
	psql := func(sql string) error {
		_, err := c.output(bg, "exec", pod, "--", "psql", "-U", user, "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-Atc", sql)
		return err
	}
	allow := "alter database " + db + " allow_connections true"
	t.Cleanup(func() {
		if err := psql(allow); err != nil {
			t.Errorf("the brokers are still refused (%v). Restore by hand: kubectl --context %s -n %s exec %s -- psql -U %s -d postgres -c '%s'",
				err, c.context, c.namespace, pod, user, allow)
		}
	})
	require.NoError(t, psql("alter database "+db+" allow_connections false"))
	require.NoError(t, psql("select count(pg_terminate_backend(pid)) from pg_stat_activity where datname = '"+db+"'"))
	cut := time.Now()

	// Every broker's sample fails and its timestamp freezes. After 15s the
	// fresh-only query has no sample: the depth is unknown, not zero.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		s, err := a.read(bg)
		if assert.NoError(ct, err) {
			assert.False(ct, s.known, "the query still reads %v", s.depth)
		}
	}, time.Minute, 2*time.Second)
	unknown := time.Since(cut)
	require.EventuallyWithT(t, func(ct *assert.CollectT) { frozen(ct) }, 30*time.Second, 2*time.Second,
		"the depth is unknown, but not because every broker's sample froze")

	// KEDA treats the empty result as an error and, after three in a row,
	// reports the fallback. The workers were idle at the minimum, so only the
	// fallback can raise them: there is no backlog to read.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		s, err := a.read(bg)
		if assert.NoError(ct, err) {
			assert.Equal(ct, "True", s.fallback, "Fallback condition")
			assert.Equal(ct, a.fallback, s.desired, "desired")
			assert.Equal(ct, a.fallback, s.running, "running")
		}
	}, 4*time.Minute, 2*time.Second)
	held := time.Since(cut)
	require.True(t, frozen(t), "the brokers were no longer frozen while the fallback held")

	require.NoError(t, psql(allow))
	back := time.Now()
	// The query is known as soon as one broker samples again; wait for all.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		ages, err := a.brokerAges(bg)
		if assert.NoError(ct, err) && assert.Len(ct, ages, brokers, "brokers Prometheus scrapes") {
			for _, age := range ages {
				assert.Less(ct, age, fresh, "a broker's depth sample is still frozen")
			}
		}
		s, err := a.read(bg)
		if assert.NoError(ct, err) {
			assert.True(ct, s.known, "depth unknown")
			assert.NotEqual(ct, "True", s.fallback, "fallback")
		}
	}, 2*time.Minute, 2*time.Second)
	known := time.Since(back)
	a.settle(t)
	settled := time.Since(back)
	require.Equal(t, pods, brokerPods(), "the brokers were replaced or restarted (pod UID and restart count)")

	// The queue works end to end again.
	cl, err := client.Dial(c.forward("svc/"+c.release+"-broker", "9090"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cl.Close() })
	res, err := cl.EnqueueBatch(bg, []client.Job{{Queue: "sleep", Payload: json.RawMessage(`{"ms":10}`)}})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Eventually(t, func() bool {
		r, err := cl.GetJob(bg, res[0].ID)
		return err == nil && r.State == hopperv1.JobState_JOB_STATE_COMPLETED.String()
	}, 2*time.Minute, time.Second, "a job enqueued after the outage never completed")

	t.Logf("database refused: depth unknown after %s with %d brokers alive and frozen; fallback held %d workers after %s; "+
		"allowed again: every broker fresh after %s, back to %d idle workers after %s; no broker restarted",
		unknown.Round(time.Second), brokers, a.fallback, held.Round(time.Second),
		known.Round(time.Second), a.min, settled.Round(time.Second))
}
