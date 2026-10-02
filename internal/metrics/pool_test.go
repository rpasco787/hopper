package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type fakePoolStat struct{ acquires int64 }

func (fakePoolStat) MaxConns() int32          { return 32 }
func (fakePoolStat) AcquiredConns() int32     { return 5 }
func (fakePoolStat) IdleConns() int32         { return 3 }
func (fakePoolStat) ConstructingConns() int32 { return 1 }
func (f fakePoolStat) AcquireCount() int64    { return f.acquires }
func (fakePoolStat) EmptyAcquireCount() int64 { return 7 }
func (fakePoolStat) EmptyAcquireWaitTime() time.Duration {
	return 1500 * time.Millisecond
}

func TestPoolCollectorReadsTheStatAtScrapeTime(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	stat := fakePoolStat{acquires: 100}
	require.NoError(t, RegisterPool(reg, func() PoolStat { return stat }))

	want := `
# HELP hopper_db_pool_acquires_total Connections taken from the pool.
# TYPE hopper_db_pool_acquires_total counter
hopper_db_pool_acquires_total 100
# HELP hopper_db_pool_connections Pool connections by state: acquired (in use, including connections held for the scheduler role and the wakeup listener), idle, or constructing.
# TYPE hopper_db_pool_connections gauge
hopper_db_pool_connections{state="acquired"} 5
hopper_db_pool_connections{state="constructing"} 1
hopper_db_pool_connections{state="idle"} 3
# HELP hopper_db_pool_empty_acquire_wait_seconds_total Seconds acquires spent waiting because the pool had no idle connection.
# TYPE hopper_db_pool_empty_acquire_wait_seconds_total counter
hopper_db_pool_empty_acquire_wait_seconds_total 1.5
# HELP hopper_db_pool_empty_acquires_total Acquires that found no idle connection and waited for one to be released or opened.
# TYPE hopper_db_pool_empty_acquires_total counter
hopper_db_pool_empty_acquires_total 7
# HELP hopper_db_pool_max_connections Most connections the broker's PostgreSQL pool may open.
# TYPE hopper_db_pool_max_connections gauge
hopper_db_pool_max_connections 32
`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(want)))

	// Nothing is cached: the next scrape reads the pool again.
	stat.acquires = 250
	family := metricFamily(t, reg, "hopper_db_pool_acquires_total")
	require.Equal(t, 250.0, family.Metric[0].GetCounter().GetValue())
}

func TestRegisterPoolReturnsRegistrationErrors(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	stat := func() PoolStat { return fakePoolStat{} }
	require.NoError(t, RegisterPool(reg, stat))
	require.Error(t, RegisterPool(reg, stat), "a second pool collector collides")
}
