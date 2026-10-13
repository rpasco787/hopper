package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PoolStat is the part of *pgxpool.Stat the pool collector reads. It is an
// interface so that this package does not import the driver and a test can
// supply the numbers.
type PoolStat interface {
	MaxConns() int32
	AcquiredConns() int32
	IdleConns() int32
	ConstructingConns() int32
	AcquireCount() int64
	EmptyAcquireCount() int64
	EmptyAcquireWaitTime() time.Duration
}

// poolCollector reads the pool's statistics at scrape time. The pool already
// counts everything, so copying its numbers on demand is cheaper and more
// exact than mirroring them into counters on every acquire.
type poolCollector struct {
	stat                                        func() PoolStat
	maxConns, conns, acquires, empty, emptyWait *prometheus.Desc
}

// RegisterPool exports the broker's PostgreSQL pool. Waiting acquires are the
// signal: an acquire that finds no idle connection waits for one, and a
// broker whose waits grow with load is short of connections, not of CPU.
func RegisterPool(reg prometheus.Registerer, stat func() PoolStat) error {
	name := func(n string) string { return prometheus.BuildFQName(namespace, "db_pool", n) }
	return reg.Register(&poolCollector{
		stat: stat,
		maxConns: prometheus.NewDesc(name("max_connections"),
			"Most connections the broker's PostgreSQL pool may open.", nil, nil),
		conns: prometheus.NewDesc(name("connections"),
			"Pool connections by state: acquired (in use, including connections held for the scheduler role and the wakeup listener), idle, or constructing.",
			[]string{"state"}, nil),
		acquires: prometheus.NewDesc(name("acquires_total"),
			"Connections taken from the pool.", nil, nil),
		empty: prometheus.NewDesc(name("empty_acquires_total"),
			"Acquires that found no idle connection and waited for one to be released or opened.", nil, nil),
		emptyWait: prometheus.NewDesc(name("empty_acquire_wait_seconds_total"),
			"Seconds acquires spent waiting because the pool had no idle connection.", nil, nil),
	})
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.maxConns, c.conns, c.acquires, c.empty, c.emptyWait} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.stat()
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, float64(s.AcquiredConns()), "acquired")
	ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, float64(s.IdleConns()), "idle")
	ch <- prometheus.MustNewConstMetric(c.conns, prometheus.GaugeValue, float64(s.ConstructingConns()), "constructing")
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.empty, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyWait, prometheus.CounterValue, s.EmptyAcquireWaitTime().Seconds())
}
