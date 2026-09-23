// Package metrics contains Prometheus collectors used by hopper processes.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const namespace = "hopper"

// JobBuckets spans a sub-10ms lease on an idle queue to a ten-minute backlog.
// The client default stops at 10s, where a draining burst's p99 would clamp.
var JobBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}

// RPCBuckets covers unary RPC latency, which is one or two database round trips.
var RPCBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// NewRegistry returns a registry with the Go runtime and process collectors,
// and nothing else. hopper never uses prometheus.DefaultRegisterer: a library
// that registers globally makes its importer's tests panic on the second
// registration, and makes two components in one binary collide.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}
