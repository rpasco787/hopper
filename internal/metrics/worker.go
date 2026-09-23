package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	workersdk "github.com/rpasco787/hopper/worker"
)

// Worker is the worker process's metrics. It implements worker.Observer.
type Worker struct {
	inflight prometheus.Gauge
	handler  *prometheus.HistogramVec
	queues   map[string]struct{}
}

var _ workersdk.Observer = (*Worker)(nil)

// NewWorker creates worker metrics for the registered queues, pre-creates
// every queue/status series at zero, and registers them with reg.
func NewWorker(reg prometheus.Registerer, queues []string) (*Worker, error) {
	m := &Worker{
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "worker_inflight_jobs",
			Help:      "Handlers executing in this worker process right now.",
		}),
		handler: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "worker_handler_seconds",
			Help:      "Handler wall time from start to return, by outcome.",
			Buckets:   JobBuckets,
		}, []string{"queue", "status"}),
		queues: make(map[string]struct{}, len(queues)),
	}
	for _, queue := range queues {
		if _, exists := m.queues[queue]; exists {
			continue
		}
		m.queues[queue] = struct{}{}
		for _, status := range []workersdk.HandlerStatus{
			workersdk.HandlerOK, workersdk.HandlerError, workersdk.HandlerPanic,
		} {
			m.handler.WithLabelValues(queue, string(status))
		}
	}
	for _, collector := range []prometheus.Collector{m.inflight, m.handler} {
		if err := reg.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register worker: %w", err)
		}
	}
	return m, nil
}

// HandlerStarted records one registered handler beginning execution.
func (m *Worker) HandlerStarted(queue string) {
	if _, ok := m.queues[queue]; ok {
		m.inflight.Inc()
	}
}

// HandlerFinished records one registered handler's outcome and wall time.
func (m *Worker) HandlerFinished(queue string, status workersdk.HandlerStatus, d time.Duration) {
	if _, ok := m.queues[queue]; !ok {
		return
	}
	m.inflight.Dec()
	if d < 0 {
		d = 0
	}
	m.handler.WithLabelValues(queue, string(status)).Observe(d.Seconds())
}
