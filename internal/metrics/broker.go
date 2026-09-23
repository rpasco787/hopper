package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rpasco787/hopper/internal/store"
)

// Broker is the broker process's hopper metrics. It implements store.Observer,
// so wrapping the store with it counts every transition the process commits,
// including the reaper's and the scheduler's, which never pass through an RPC.
type Broker struct {
	enqueued, completed, failed, dead, expired *prometheus.CounterVec
	wait, run                                  *prometheus.HistogramVec
	labels                                     map[string]struct{} // allowlist; read-only after NewBroker
}

var _ store.Observer = (*Broker)(nil)

// OtherQueue labels every queue not on the allowlist (docs/semantics.md §14).
const OtherQueue = "_other"

// NewBroker creates the broker metrics, pre-creates every series for queues
// and OtherQueue at zero, and registers them with reg. It returns the
// registration error rather than panicking.
func NewBroker(reg prometheus.Registerer, queues []string) (*Broker, error) {
	b := &Broker{
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_enqueued_total",
			Help:      "Rows inserted by Enqueue calls this process committed; deduplicated calls are excluded.",
		}, []string{"queue"}),
		completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_completed_total",
			Help:      "Jobs moved to completed by an Ack this process committed.",
		}, []string{"queue"}),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_failed_total",
			Help:      "Failed attempts: Nacks this process committed, rescheduled or dead-lettered.",
		}, []string{"queue"}),
		dead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "jobs_dead_total",
			Help:      "Jobs moved to dead by a Nack or lease expiry this process committed.",
		}, []string{"queue"}),
		expired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "lease_expirations_total",
			Help:      "Expired leases the reaper in this process committed taking back.",
		}, []string{"queue"}),
		wait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_wait_seconds",
			Help:      "Seconds from due time to first lease for jobs this process leased.",
			Buckets:   JobBuckets,
		}, []string{"queue"}),
		run: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_run_seconds",
			Help:      "Seconds from lease to Ack for jobs this process completed.",
			Buckets:   JobBuckets,
		}, []string{"queue"}),
		labels: make(map[string]struct{}, len(queues)+1),
	}
	for _, queue := range queues {
		b.labels[queue] = struct{}{}
	}
	b.labels[OtherQueue] = struct{}{}

	for queue := range b.labels {
		b.enqueued.WithLabelValues(queue)
		b.completed.WithLabelValues(queue)
		b.failed.WithLabelValues(queue)
		b.dead.WithLabelValues(queue)
		b.expired.WithLabelValues(queue)
		b.wait.WithLabelValues(queue)
		b.run.WithLabelValues(queue)
	}
	for _, collector := range []prometheus.Collector{
		b.enqueued, b.completed, b.failed, b.dead, b.expired, b.wait, b.run,
	} {
		if err := reg.Register(collector); err != nil {
			return nil, fmt.Errorf("metrics: register broker: %w", err)
		}
	}
	return b, nil
}

func (b *Broker) label(queue string) string {
	if _, ok := b.labels[queue]; ok {
		return queue
	}
	return OtherQueue
}

// Enqueued records one row inserted by Enqueue.
func (b *Broker) Enqueued(queue string) {
	b.enqueued.WithLabelValues(b.label(queue)).Inc()
}

// Leased records due-time wait for first deliveries only.
func (b *Broker) Leased(jobs []store.Job) {
	for _, job := range jobs {
		if job.Attempts != 1 || job.LeasedAt == nil {
			continue
		}
		wait := job.LeasedAt.Sub(job.RunAt)
		// Both instants come from the database, so this cannot normally be
		// negative; clamp rather than emit an invalid duration if it is.
		if wait < 0 {
			wait = 0
		}
		b.wait.WithLabelValues(b.label(job.Queue)).Observe(wait.Seconds())
	}
}

// Completed records one committed Ack and its lease-to-Ack duration when the
// row carried a lease timestamp.
func (b *Broker) Completed(queue string, leasedAt, finishedAt time.Time) {
	label := b.label(queue)
	b.completed.WithLabelValues(label).Inc()
	if leasedAt.IsZero() {
		return
	}
	run := finishedAt.Sub(leasedAt)
	if run < 0 {
		run = 0
	}
	b.run.WithLabelValues(label).Observe(run.Seconds())
}

// Nacked records one failed attempt and, when applicable, a transition to dead.
func (b *Broker) Nacked(queue string, dead bool) {
	label := b.label(queue)
	b.failed.WithLabelValues(label).Inc()
	if dead {
		b.dead.WithLabelValues(label).Inc()
	}
}

// LeaseExpired records one lease reclaimed by the reaper and, when
// applicable, a transition to dead.
func (b *Broker) LeaseExpired(queue string, dead bool) {
	label := b.label(queue)
	b.expired.WithLabelValues(label).Inc()
	if dead {
		b.dead.WithLabelValues(label).Inc()
	}
}
