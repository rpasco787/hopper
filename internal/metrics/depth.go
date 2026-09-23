package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rpasco787/hopper/internal/store"
)

// DepthFreshness is how old a broker's last successful sample may be before
// dashboards exclude its depth: three sample intervals (docs/semantics.md §14).
// The dashboard's PromQL literal must match; the dashboard test checks it.
const DepthFreshness = 15 * time.Second

const defaultDepthInterval = 5 * time.Second

// DepthSampler refreshes a Broker's queue depth from src every Interval
// (default 5s), sampling once immediately. Every broker runs one, so every
// broker exports the same series: drop stale instances, then max by (queue,
// state); never sum (docs/semantics.md §14).
type DepthSampler struct {
	Source   store.QueueDepthSource
	Metrics  *Broker
	Interval time.Duration
	Logger   *slog.Logger
}

func (s *DepthSampler) interval() time.Duration {
	if s.Interval <= 0 {
		return defaultDepthInterval
	}
	return s.Interval
}

func (s *DepthSampler) logger() *slog.Logger {
	if s.Logger == nil {
		return slog.Default()
	}
	return s.Logger
}

// Run samples immediately and then on every interval until ctx is cancelled.
func (s *DepthSampler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval())
	defer ticker.Stop()
	for {
		if err := s.Once(ctx); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Once reads and publishes one complete sample. Errors leave the last good
// snapshot intact and are logged here so direct and looped calls behave alike.
func (s *DepthSampler) Once(ctx context.Context) error {
	sampleCtx, cancel := context.WithTimeout(ctx, s.interval())
	defer cancel()
	rows, err := s.Source.QueueDepths(sampleCtx)
	if err != nil {
		err = fmt.Errorf("metrics: sample queue depth: %w", err)
		s.logger().WarnContext(ctx, "queue depth sample failed", "err", err)
		return err
	}
	if err := s.Metrics.UpdateDepth(rows, time.Now()); err != nil {
		err = fmt.Errorf("metrics: sample queue depth: %w", err)
		s.logger().WarnContext(ctx, "queue depth sample failed", "err", err)
		return err
	}
	return nil
}

// depthStates are reported for every labelled queue, zero-filled, so a queue
// that drains reads 0 instead of disappearing (docs/semantics.md §14).
var depthStates = []store.State{
	store.StateScheduled, store.StateAvailable, store.StateLeased, store.StateDead,
}

type depthKey struct {
	queue string
	state store.State
}

// depthSnapshot is immutable once published; Collect reads a pointer to it.
type depthSnapshot struct {
	counts    map[depthKey]int64 // keyed by label (allowlisted queue or _other)
	sampledAt time.Time
}

type depthCollector struct {
	desc, sampledDesc *prometheus.Desc
	labels            []string // allowlist plus _other, fixed at NewBroker
	mu                sync.RWMutex
	snap              *depthSnapshot // nil until the first successful sample
}

func newDepthCollector(labels []string) *depthCollector {
	return &depthCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "queue_depth"),
			"Jobs in each non-terminal work state at the last successful sample.",
			[]string{"queue", "state"}, nil,
		),
		sampledDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "", "queue_depth_last_sample_timestamp_seconds"),
			"Unix time of this broker's last successful queue depth sample.",
			nil, nil,
		),
		labels: append([]string(nil), labels...),
	}
}

func (c *depthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
	ch <- c.sampledDesc
}

func (c *depthCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	snap := c.snap
	c.mu.RUnlock()
	for _, queue := range c.labels {
		for _, state := range depthStates {
			var count int64
			if snap != nil {
				count = snap.counts[depthKey{queue: queue, state: state}]
			}
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(count), queue, string(state))
		}
	}
	if snap != nil {
		ch <- prometheus.MustNewConstMetric(c.sampledDesc, prometheus.GaugeValue, float64(snap.sampledAt.UnixNano())/1e9)
	}
}

func validDepthState(state store.State) bool {
	switch state {
	case store.StateScheduled, store.StateAvailable, store.StateLeased, store.StateDead:
		return true
	default:
		return false
	}
}

// UpdateDepth validates and atomically publishes one complete depth snapshot.
func (b *Broker) UpdateDepth(rows []store.QueueDepth, at time.Time) error {
	seen := make(map[depthKey]struct{}, len(rows))
	counts := make(map[depthKey]int64, len(rows))
	for _, row := range rows {
		key := depthKey{queue: row.Queue, state: row.State}
		switch {
		case row.Queue == "":
			return fmt.Errorf("queue is empty")
		case !validDepthState(row.State):
			return fmt.Errorf("invalid state %q", row.State)
		case row.Count < 0:
			return fmt.Errorf("negative count")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate queue and state")
		}
		seen[key] = struct{}{}
		counts[depthKey{queue: b.label(row.Queue), state: row.State}] += row.Count
	}
	next := &depthSnapshot{counts: counts, sampledAt: at}
	b.depth.mu.Lock()
	b.depth.snap = next
	b.depth.mu.Unlock()
	return nil
}
