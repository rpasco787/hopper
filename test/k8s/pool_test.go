//go:build k8s

package k8s

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPoolWhileSummarizesThePause needs no cluster.
func TestPoolWhileSummarizesThePause(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(int64(1000+s), 0) }
	samples := []poolSample{
		{At: at(3), Pod: "b1", Max: 16, Acquired: 9, WaitS: 4.0},
		{At: at(0), Pod: "b1", Max: 16, Acquired: 3, WaitS: 1.0}, // the baseline, out of order
		{At: at(1), Pod: "b1", Max: 16, Acquired: 16, WaitS: 1.5},
		{At: at(2), Pod: "b1", Max: 16, Acquired: 16, WaitS: 3.0},
		{At: at(2), Pod: "b1", Err: "timeout"},
		{At: at(4), Pod: "b1", Max: 16, Acquired: 16, WaitS: 9.0}, // after the window
		{At: at(0), Pod: "b2", Max: 16, Acquired: 2},
		{At: at(2), Pod: "b2", Max: 16, Acquired: 2},
	}
	full, wait := poolWhile(samples, at(0), at(3))
	require.InDelta(t, 2.0/3, full["b1"], 1e-9, "two of b1's three samples in (0s, 3s] found every connection acquired")
	require.InDelta(t, 3.0, wait["b1"], 1e-9, "acquires waited 4.0 - 1.0 s, counted from the baseline at 0s")
	require.InDelta(t, 0, full["b2"], 1e-9)
	require.InDelta(t, 0, wait["b2"], 1e-9)
}

// poolSample is one broker's PostgreSQL pool, read from its metrics
// (internal/metrics/pool.go).
type poolSample struct {
	At       time.Time `json:"at"`
	Pod      string    `json:"pod"`
	Max      float64   `json:"max_connections"`
	Acquired float64   `json:"acquired"`
	WaitS    float64   `json:"empty_acquire_wait_seconds_total"`
	Err      string    `json:"err,omitempty"`
}

// samplePools reads every broker's pool through the API server's pod proxy,
// every interval, until the function it returns is called, which returns the
// samples. It never fails the test: it runs on its own goroutine.
func samplePools(c *cluster, pods []string, interval time.Duration) func() []poolSample {
	var (
		mu      sync.Mutex
		samples []poolSample
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			var wg sync.WaitGroup
			for _, pod := range pods {
				wg.Go(func() {
					s := readPool(ctx, c, pod)
					mu.Lock()
					samples = append(samples, s)
					mu.Unlock()
				})
			}
			wg.Wait()
			select {
			case <-ctx.Done():
			case <-time.After(interval):
			}
		}
	}()
	return func() []poolSample {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(samples)
	}
}

// readPool reads one broker's pool metrics.
func readPool(ctx context.Context, c *cluster, pod string) poolSample {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	s := poolSample{At: time.Now(), Pod: pod}
	out, err := c.output(ctx, "get", "--raw", "/api/v1/namespaces/"+c.namespace+"/pods/"+pod+":9091/proxy/metrics")
	if err != nil {
		s.Err = err.Error()
		return s
	}
	for line := range strings.Lines(out) {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		switch name {
		case "hopper_db_pool_max_connections":
			s.Max = v
		case `hopper_db_pool_connections{state="acquired"}`:
			s.Acquired = v
		case "hopper_db_pool_empty_acquire_wait_seconds_total":
			s.WaitS = v
		}
	}
	return s
}

// poolWhile summarizes each broker's pool over (from, to]: the share of its
// samples that found every connection acquired, and the seconds acquires
// spent waiting for a connection, counted from its last sample at or before
// from.
func poolWhile(samples []poolSample, from, to time.Time) (full, wait map[string]float64) {
	samples = slices.Clone(samples)
	slices.SortFunc(samples, func(a, b poolSample) int { return a.At.Compare(b.At) })
	full, wait = map[string]float64{}, map[string]float64{}
	base, last := map[string]float64{}, map[string]float64{}
	in, atMax := map[string]int{}, map[string]int{}
	for _, s := range samples {
		switch {
		case s.Err != "":
		case !s.At.After(from):
			base[s.Pod] = s.WaitS
		case !s.At.After(to):
			in[s.Pod]++
			if s.Max > 0 && s.Acquired >= s.Max {
				atMax[s.Pod]++
			}
			last[s.Pod] = s.WaitS
		}
	}
	for pod, k := range in {
		full[pod] = float64(atMax[pod]) / float64(k)
		wait[pod] = last[pod] - base[pod]
	}
	return full, wait
}
