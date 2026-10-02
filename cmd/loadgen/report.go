package main

import (
	"fmt"
	"io"
	"math"
	"slices"
	"time"
)

// Summary is a distribution in milliseconds. Percentiles are exact
// nearest-rank values over every sample, not estimates: a run holds at most a
// few hundred thousand samples, which sort in milliseconds.
type Summary struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50_ms"`
	P90 float64 `json:"p90_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

// summarize sorts samples in place and reads its percentiles.
func summarize(samples []time.Duration) Summary {
	if len(samples) == 0 {
		return Summary{}
	}
	slices.Sort(samples)
	return Summary{
		N:   len(samples),
		P50: ms(rank(samples, 50)),
		P90: ms(rank(samples, 90)),
		P99: ms(rank(samples, 99)),
		Max: ms(samples[len(samples)-1]),
	}
}

// rank is the nearest-rank percentile of sorted: the smallest sample with at
// least p percent of the samples at or below it.
func rank(sorted []time.Duration, p float64) time.Duration {
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[max(i, 0)]
}

func ms(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Microsecond)) / 1000
}

// Report is everything one run measured. It is the JSON loadgen prints with
// -json, so field names are part of the benchmark record.
type Report struct {
	Run          string   `json:"run"`
	Brokers      []string `json:"brokers"`
	Queue        string   `json:"queue"`
	Producers    int      `json:"producers"`
	Batch        int      `json:"batch"`
	WorkMs       int64    `json:"work_ms"`
	PayloadBytes int      `json:"payload_bytes"`
	// OfferedRate is the target in jobs per second; 0 means unthrottled.
	OfferedRate float64 `json:"offered_rate"`

	Jobs   int64 `json:"jobs"`
	Calls  int64 `json:"calls"`
	Errors int64 `json:"errors"`
	// FirstError is the first failed call's error, so a failed run says why.
	FirstError string `json:"first_error,omitempty"`
	// ProduceSeconds runs from the first scheduled call to the last answer.
	ProduceSeconds float64 `json:"produce_seconds"`
	EnqueueRate    float64 `json:"enqueue_rate"`
	// Enqueue is per call, measured from when the call was due under the
	// offered rate, so a producer that fell behind is charged for it.
	Enqueue Summary `json:"enqueue"`
	// MaxLagMs is how late the latest call was sent relative to its schedule.
	MaxLagMs float64 `json:"max_lag_ms"`

	// The rest is filled only when the run waited for its jobs.
	Waited     bool  `json:"waited"`
	Finished   int64 `json:"finished"`
	Unfinished int64 `json:"unfinished"`
	// Retried counts jobs that took more than one lease.
	Retried int64 `json:"retried"`
	// Pickup is leased_at - created_at; EndToEnd is finished_at - created_at.
	// Both are the database's clock at both ends.
	Pickup   Summary `json:"pickup"`
	EndToEnd Summary `json:"end_to_end"`
	// Throughput is finished jobs over the span from the first insert to the
	// last completion.
	Throughput float64 `json:"throughput"`
}

// Failed reports whether the run must not be quoted as a result.
func (r *Report) Failed() bool {
	return r.Errors > 0 || r.Unfinished > 0 || (r.Waited && r.Finished != r.Jobs)
}

// writeSummary prints the one-line human summary.
func (r *Report) writeSummary(w io.Writer) {
	fmt.Fprintf(w, "run=%s jobs=%d calls=%d errors=%d enqueue_rate=%.0f/s enqueue_ms p50=%.2f p99=%.2f max_lag_ms=%.1f",
		r.Run, r.Jobs, r.Calls, r.Errors, r.EnqueueRate, r.Enqueue.P50, r.Enqueue.P99, r.MaxLagMs)
	if r.Waited {
		fmt.Fprintf(w, " | finished=%d unfinished=%d retried=%d pickup_ms p50=%.1f p99=%.1f e2e_ms p50=%.1f p99=%.1f throughput=%.0f/s",
			r.Finished, r.Unfinished, r.Retried, r.Pickup.P50, r.Pickup.P99, r.EndToEnd.P50, r.EndToEnd.P99, r.Throughput)
	}
	fmt.Fprintln(w)
}
