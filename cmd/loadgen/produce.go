package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// produce runs the producers to completion and returns the ids of every job
// the brokers confirmed. The first failed call stops every producer: a run
// with errors is not a result, and loadgen never retries, because a call
// without idempotency keys may have committed before it failed.
func produce(ctx context.Context, cfg config, clients []api, rep *Report) map[string]struct{} {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	body := payload(cfg.workMs, cfg.payloadBytes)
	// interval is how far apart one producer's calls are due. Each call
	// carries batch jobs and producers share the rate, so together they offer
	// exactly cfg.rate jobs a second.
	var interval time.Duration
	if cfg.rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(cfg.producers*cfg.batch) / cfg.rate)
	}
	var issued atomic.Int64 // jobs claimed so far, against cfg.n
	claim := func() int {
		if cfg.n == 0 {
			return cfg.batch
		}
		end := issued.Add(int64(cfg.batch))
		start := end - int64(cfg.batch)
		if start >= cfg.n {
			return 0
		}
		return int(min(end, cfg.n) - start)
	}

	type result struct {
		ids       []string
		latencies []time.Duration
		maxLag    time.Duration
		calls     int64
		err       error
	}
	var confirmed sync.Map // job IDs must be unique across every producer and call
	results := make([]result, cfg.producers)
	start := time.Now()
	var wg sync.WaitGroup
	for p := range cfg.producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &results[p]
			c := clients[p%len(clients)]
			// Producers are staggered across one interval, so an offered rate
			// arrives as a steady stream rather than in bursts of producers.
			offset := interval * time.Duration(p) / time.Duration(cfg.producers)
			for k := 0; ctx.Err() == nil; k++ {
				due := start.Add(offset + time.Duration(k)*interval)
				// A scheduled call belongs to the run if it was due inside
				// it; an unthrottled one if it starts inside it.
				if at := due; cfg.duration > 0 {
					if interval == 0 {
						at = time.Now()
					}
					if at.Sub(start) >= cfg.duration {
						return
					}
				}
				size := claim()
				if size == 0 {
					return
				}
				if wait := time.Until(due); wait > 0 {
					timer := time.NewTimer(wait)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					}
				}
				sent := time.Now()
				if interval == 0 {
					due = sent // unthrottled: nothing was scheduled
				}
				r.maxLag = max(r.maxLag, sent.Sub(due))
				ids, err := call(ctx, cfg, c, body, p, k, size)
				r.calls++
				if err == nil {
					for _, id := range ids {
						if _, duplicate := confirmed.LoadOrStore(id, struct{}{}); duplicate {
							err = fmt.Errorf("broker confirmed duplicate job ID %q", id)
							break
						}
					}
				}
				if err != nil {
					if ctx.Err() == nil {
						r.err = fmt.Errorf("producer %d call %d: %w", p, k, err)
						cancel()
					}
					return
				}
				r.latencies = append(r.latencies, time.Since(due))
				r.ids = append(r.ids, ids...)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	ids := make(map[string]struct{})
	var latencies []time.Duration
	for _, r := range results {
		rep.Calls += r.calls
		latencies = append(latencies, r.latencies...)
		rep.MaxLagMs = max(rep.MaxLagMs, ms(r.maxLag))
		for _, id := range r.ids {
			ids[id] = struct{}{}
		}
		if r.err != nil {
			rep.Errors++
			if rep.FirstError == "" {
				rep.FirstError = r.err.Error()
			}
		}
	}
	if err := parent.Err(); err != nil && rep.Errors == 0 {
		rep.Errors++
		rep.FirstError = err.Error()
	}
	rep.Jobs = int64(len(ids))
	rep.Enqueue = summarize(latencies)
	rep.ProduceSeconds = elapsed.Seconds()
	if elapsed > 0 {
		rep.EnqueueRate = float64(rep.Jobs) / elapsed.Seconds()
	}
	return ids
}

// call makes one Enqueue or EnqueueBatch and returns the ids the broker
// confirmed. Keys, when asked for, are unique per job within the run.
func call(ctx context.Context, cfg config, c api, body []byte, producer, k, size int) ([]string, error) {
	specs := make([]*hopperv1.JobSpec, size)
	for i := range specs {
		specs[i] = &hopperv1.JobSpec{Queue: cfg.queue, Payload: body, MaxAttempts: int32(cfg.maxAttempts)}
		if cfg.keys {
			key := fmt.Sprintf("%s-%d-%d-%d", cfg.run, producer, k, i)
			specs[i].IdempotencyKey = &key
		}
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
	defer cancel()
	if cfg.batch == 1 {
		resp, err := c.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: specs[0]})
		if err != nil {
			return nil, err
		}
		if resp.GetJob().GetId() == "" {
			return nil, fmt.Errorf("enqueue response has no job ID")
		}
		return []string{resp.GetJob().GetId()}, nil
	}
	resp, err := c.EnqueueBatch(ctx, &hopperv1.EnqueueBatchRequest{Jobs: specs})
	if err != nil {
		return nil, err
	}
	if len(resp.GetResults()) != size {
		return nil, fmt.Errorf("enqueue batch confirmed %d jobs, want %d", len(resp.GetResults()), size)
	}
	seen := make(map[string]struct{}, size)
	ids := make([]string, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		id := r.GetJob().GetId()
		if id == "" {
			return nil, fmt.Errorf("enqueue batch response has no job ID")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("enqueue batch confirmed duplicate job ID %q", id)
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, nil
}
