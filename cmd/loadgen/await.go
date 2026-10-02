package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// drainPoll is how often await asks whether the queue has drained. Each check
// is one ListJobs call with limit 1, which walks the primary key until it
// meets an unfinished job.
var drainPoll = 500 * time.Millisecond

// unfinished are the states a job can still leave on its own.
var unfinished = []hopperv1.JobState{
	hopperv1.JobState_JOB_STATE_SCHEDULED,
	hopperv1.JobState_JOB_STATE_AVAILABLE,
	hopperv1.JobState_JOB_STATE_LEASED,
}

// await waits until the queue holds no unfinished job, then reads back every
// job this run enqueued and fills the report's per-job numbers.
func await(ctx context.Context, cfg config, c api, ids map[string]struct{}, rep *Report) error {
	rep.Waited = true
	ctx, cancel := context.WithTimeout(ctx, cfg.drainTimeout)
	defer cancel()
	for {
		resp, err := listPage(ctx, cfg, c, &hopperv1.ListJobsRequest{Queue: cfg.queue, States: unfinished, Limit: 1})
		if err != nil {
			return fmt.Errorf("waiting for the queue to drain: %w", err)
		}
		if len(resp.GetJobs()) == 0 {
			break
		}
		timer := time.NewTimer(drainPoll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("queue %s did not drain within %s", cfg.queue, cfg.drainTimeout)
		}
	}

	var pickup, e2e []time.Duration
	var first, last time.Time
	seen := make(map[string]struct{}, len(ids))
	req := &hopperv1.ListJobsRequest{Queue: cfg.queue, Limit: pageLimit(cfg.payloadBytes)}
	for {
		resp, err := listPage(ctx, cfg, c, req)
		if err != nil {
			return fmt.Errorf("reading back jobs: %w", err)
		}
		for _, j := range resp.GetJobs() {
			if _, ours := ids[j.GetId()]; !ours {
				continue
			}
			if _, duplicate := seen[j.GetId()]; duplicate {
				return fmt.Errorf("reading back duplicate job ID %q", j.GetId())
			}
			seen[j.GetId()] = struct{}{}
			if j.GetAttempts() > 1 {
				rep.Retried++
			}
			if j.GetState() != hopperv1.JobState_JOB_STATE_COMPLETED {
				rep.Unfinished++
				continue
			}
			for _, ts := range []*timestamppb.Timestamp{j.GetCreatedAt(), j.GetLeasedAt(), j.GetFinishedAt()} {
				if err := ts.CheckValid(); err != nil {
					return fmt.Errorf("job %q has an invalid measurement timestamp: %w", j.GetId(), err)
				}
			}
			rep.Finished++
			created, leased, finished := j.GetCreatedAt().AsTime(), j.GetLeasedAt().AsTime(), j.GetFinishedAt().AsTime()
			pickup = append(pickup, leased.Sub(created))
			e2e = append(e2e, finished.Sub(created))
			if first.IsZero() || created.Before(first) {
				first = created
			}
			if finished.After(last) {
				last = finished
			}
		}
		if req.PageToken = resp.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	// A job the brokers confirmed but the queue no longer holds is not
	// finished either; retention or a purge would have to have removed it.
	rep.Unfinished += int64(len(ids) - len(seen))
	rep.Pickup = summarize(pickup)
	rep.EndToEnd = summarize(e2e)
	if span := last.Sub(first); span > 0 {
		rep.Throughput = float64(rep.Finished) / span.Seconds()
	}
	return nil
}

// listPage is one ListJobs call under its own deadline. A broker that is
// briefly unavailable is asked again; anything else ends the wait.
func listPage(ctx context.Context, cfg config, c api, req *hopperv1.ListJobsRequest) (*hopperv1.ListJobsResponse, error) {
	for {
		callCtx, cancel := context.WithTimeout(ctx, cfg.rpcTimeout)
		resp, err := c.ListJobs(callCtx, req)
		cancel()
		if err == nil || status.Code(err) != codes.Unavailable || ctx.Err() != nil {
			return resp, err
		}
		timer := time.NewTimer(drainPoll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

// pageLimit keeps a page of jobs, payloads included, well under the broker's
// 4 MiB message ceiling.
func pageLimit(payloadBytes int) int32 {
	const budget = 3 << 20
	const perJobOverhead = 600
	return int32(max(1, min(1000, budget/(payloadBytes+perJobOverhead))))
}
