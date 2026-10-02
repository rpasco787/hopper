package broker

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// maxBatchSize bounds one EnqueueBatch. It is not a correctness limit — the
// loop would work at any size — but an unbounded batch is an unbounded
// transaction-less write loop holding a connection, and a client that wants
// 100k jobs should send 100 requests so the broker can interleave other work.
const maxBatchSize = 1000

func (s *Service) Enqueue(ctx context.Context, req *hopperv1.EnqueueRequest) (*hopperv1.EnqueueResponse, error) {
	params, err := specToParams(req.GetJob())
	if err != nil {
		return nil, err
	}
	job, inserted, err := s.store.Enqueue(ctx, params)
	if err != nil {
		return nil, s.fail(ctx, "Enqueue", err)
	}
	p, err := jobToProto(job)
	if err != nil {
		return nil, s.fail(ctx, "Enqueue", err)
	}
	s.cfg.Logger.DebugContext(ctx, "job enqueued",
		"job_id", job.ID.String(), "queue", job.Queue, "deduplicated", !inserted)
	s.announceIfLeasable(job, inserted)
	// inserted is inverted: the store reports what it did, the API reports the
	// fact the caller acts on.
	return &hopperv1.EnqueueResponse{Job: p, Deduplicated: !inserted}, nil
}

// EnqueueBatch reduces client RPC round trips, not database round trips.
// Broker prevalidation failures write nothing. A database rejection, error,
// or response-size failure can leave a committed prefix (or all jobs).
// Stable keys deduplicate retries only while the original rows still exist.
func (s *Service) EnqueueBatch(ctx context.Context, req *hopperv1.EnqueueBatchRequest) (*hopperv1.EnqueueBatchResponse, error) {
	specs := req.GetJobs()
	switch {
	case len(specs) == 0:
		return nil, status.Error(codes.InvalidArgument, "jobs is required")
	case len(specs) > maxBatchSize:
		return nil, status.Errorf(codes.InvalidArgument,
			"batch of %d exceeds the limit of %d", len(specs), maxBatchSize)
	}

	params := make([]store.EnqueueParams, len(specs))
	for i, spec := range specs {
		p, err := specToParams(spec)
		if err != nil {
			s.cfg.Logger.DebugContext(ctx, "enqueue batch rejected",
				"index", i, "code", codes.InvalidArgument.String())
			return nil, status.Errorf(codes.InvalidArgument, "jobs[%d]: %v", i, status.Convert(err).Message())
		}
		params[i] = p
	}

	results := make([]*hopperv1.EnqueueResponse, 0, len(params))
	for i, p := range params {
		job, inserted, err := s.store.Enqueue(ctx, p)
		if err != nil {
			// The items before this one are enqueued and stay enqueued. Say
			// how far we got so the operator reading the log is not guessing.
			s.cfg.Logger.ErrorContext(ctx, "enqueue batch failed part way",
				"index", i, "enqueued", len(results), "of", len(params), "err", err)
			return nil, s.fail(ctx, "EnqueueBatch", err)
		}
		proto, err := jobToProto(job)
		if err != nil {
			return nil, s.fail(ctx, "EnqueueBatch", err)
		}
		s.cfg.Logger.DebugContext(ctx, "job enqueued",
			"job_id", job.ID.String(), "queue", job.Queue, "deduplicated", !inserted)
		s.announceIfLeasable(job, inserted)
		results = append(results, &hopperv1.EnqueueResponse{Job: proto, Deduplicated: !inserted})
	}
	return &hopperv1.EnqueueBatchResponse{Results: results}, nil
}

// announceIfLeasable wakes the streams that serve a job's queue when this call
// made the job leasable. A delayed job is not leasable until the promoter
// moves it, and a deduplicated call added nothing.
func (s *Service) announceIfLeasable(job store.Job, inserted bool) {
	if inserted && job.State == store.StateAvailable {
		if s.cfg.Wakeups == nil {
			// Local-only services have no announcer to drain pending queues.
			s.wakes.wake(job.Queue)
		} else {
			s.wakes.announce(job.Queue)
		}
	}
}
