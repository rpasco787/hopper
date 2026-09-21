package broker

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// ListJobs pages a queue. The page token is the last id of the previous page,
// which is a cursor the client can hold across broker restarts precisely
// because it is derived from the data and not from server state.
func (s *Service) ListJobs(ctx context.Context, req *hopperv1.ListJobsRequest) (*hopperv1.ListJobsResponse, error) {
	if req.GetQueue() == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	limit := int(req.GetLimit())
	switch {
	case limit < 0:
		return nil, status.Error(codes.InvalidArgument, "limit must not be negative")
	case limit == 0:
		limit = DefaultListLimit
	case limit > MaxListLimit:
		// Clamp rather than reject: a caller asking for too much wants as much
		// as it can have, and the next_page_token tells it there is more.
		limit = MaxListLimit
	}

	states := make([]store.State, 0, len(req.GetStates()))
	for _, p := range req.GetStates() {
		st, err := stateFromProto(p)
		if err != nil {
			return nil, err
		}
		states = append(states, st)
	}

	var after *uuid.UUID
	if tok := req.GetPageToken(); tok != "" {
		id, err := parseID("page_token", tok)
		if err != nil {
			return nil, err
		}
		after = &id
	}

	jobs, err := s.store.ListJobs(ctx, req.GetQueue(), states, limit, after)
	if err != nil {
		return nil, s.fail(ctx, "ListJobs", err)
	}

	out := make([]*hopperv1.Job, 0, len(jobs))
	for _, j := range jobs {
		p, err := jobToProto(j)
		if err != nil {
			return nil, s.fail(ctx, "ListJobs", err)
		}
		out = append(out, p)
	}

	resp := &hopperv1.ListJobsResponse{Jobs: out}
	// A short page is the end. A full page might be, but the client finds out
	// by asking once more — which is cheaper than counting the whole table to
	// answer it here.
	if len(jobs) == limit {
		resp.NextPageToken = jobs[len(jobs)-1].ID.String()
	}
	return resp, nil
}
