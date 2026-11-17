package broker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// ack confirms a delivery through the ack batcher, or directly when there is
// none. A pair its batch didn't apply (store.ErrNotApplied: its row was
// locked, or its guard failed) is applied here with the one-row Ack, on this
// call's goroutine and under its own deadline, after the batch has committed.
// One locked row therefore waits alone: its batch's other callers have their
// answers already, and no flight is held. Either way the call returns only
// after the statement that applied it committed, so docs/semantics.md §11.3
// is unchanged.
func (s *Service) ack(ctx context.Context, id, leaseID uuid.UUID) error {
	if s.acks != nil {
		err := s.acks.do(ctx, store.LeaseRef{ID: id, LeaseID: leaseID})
		if !errors.Is(err, store.ErrNotApplied) {
			return err
		}
	}
	return s.store.Ack(ctx, id, leaseID)
}

// extend is ack's counterpart for heartbeats.
func (s *Service) extend(ctx context.Context, id, leaseID uuid.UUID, dur time.Duration) error {
	if s.extends != nil {
		err := s.extends.do(ctx, store.Extension{LeaseRef: store.LeaseRef{ID: id, LeaseID: leaseID}, Duration: dur})
		if !errors.Is(err, store.ErrNotApplied) {
			return err
		}
	}
	return s.store.ExtendLease(ctx, id, leaseID, dur)
}

// waitConfirmations returns once no confirmation statement is running, or
// with ctx's error if ctx ends first.
func (s *Service) waitConfirmations(ctx context.Context) error {
	if s.acks == nil {
		return nil
	}
	if err := s.acks.wait(ctx); err != nil {
		return err
	}
	return s.extends.wait(ctx)
}
