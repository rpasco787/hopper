package broker

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// batcherFor is the index of id's shard's batcher, or -1 when id names no
// shard, which the one-row call then answers NotFound.
func (s *Service) batcherFor(id uuid.UUID) int {
	if s.router == nil {
		return 0
	}
	i, err := s.router.ShardOf(id)
	if err != nil {
		return -1
	}
	return i
}

// ack confirms a delivery through its shard's ack batcher, or directly when
// there is none. There is one batcher per shard, so a shard that has stopped
// answering holds up no other shard's confirmations. A pair its batch didn't
// apply (store.ErrNotApplied: its row was locked, or its guard failed) is
// applied here with the one-row Ack, on this call's goroutine and under its
// own deadline, after the batch has committed. One locked row therefore waits
// alone: its batch's other callers have their answers already, and no flight
// is held. Either way the call returns only after the statement that applied
// it committed, so docs/semantics.md §11.3 is unchanged.
func (s *Service) ack(ctx context.Context, id, leaseID uuid.UUID) error {
	if i := s.batcherFor(id); s.acks != nil && i >= 0 {
		err := s.acks[i].do(ctx, store.LeaseRef{ID: id, LeaseID: leaseID})
		if !errors.Is(err, store.ErrNotApplied) {
			return err
		}
	}
	return s.store.Ack(ctx, id, leaseID)
}

// extend is ack's counterpart for heartbeats.
func (s *Service) extend(ctx context.Context, id, leaseID uuid.UUID, dur time.Duration) error {
	if i := s.batcherFor(id); s.extends != nil && i >= 0 {
		err := s.extends[i].do(ctx, store.Extension{LeaseRef: store.LeaseRef{ID: id, LeaseID: leaseID}, Duration: dur})
		if !errors.Is(err, store.ErrNotApplied) {
			return err
		}
	}
	return s.store.ExtendLease(ctx, id, leaseID, dur)
}

// waitConfirmations returns once no confirmation statement is running on any
// shard's batcher, or with ctx's error if ctx ends first.
func (s *Service) waitConfirmations(ctx context.Context) error {
	for _, b := range s.acks {
		if err := b.wait(ctx); err != nil {
			return err
		}
	}
	for _, b := range s.extends {
		if err := b.wait(ctx); err != nil {
			return err
		}
	}
	return nil
}
