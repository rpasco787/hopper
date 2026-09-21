package broker

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/rpasco787/hopper/internal/store"
)

// fakeStore is a store.Store whose methods are supplied per test.
//
// The embedded interface is the trick and it is deliberate: it gives fakeStore
// the whole method set for free, and because the embedded value is nil, any
// method a test did not script panics the moment it is called. A fake that
// silently returned zero values would let a handler call the wrong method and
// still pass.
//
// It exists because the interesting half of this package is error handling,
// and ErrLeaseLost is trivial to inject here and genuinely difficult to
// provoke against a real Postgres on demand. The real store is exercised
// end-to-end in test/integration.
type fakeStore struct {
	store.Store

	enqueueFn  func(context.Context, store.EnqueueParams) (store.Job, bool, error)
	getJobFn   func(context.Context, uuid.UUID) (store.Job, error)
	cancelFn   func(context.Context, uuid.UUID) (store.Job, error)
	ackFn      func(context.Context, uuid.UUID, uuid.UUID) error
	nackFn     func(context.Context, uuid.UUID, uuid.UUID, string) error
	extendFn   func(context.Context, uuid.UUID, uuid.UUID, time.Duration) error
	leaseFn    func(context.Context, []string, int, time.Duration, string) ([]store.Job, error)
	listJobsFn func(context.Context, string, []store.State, int, *uuid.UUID) ([]store.Job, error)
}

func (f *fakeStore) Enqueue(ctx context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	return f.enqueueFn(ctx, p)
}
func (f *fakeStore) GetJob(ctx context.Context, id uuid.UUID) (store.Job, error) {
	return f.getJobFn(ctx, id)
}
func (f *fakeStore) Cancel(ctx context.Context, id uuid.UUID) (store.Job, error) {
	return f.cancelFn(ctx, id)
}
func (f *fakeStore) Ack(ctx context.Context, id, leaseID uuid.UUID) error {
	return f.ackFn(ctx, id, leaseID)
}
func (f *fakeStore) Nack(ctx context.Context, id, leaseID uuid.UUID, msg string) error {
	return f.nackFn(ctx, id, leaseID, msg)
}
func (f *fakeStore) ExtendLease(ctx context.Context, id, leaseID uuid.UUID, d time.Duration) error {
	return f.extendFn(ctx, id, leaseID, d)
}
func (f *fakeStore) Lease(ctx context.Context, queues []string, n int, d time.Duration, worker string) ([]store.Job, error) {
	return f.leaseFn(ctx, queues, n, d, worker)
}

// sampleJob is a plausible stored row for tests that only care that the row
// made it back out over the wire intact.
func sampleJob(queue string) store.Job {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return store.Job{
		ID:          uuid.New(),
		Queue:       queue,
		Payload:     []byte(`{"n":1}`),
		State:       store.StateAvailable,
		Priority:    0,
		RunAt:       now,
		MaxAttempts: 5,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}
