package reaper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeStore returns a scripted sequence of counts, then zeros. It records
// every limit it was called with so tests can see the batch size.
type fakeStore struct {
	mu     sync.Mutex
	script []int
	err    error
	limits []int
	called chan struct{} // closed on first call
	once   sync.Once
}

func newFake(script ...int) *fakeStore {
	return &fakeStore{script: script, called: make(chan struct{})}
}

func (f *fakeStore) ReapExpiredLeases(_ context.Context, limit int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.once.Do(func() { close(f.called) })
	f.limits = append(f.limits, limit)
	if f.err != nil {
		return 0, f.err
	}
	if len(f.script) == 0 {
		return 0, nil
	}
	n := f.script[0]
	f.script = f.script[1:]
	return n, nil
}

func (f *fakeStore) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.limits)
}

func TestSweepDrainsFullBatches(t *testing.T) {
	// Two full batches then a short one: the sweep keeps going until the
	// store comes back short, so a backlog clears in one tick.
	f := newFake(10, 10, 3)
	r := &Reaper{Store: f, BatchSize: 10}

	n, err := r.Sweep(context.Background())
	require.NoError(t, err)
	require.Equal(t, 23, n)
	require.Equal(t, []int{10, 10, 10}, f.limits)
}

func TestSweepStopsOnEmpty(t *testing.T) {
	f := newFake()
	r := &Reaper{Store: f, BatchSize: 10}

	n, err := r.Sweep(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, 1, f.calls())
}

func TestSweepReturnsStoreError(t *testing.T) {
	f := newFake()
	f.err = errors.New("db down")
	r := &Reaper{Store: f, BatchSize: 10}

	_, err := r.Sweep(context.Background())
	require.ErrorContains(t, err, "db down")
}

func TestRunSweepsImmediatelyThenOnTick(t *testing.T) {
	f := newFake()
	r := &Reaper{Store: f, Interval: 20 * time.Millisecond, BatchSize: 5}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-f.called:
	case <-time.After(time.Second):
		t.Fatal("Run did not sweep on start")
	}
	// Give a few ticks a chance to fire, then stop.
	time.Sleep(75 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.GreaterOrEqual(t, f.calls(), 3, "one immediate sweep plus at least two ticks")
	require.Equal(t, 5, f.limits[0], "BatchSize is passed through as the limit")
}

func TestRunKeepsGoingAfterStoreError(t *testing.T) {
	f := newFake()
	f.err = errors.New("transient")
	r := &Reaper{Store: f, Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	err := r.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, f.calls(), 3, "an error is logged, not fatal")
}

func TestDefaults(t *testing.T) {
	r := &Reaper{Store: newFake()}
	require.Equal(t, time.Second, r.interval())
	require.Equal(t, 100, r.batchSize())
}
