package sweep

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeBatch struct {
	mu     sync.Mutex
	script []int
	err    error
	limits []int
	called chan struct{}
	once   sync.Once
}

func newFake(script ...int) *fakeBatch {
	return &fakeBatch{script: script, called: make(chan struct{})}
}

func (f *fakeBatch) run(_ context.Context, limit int) (int, error) {
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

func (f *fakeBatch) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.limits)
}

func TestOnceDrainsFullBatches(t *testing.T) {
	f := newFake(10, 10, 3)
	l := &Loop{Name: "test", Batch: f.run, BatchSize: 10}
	n, err := l.Once(context.Background())
	require.NoError(t, err)
	require.Equal(t, 23, n)
	require.Equal(t, []int{10, 10, 10}, f.limits)
}

func TestOnceStopsOnEmpty(t *testing.T) {
	f := newFake()
	l := &Loop{Name: "test", Batch: f.run, BatchSize: 10}
	n, err := l.Once(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, 1, f.calls())
}

func TestOnceReturnsStoreError(t *testing.T) {
	f := newFake()
	f.err = errors.New("db down")
	l := &Loop{Name: "test", Batch: f.run, BatchSize: 10}
	_, err := l.Once(context.Background())
	require.ErrorContains(t, err, "db down")
}

func TestRunSweepsImmediatelyThenOnTick(t *testing.T) {
	f := newFake()
	l := &Loop{Name: "test", Batch: f.run, Interval: 20 * time.Millisecond, BatchSize: 5}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	select {
	case <-f.called:
	case <-time.After(time.Second):
		t.Fatal("Run did not sweep on start")
	}
	time.Sleep(75 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
	require.GreaterOrEqual(t, f.calls(), 3)
	require.Equal(t, 5, f.limits[0])
}

func TestRunKeepsGoingAfterStoreError(t *testing.T) {
	f := newFake()
	f.err = errors.New("transient")
	l := &Loop{Name: "test", Batch: f.run, Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := l.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, f.calls(), 3)
}

func TestDefaults(t *testing.T) {
	l := &Loop{Batch: newFake().run}
	require.Equal(t, time.Second, l.interval())
	require.Equal(t, 100, l.batchSize())
	require.Equal(t, "sweep", l.name())
}

type fakeStore struct {
	mu       sync.Mutex
	reaped   []int
	promoted []int
	deleted  []int
	cutoffs  []time.Time
}

func (f *fakeStore) ReapExpiredLeases(_ context.Context, limit int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reaped = append(f.reaped, limit)
	return 0, nil
}

func (f *fakeStore) PromoteDue(_ context.Context, limit int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoted = append(f.promoted, limit)
	return 0, nil
}

func (f *fakeStore) DeleteFinished(_ context.Context, cutoff time.Time, limit int) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cutoffs = append(f.cutoffs, cutoff)
	f.deleted = append(f.deleted, limit)
	return 0, nil
}

func TestConstructorsWireTheRightMethod(t *testing.T) {
	f := &fakeStore{}
	ctx := context.Background()
	_, err := Reaper(f).Once(ctx)
	require.NoError(t, err)
	_, err = Promoter(f).Once(ctx)
	require.NoError(t, err)
	before := time.Now()
	_, err = Janitor(f, 24*time.Hour).Once(ctx)
	require.NoError(t, err)
	require.Equal(t, []int{100}, f.reaped)
	require.Equal(t, []int{100}, f.promoted)
	require.Equal(t, []int{1000}, f.deleted)
	require.Len(t, f.cutoffs, 1)
	require.WithinDuration(t, before.Add(-24*time.Hour), f.cutoffs[0], 2*time.Second)
}

func TestAllReturnsTheThreeBrokerLoops(t *testing.T) {
	loops := All(&fakeStore{}, time.Hour)
	require.Len(t, loops, 3)
	names := make([]string, 0, len(loops))
	for _, l := range loops {
		names = append(names, l.Name)
		require.NotNil(t, l.Batch, "%s has no work", l.Name)
	}
	require.ElementsMatch(t, []string{"reaper", "promoter", "janitor"}, names)
}
