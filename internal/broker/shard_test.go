package broker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// twoShards batches confirmations; shard 1's statements hang until released.
type twoShards struct {
	fakeStore
	release chan struct{}
	many    []store.EnqueueParams
}

func (f *twoShards) Shards() int                       { return 2 }
func (f *twoShards) ShardOf(id uuid.UUID) (int, error) { return store.ShardOf(id, 2) }
func (f *twoShards) AckMany(ctx context.Context, refs []store.LeaseRef) []error {
	if i, _ := store.ShardOf(refs[0].ID, 2); i == 1 {
		select {
		case <-f.release:
		case <-ctx.Done():
		}
	}
	return make([]error, len(refs))
}
func (f *twoShards) ExtendLeaseMany(_ context.Context, exts []store.Extension) []error {
	return make([]error, len(exts))
}
func (f *twoShards) EnqueueMany(_ context.Context, ps []store.EnqueueParams) ([]store.EnqueueResult, error) {
	f.many = ps
	out := make([]store.EnqueueResult, len(ps))
	for i, p := range ps {
		out[i] = store.EnqueueResult{Done: true, Inserted: true,
			Job: store.Job{ID: uuid.Must(uuid.NewV7()), Queue: p.Queue, State: store.StateAvailable, Payload: []byte(`{}`)}}
	}
	return out, nil
}

func shardID(i int) uuid.UUID { return store.StampShard(uuid.Must(uuid.NewV7()), i) }

// One flight per kind makes the point sharply: with one batcher, shard 0's
// ack would queue behind shard 1's hung statement.
func TestAShardThatHangsHoldsNoOtherShardsAcks(t *testing.T) {
	f := &twoShards{release: make(chan struct{})}
	svc := NewService(f, Config{ConfirmBatch: 64, ConfirmFlights: 1})
	go func() { _ = svc.ack(context.Background(), shardID(1), uuid.New()) }()
	time.Sleep(50 * time.Millisecond) // shard 1's flight is now running
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, svc.ack(ctx, shardID(0), uuid.New()))
	close(f.release)
}

func TestEnqueueBatchUsesTheStoresBatchEnqueuer(t *testing.T) {
	f := &twoShards{release: make(chan struct{})}
	svc := NewService(f, Config{})
	resp, err := svc.EnqueueBatch(context.Background(), &hopperv1.EnqueueBatchRequest{Jobs: []*hopperv1.JobSpec{
		{Queue: "a"}, {Queue: "b"},
	}})
	require.NoError(t, err)
	require.Len(t, resp.GetResults(), 2)
	require.Equal(t, "b", resp.GetResults()[1].GetJob().GetQueue())
	require.Len(t, f.many, 2)
}

type countingListener struct{ calls atomic.Int32 }

func (c *countingListener) Listen(ctx context.Context, wake func(string)) error {
	c.calls.Add(1)
	wake("")
	<-ctx.Done()
	return ctx.Err()
}

type nopAnnouncer struct{}

func (nopAnnouncer) Announce(context.Context, []string) error { return nil }

func TestRunWakeupsListensOnEveryListener(t *testing.T) {
	a, b := &countingListener{}, &countingListener{}
	svc := NewService(&fakeStore{}, Config{Wakeups: nopAnnouncer{}, Listeners: []store.Listener{a, b}})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = svc.RunWakeups(ctx); close(done) }()
	require.Eventually(t, func() bool { return a.calls.Load() == 1 && b.calls.Load() == 1 }, time.Second, 10*time.Millisecond)
	cancel()
	<-done
}
