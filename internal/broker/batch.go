package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// batchTimeout caps one batch's statement, for callers with no deadline of
// their own, so a hung database cannot hold a flight forever. Within it, a
// statement runs while any of its callers still waits for it, and is
// cancelled when the last one leaves (docs/design/confirmation-batching.md).
const batchTimeout = 10 * time.Second

// batcher is group commit: concurrent calls share one statement and one
// commit (docs/design/confirmation-batching.md).
//
// There is no timer. A call that finds fewer than flights batches running
// starts one, which takes everything queued (at most max) and runs it; calls
// that arrive meanwhile queue for the next. A lone call therefore waits for
// nothing, and a busy broker batches as much as one round trip lets queue.
type batcher[T any] struct {
	run     func(context.Context, []T) []error
	max     int
	flights int
	timeout time.Duration

	mu      sync.Mutex
	queue   []*waiter[T]
	running int           // flushes started and not yet finished; guarded by mu
	idle    chan struct{} // closed while running is zero; guarded by mu
}

type waiter[T any] struct {
	ctx  context.Context
	item T
	err  error
	done chan struct{} // closed once err is set
}

func newBatcher[T any](max, flights int, run func(context.Context, []T) []error) *batcher[T] {
	idle := make(chan struct{})
	close(idle)
	return &batcher[T]{run: run, max: max, flights: flights, timeout: batchTimeout, idle: idle}
}

// do queues item and waits for the statement that carries it. It returns
// the item's own result, or ctx's error if the caller stops waiting first.
// In that case the item may still be applied, which is safe because every
// confirmation is idempotent under its lease token (docs/semantics.md §5).
func (b *batcher[T]) do(ctx context.Context, item T) error {
	w := &waiter[T]{ctx: ctx, item: item, done: make(chan struct{})}
	b.mu.Lock()
	b.queue = append(b.queue, w)
	start := b.running < b.flights
	if start {
		if b.running == 0 {
			b.idle = make(chan struct{})
		}
		b.running++
	}
	b.mu.Unlock()
	if start {
		go b.flush()
	}
	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wait returns once no flush is running, or with ctx's error if ctx ends
// first. Server.Shutdown calls it after the gRPC server has stopped, so the
// pool is never closed under a statement still running.
func (b *batcher[T]) wait(ctx context.Context) error {
	b.mu.Lock()
	idle := b.idle
	b.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flush runs batches until it finds the queue empty.
func (b *batcher[T]) flush() {
	for {
		batch := b.take()
		if batch == nil {
			return
		}
		errs := b.runBatch(batch)
		for i, w := range batch {
			w.err = errs[i]
			close(w.done)
		}
	}
}

// runBatch runs one statement for batch. Its context ends when the last
// caller in the batch stops waiting, or at timeout. A statement nobody waits
// for is cancelled, and one somebody waits for is never cut short by another
// caller leaving. A deadline copied from the callers would not do: it
// carries no cancellation.
func (b *batcher[T]) runBatch(batch []*waiter[T]) []error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	var waiting atomic.Int64
	waiting.Store(int64(len(batch)))
	items := make([]T, len(batch))
	stops := make([]func() bool, len(batch))
	for i, w := range batch {
		items[i] = w.item
		stops[i] = context.AfterFunc(w.ctx, func() {
			if waiting.Add(-1) == 0 {
				cancel()
			}
		})
	}
	errs := b.run(ctx, items)
	for _, stop := range stops {
		stop()
	}
	if len(errs) != len(batch) {
		err := fmt.Errorf("broker: a batch returned %d results for %d items", len(errs), len(batch))
		errs = make([]error, len(batch))
		for i := range errs {
			errs[i] = err
		}
	}
	return errs
}

// take removes up to max waiters that are still waiting. One whose caller
// has gone is answered here and left out: nobody would hear its result, and
// its client retries. Finding nothing, take gives up this flush's slot under
// the same lock that do checks, so no call is ever queued with no flush to
// take it.
func (b *batcher[T]) take() []*waiter[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	var batch []*waiter[T]
	n := 0
	for n < len(b.queue) && len(batch) < b.max {
		w := b.queue[n]
		n++
		if err := w.ctx.Err(); err != nil {
			w.err = err
			close(w.done)
			continue
		}
		batch = append(batch, w)
	}
	clear(b.queue[:n])
	b.queue = b.queue[n:]
	if len(batch) == 0 {
		b.running--
		if b.running == 0 {
			close(b.idle)
		}
		return nil
	}
	return batch
}
