//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/test/accounting"
)

// Bounds on the producer.
const (
	producerSenders = 16              // calls in flight at most
	producerCall    = 2 * time.Second // one Enqueue; a call unanswered by then is in doubt
	producerBackoff = 100 * time.Millisecond
)

// call is one Enqueue call as the producer saw it.
type call struct {
	start, end time.Time
	ok, dedup  bool      // answered; answered with an existing job
	id         uuid.UUID // the job, when ok
	code       string    // the status code, when not ok
}

// producer enqueues keyed sleep jobs, one Enqueue call per job, at a steady
// total rate, and records every call. A failed call is in doubt: its insert
// may have committed. It is retried under the same key until a call is
// answered, so a retry finds the committed job, deduplicated, rather than
// inserting another (docs/semantics.md §8). Stopping abandons the keys in
// flight, which were never acknowledged.
type producer struct {
	cl      *client.Client
	payload json.RawMessage
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu    sync.Mutex
	calls []call
}

func startProducer(cl *client.Client, run string, rate, ms int) *producer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &producer{cl: cl, payload: json.RawMessage(fmt.Sprintf(`{"ms":%d}`, ms)), cancel: cancel}
	keys := make(chan int)
	p.wg.Go(func() {
		defer close(keys)
		tick := time.NewTicker(time.Second / time.Duration(rate))
		defer tick.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			select {
			case <-ctx.Done():
				return
			case keys <- i:
			}
		}
	})
	for range producerSenders {
		p.wg.Go(func() {
			for i := range keys {
				p.produce(ctx, fmt.Sprintf("%s-%d", run, i))
			}
		})
	}
	return p
}

// produce enqueues one key until a call is answered or the producer stops.
func (p *producer) produce(ctx context.Context, key string) {
	for {
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, producerCall)
		res, err := p.cl.Enqueue(cctx, client.Job{Queue: "sleep", Payload: p.payload, MaxAttempts: 20, IdempotencyKey: key})
		cancel()
		if ctx.Err() != nil {
			return // stopping: this answer is not counted
		}
		c := call{start: start, end: time.Now(), ok: err == nil}
		if err == nil {
			c.id, c.dedup = res.ID, res.Deduplicated
		} else {
			c.code = status.Code(err).String()
		}
		p.mu.Lock()
		p.calls = append(p.calls, c)
		p.mu.Unlock()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(producerBackoff):
		}
	}
}

// stop ends production and waits for every call to return. A second call is
// harmless.
func (p *producer) stop() {
	p.cancel()
	p.wg.Wait()
}

func (p *producer) snapshot() []call {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]call(nil), p.calls...)
}

func (p *producer) ackedCount() int {
	n := 0
	for _, c := range p.snapshot() {
		if c.ok {
			n++
		}
	}
	return n
}

// recovery is the first failed call after t, and the end of the first
// answered call that began after that failure: how long producers could not
// enqueue. A call answered just after the first failure but begun before it
// may have been served by the old primary, so it doesn't count. Either is zero
// until it happens.
func (p *producer) recovery(t time.Time) (firstFailure, firstAck time.Time) {
	calls := p.snapshot()
	for _, c := range calls {
		if !c.ok && c.end.After(t) && (firstFailure.IsZero() || c.end.Before(firstFailure)) {
			firstFailure = c.end
		}
	}
	if firstFailure.IsZero() {
		return
	}
	for _, c := range calls {
		if c.ok && c.start.After(firstFailure) && (firstAck.IsZero() || c.end.Before(firstAck)) {
			firstAck = c.end
		}
	}
	return
}

// awaitRecovery waits until producers have failed after t and been answered
// again.
func (p *producer) awaitRecovery(t time.Time, timeout time.Duration) (firstFailure, firstAck time.Time, ok bool) {
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if f, a := p.recovery(t); !a.IsZero() {
			return f, a, true
		}
	}
	f, a := p.recovery(t)
	return f, a, false
}

// produced is what the producer recorded, for the proof's checks.
type produced struct {
	acked      []accounting.Acknowledged
	fresh      map[uuid.UUID]bool // jobs some call acknowledged as newly inserted
	deduped    int                // answers that found their job already there
	failed     map[string]int     // failed calls by status code
	window     int                // answered calls begun and ended inside the window
	windowDups int                // of those, answered as deduplicated
}

// results summarizes every call. The window, when not zero, is the
// replication pause: from its start to the crash.
func (p *producer) results(from, to time.Time) produced {
	r := produced{fresh: map[uuid.UUID]bool{}, failed: map[string]int{}}
	for _, c := range p.snapshot() {
		if !c.ok {
			r.failed[c.code]++
			continue
		}
		r.acked = append(r.acked, accounting.Acknowledged{ID: c.id, At: c.end})
		if c.dedup {
			r.deduped++
		} else {
			r.fresh[c.id] = true
		}
		if !from.IsZero() && c.start.After(from) && !c.end.After(to) {
			r.window++
			if c.dedup {
				r.windowDups++
			}
		}
	}
	return r
}
