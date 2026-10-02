package broker

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"
)

// Bounds on the wakeup machinery. Announcing and listening are hints, so
// neither ever blocks an RPC: an announcement that cannot be sent, or a
// listener that is down, costs an idle stream one poll interval.
const (
	announceTimeout = 2 * time.Second
	listenRetryMin  = 100 * time.Millisecond
	listenRetryMax  = 5 * time.Second
)

// wakeups tells idle lease streams that a queue they serve may have work, so
// they lease now instead of at their next poll (docs/semantics.md §9).
//
// A stream registers the channel its idle wait already selects on for credit;
// a wakeup is one more non-blocking send to it. Enqueue announces a queue: the
// streams on this broker are woken at once, and the queue is added to a
// pending set that the announcer sends to every other broker through the
// store. The announcer sends one Announce at a time and takes the whole set
// each time, so under load announcements coalesce into one statement per
// round trip instead of one per job.
type wakeups struct {
	mu      sync.Mutex
	streams map[chan<- struct{}][]string // a stream's wake channel and its queues
	pending map[string]struct{}          // queues not yet announced to other brokers
	kick    chan struct{}                // buffered 1: pending is non-empty
}

func newWakeups() *wakeups {
	return &wakeups{
		streams: make(map[chan<- struct{}][]string),
		pending: make(map[string]struct{}),
		kick:    make(chan struct{}, 1),
	}
}

// subscribe registers ch for queues and returns the function that removes it.
func (w *wakeups) subscribe(ch chan<- struct{}, queues []string) func() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.streams[ch] = queues
	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.streams, ch)
	}
}

// wake signals every stream that serves queue. The empty queue wakes every
// stream; the listener sends it when it (re)connects, because announcements
// made while it was not listening are lost.
func (w *wakeups) wake(queue string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for ch, queues := range w.streams {
		if queue == "" || slices.Contains(queues, queue) {
			select {
			case ch <- struct{}{}:
			default: // already signalled; the stream has not looked yet
			}
		}
	}
}

// announce wakes this broker's streams for queue and schedules the
// announcement to the others.
func (w *wakeups) announce(queue string) {
	w.wake(queue)
	w.mu.Lock()
	w.pending[queue] = struct{}{}
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// take empties the pending set and returns it, sorted.
func (w *wakeups) take() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	queues := slices.Sorted(maps.Keys(w.pending))
	clear(w.pending)
	return queues
}

// RunWakeups sends this broker's announcements to the others and delivers
// theirs to its lease streams, until ctx ends. Without Config.Wakeups there is
// nothing to run: streams on this broker are still woken by its own enqueues,
// and everything else waits for the poll.
func (s *Service) RunWakeups(ctx context.Context) error {
	if s.cfg.Wakeups == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	var wg sync.WaitGroup
	wg.Go(func() { s.announceLoop(ctx) })
	wg.Go(func() { s.listenLoop(ctx) })
	wg.Wait()
	return ctx.Err()
}

func (s *Service) announceLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wakes.kick:
		}
		queues := s.wakes.take()
		if len(queues) == 0 {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, announceTimeout)
		err := s.cfg.Wakeups.Announce(actx, queues)
		cancel()
		if err != nil && ctx.Err() == nil {
			// Debug, not warn: the enqueue that caused it succeeded, the other
			// brokers will poll, and a database outage is already reported
			// by every RPC that meets it.
			s.cfg.Logger.DebugContext(ctx, "wakeup announcement failed; other brokers will poll",
				"queues", queues, "err", err)
		}
	}
}

func (s *Service) listenLoop(ctx context.Context) {
	delay := listenRetryMin
	for {
		started := time.Now()
		err := s.cfg.Wakeups.Listen(ctx, s.wakes.wake)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > listenRetryMax {
			delay = listenRetryMin // it was up for a while: this is a new failure
		}
		s.cfg.Logger.WarnContext(ctx, "wakeup listener stopped; lease streams poll until it reconnects",
			"err", err, "retry_in", delay.String())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(2*delay, listenRetryMax)
	}
}
