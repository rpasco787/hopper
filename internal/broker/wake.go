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
// a wakeup is one more non-blocking send to it.
//
// One enqueue adds one job, so it wakes one stream: the others would lease,
// find nothing, and park again, and with a stream per worker that cost grew
// with the fleet. Streams say when they are parked in their idle wait; only a
// parked stream is known to have credit and to be waiting for work. A wake no
// parked stream can take is not lost: it moves a generation that every stream
// reads before it leases and checks before it parks (generation, park).
//
// Enqueue announces a queue. A parked stream on this broker takes it at once,
// and that is all. If none could, the queue joins a pending set that the
// announcer sends to every other broker through the store. The announcer sends
// one Announce at a time and takes the whole set each time, so under load
// announcements coalesce into one statement per round trip instead of one per
// job.
type wakeups struct {
	mu      sync.Mutex
	streams map[chan<- struct{}]*wakeStream
	gen     uint64              // moves when a wake found no parked stream to take it
	pending map[string]struct{} // queues not yet announced to other brokers
	kick    chan struct{}       // buffered 1: pending is non-empty
}

// wakeStream is what the registry knows about one lease stream.
type wakeStream struct {
	queues []string
	parked bool // waiting for work after an empty lease, so a wake can use it
}

func newWakeups() *wakeups {
	return &wakeups{
		streams: make(map[chan<- struct{}]*wakeStream),
		pending: make(map[string]struct{}),
		kick:    make(chan struct{}, 1),
	}
}

// subscribe registers ch for queues and returns the function that removes it.
func (w *wakeups) subscribe(ch chan<- struct{}, queues []string) func() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.streams[ch] = &wakeStream{queues: queues}
	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.streams, ch)
	}
}

// generation is read by a stream just before it leases. It hands the value to
// park, which refuses if an unserved wake has moved it since.
func (w *wakeups) generation() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen
}

// park records that the stream behind ch is about to wait for work after a
// lease that began at generation gen. It reports false, and parks nothing, if
// a wake has gone unserved since then: that lease may have read a snapshot
// from before the job was committed, so the stream must lease again.
func (w *wakeups) park(ch chan<- struct{}, gen uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen != w.gen {
		return false
	}
	if st, ok := w.streams[ch]; ok {
		st.parked = true
	}
	return true
}

// unpark records that the stream's wait is over, by a signal, its poll timer
// or its context. One that was unsubscribed in the meantime is ignored.
func (w *wakeups) unpark(ch chan<- struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st, ok := w.streams[ch]; ok {
		st.parked = false
	}
}

// wake tells this broker's streams that queue may have work, and reports
// whether a parked stream took it.
//
// It signals one parked stream that serves queue, and unparks it so the next
// wake picks another. With none parked the streams are busy, and a wake nobody
// takes could be missed by one that is between an empty lease and its wait, so
// it moves the generation instead: any stream that began a lease before it will
// lease again rather than park. No stream is signalled for nothing.
//
// The empty queue wakes every stream and moves the generation; the listener
// sends it when it (re)connects, because announcements made while it was not
// listening are lost.
//
// A stream whose channel already holds a signal has not looked yet, and will
// lease after the commit that made this call, so it counts as woken.
func (w *wakeups) wake(queue string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if queue == "" {
		w.gen++
		for ch := range w.streams {
			signal(ch)
		}
		return len(w.streams) > 0
	}
	for ch, st := range w.streams {
		if st.parked && slices.Contains(st.queues, queue) {
			st.parked = false
			signal(ch)
			return true
		}
	}
	w.gen++
	return false
}

func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default: // already signalled; the stream has not looked yet
	}
}

// announce wakes one of this broker's streams for queue. If none could take
// it, the queue is scheduled for the other brokers: one of their idle streams
// may. A job a local stream took needs no announcement, since that stream
// leases it now, and the announcement would wake further streams, here and
// there, to find it gone.
func (w *wakeups) announce(queue string) {
	if w.wake(queue) {
		return
	}
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
		err := s.cfg.Wakeups.Listen(ctx, func(queue string) {
			s.wakes.wake(queue)
			if queue == "" {
				// Report readiness after the reconnect wake has been delivered.
				s.cfg.Logger.DebugContext(ctx, "wakeup listener ready")
			}
		})
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
