package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// lifecycle owns shutdown.
//
// There is exactly one deadline. Cancelling Run means stop accepting work and
// drain; a permanent consume failure means the same. The first transition to
// draining records shutdownAt + ShutdownTimeout, and opening a stream, sending
// on it, half-closing it, waiting for handlers and retrying confirmations all
// share that one instant. There is deliberately no second timeout after stream
// teardown: two sequential budgets would let a worker take twice as long as it
// was configured to, which is how a pod gets SIGKILLed mid-handler.
//
// It also owns the counts. They are kept separately because they call for
// different responses: handlers still running at the deadline means the budget
// is too small for the slowest handler, while confirmations still pending means
// jobs that almost certainly ran are about to be redelivered.
type lifecycle struct {
	log     *slog.Logger
	timeout time.Duration

	// forceCtx carries the caller's values but not its cancellation, so a
	// handler can still read a trace id out of the context that started the
	// worker without being cancelled the instant the caller is. Its own
	// cancellation is the forced-shutdown signal, and everything the SDK does
	// on the network descends from it.
	forceCtx context.Context
	force    context.CancelCauseFunc

	// drainSignal is closed when drain begins. The stream writer selects on it
	// to half-close; nothing else may close it.
	drainSignal chan struct{}
	// drained is closed when the last active delivery finishes during drain.
	drained chan struct{}
	// stopped ends the caller-cancellation watcher when Run returns.
	stopped chan struct{}

	mu             sync.Mutex
	draining       bool
	deadline       time.Time
	active         int
	acquiring      int
	handlers       int
	confirming     int
	intakeFinished bool
	drainedClosed  bool
	stopOnce       sync.Once
	// deadlineFired records that the budget ran out, and deadlineSnap what was
	// still in flight at that instant. The verdict has to be this snapshot
	// rather than a later reading of the counters: an abandoned delivery
	// decrements them on its way out, so a second look can show an empty,
	// apparently clean worker that has in fact just dropped a job.
	deadlineFired bool
	deadlineSnap  counts
}

// counts is a consistent snapshot of what is in flight.
type counts struct {
	deliveries, acquiring, handlers, confirming int
}

// newLifecycle installs cancellation handling. It is called before anything
// dials or opens a stream: a caller who cancels during the dial must get the
// same bounded drain as one who cancels an hour later.
func newLifecycle(ctx context.Context, timeout time.Duration, log *slog.Logger) *lifecycle {
	forceCtx, force := context.WithCancelCause(context.WithoutCancel(ctx))
	l := &lifecycle{
		log:         log,
		timeout:     timeout,
		forceCtx:    forceCtx,
		force:       force,
		drainSignal: make(chan struct{}),
		drained:     make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	// A context that is already cancelled must put the worker in drain before
	// newLifecycle returns, not whenever the watcher happens to be scheduled.
	// Otherwise whether a doomed worker opens a lease stream — and leases jobs
	// it will immediately abandon — depends on goroutine timing.
	if ctx.Err() != nil {
		l.beginDrain("caller cancelled")
		return l
	}
	go l.watch(ctx)
	return l
}

func (l *lifecycle) watch(ctx context.Context) {
	select {
	case <-ctx.Done():
		l.beginDrain("caller cancelled")
	case <-l.stopped:
	}
}

// beginDrain starts the drain and is safe to call from anywhere, any number of
// times. Only the first call sets the deadline: a later caller must not be able
// to extend a shutdown that is already running out of time.
func (l *lifecycle) beginDrain(reason string) {
	l.mu.Lock()
	if l.draining {
		l.mu.Unlock()
		return
	}
	l.draining = true
	l.deadline = time.Now().Add(l.timeout)
	snap := l.countsLocked()
	deadline := l.deadline
	l.mu.Unlock()

	close(l.drainSignal)
	// Enforcement is armed from the absolute deadline before logging. A slow
	// or blocked handler must not be able to extend the shutdown budget merely
	// by delaying this diagnostic.
	go l.enforceDeadline(deadline)
	// Task 11 reads this line out of the worker process's stdout to prove the
	// signal landed while a handler was actually running, so the snapshot has
	// to be the synchronized one taken above, not a second look from here.
	l.log.Info("shutdown started",
		"reason", reason,
		"deadline", deadline.Format(time.RFC3339Nano),
		"timeout", l.timeout.String(),
		"active_deliveries", snap.deliveries,
		"running_handlers", snap.handlers)
}

// enforceDeadline is what makes the budget real. It cannot wait for the drain
// to finish, because the drain itself may be blocked on a broker that never
// answered a half-close; the timer has to be able to cancel the transport out
// from under it.
func (l *lifecycle) enforceDeadline(deadline time.Time) {
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-timer.C:
		// One synchronized snapshot, taken before anything is cancelled, serves
		// both the log line and await's verdict. Reading the counters twice
		// would let the two disagree about the same shutdown.
		l.mu.Lock()
		l.deadlineFired = true
		l.deadlineSnap = l.countsLocked()
		snap := l.deadlineSnap
		l.mu.Unlock()

		l.force(ErrShuttingDown)
		l.log.Warn("shutdown deadline reached", "counts", snap)
	case <-l.forceCtx.Done():
	}
}

// abandonedAtDeadline reports what was in flight when the budget ran out, and
// whether it ran out at all.
func (l *lifecycle) abandonedAtDeadline() (counts, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.deadlineSnap, l.deadlineFired
}

// await blocks until everything has drained or the deadline passes. It returns
// an error wrapping ErrAbandoned when work was still in flight at the deadline.
func (l *lifecycle) await() error {
	l.beginDrain("run returned")
	// consume calls await only after the stream reader has stopped, so no new
	// delivery can begin beyond this point. Before that point a half-closing
	// stream may still yield buffered deliveries even though drain has begun.
	l.finishIntake()
	select {
	case <-l.drained:
	case <-l.forceCtx.Done():
	}
	// Either branch can win *after* the budget has already run out: abandoning
	// the last delivery zeroes the counters and closes drained in the same
	// decrement, so "drained" does not prove the drain was clean. Only the
	// snapshot taken when the deadline fired distinguishes a worker that
	// finished from one that gave up, and reporting success for the latter is
	// how a process exits zero having dropped a job.
	snap, fired := l.abandonedAtDeadline()
	if !fired || snap.deliveries == 0 {
		return nil
	}
	return &abandonedError{
		deliveries: snap.deliveries,
		handlers:   snap.handlers,
		acquiring:  snap.acquiring,
		confirming: snap.confirming,
	}
}

func (l *lifecycle) finishIntake() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.intakeFinished {
		return
	}
	l.intakeFinished = true
	if l.active == 0 {
		l.closeDrainedLocked()
	}
}

// stop releases the lifecycle. It cancels anything still attached to the force
// context, which is the last line of defence against an SDK goroutine
// outliving Run.
func (l *lifecycle) stop() {
	l.stopOnce.Do(func() {
		close(l.stopped)
		l.force(ErrShuttingDown)
	})
}

func (l *lifecycle) isDraining() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.draining
}

// shutdownDeadline reports the shared deadline, if drain has begun.
func (l *lifecycle) shutdownDeadline() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.deadline, l.draining
}

func (l *lifecycle) counts() counts {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.countsLocked()
}

func (l *lifecycle) countsLocked() counts {
	return counts{
		deliveries: l.active,
		acquiring:  l.acquiring,
		handlers:   l.handlers,
		confirming: l.confirming,
	}
}

func (l *lifecycle) closeDrainedLocked() {
	if !l.drainedClosed {
		l.drainedClosed = true
		close(l.drained)
	}
}

// phase is what a delivery is spending its capacity token on. Every phase is
// active delivery work: a delivery holds its token from the moment it is read
// off the stream until its confirmation ends, because a job whose outcome is
// still unknown has not freed a slot for anything else.
type phase int

const (
	phaseNone phase = iota
	phaseAcquiring
	phaseRunning
	phaseConfirming
)

// work is one delivery's claim on the lifecycle. It exists so a delivery that
// ends early — malformed, refused, abandoned — still decrements exactly the
// counters it incremented, exactly once.
type work struct {
	lc  *lifecycle
	mu  sync.Mutex
	cur phase
}

// begin registers a new active delivery, in its acquisition phase.
func (l *lifecycle) begin() *work {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active++
	l.acquiring++
	return &work{lc: l, cur: phaseAcquiring}
}

// acceptHandler is the linearization point between shutdown and user code.
//
// It has to be one atomic decision under the same lock that beginDrain takes.
// Checking a flag and then starting a handler is two decisions, and a drain
// that lands between them starts a handler the worker has already promised not
// to start. Once acceptance wins, that handler is in flight and shutdown waits
// for it; once drain wins, user code never runs and the job is nacked unstarted.
func (w *work) acceptHandler() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur != phaseAcquiring {
		return false
	}
	w.lc.mu.Lock()
	defer w.lc.mu.Unlock()
	if w.lc.draining {
		return false
	}
	w.lc.acquiring--
	w.lc.handlers++
	w.cur = phaseRunning
	return true
}

// confirming moves this delivery into its confirmation phase. Handler time and
// confirmation time are counted separately so a future metric cannot label one
// as the other.
func (w *work) confirming() {
	w.to(phaseConfirming)
}

func (w *work) to(next phase) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == phaseNone || w.cur == next {
		return
	}
	w.lc.mu.Lock()
	defer w.lc.mu.Unlock()
	w.lc.countOf(w.cur, -1)
	w.lc.countOf(next, +1)
	w.cur = next
}

// end finishes the delivery. It is idempotent: a delivery is finished once, no
// matter how many unwinding paths lead here.
func (w *work) end() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == phaseNone {
		return
	}
	w.lc.mu.Lock()
	defer w.lc.mu.Unlock()
	w.lc.countOf(w.cur, -1)
	w.cur = phaseNone
	w.lc.active--
	if w.lc.draining && w.lc.intakeFinished && w.lc.active == 0 {
		w.lc.closeDrainedLocked()
	}
}

// countOf adjusts the counter for one phase. Callers hold l.mu.
func (l *lifecycle) countOf(p phase, delta int) {
	switch p {
	case phaseAcquiring:
		l.acquiring += delta
	case phaseRunning:
		l.handlers += delta
	case phaseConfirming:
		l.confirming += delta
	}
}
