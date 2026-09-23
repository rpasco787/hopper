package worker

import "time"

// Observer receives handler lifecycle events, for metrics. It is called
// synchronously on the delivery's goroutine, so implementations must be safe
// for concurrent use, fast and non-blocking, and must not panic: the SDK does
// not recover an observer. A nil Observer in Options observes nothing.
//
// Time spent in HandlerFinished delays the Ack or Nack and uses up lease time.
type Observer interface {
	// HandlerStarted is called once the handler has been accepted to run, and
	// never for a delivery that was nacked unstarted.
	HandlerStarted(queue string)
	// HandlerFinished is called when the handler returns, with how it returned
	// and its wall time from start to return.
	HandlerFinished(queue string, status HandlerStatus, d time.Duration)
}

// HandlerStatus describes how a handler returned.
type HandlerStatus string

const (
	HandlerOK    HandlerStatus = "ok"
	HandlerError HandlerStatus = "error"
	HandlerPanic HandlerStatus = "panic"
)

type nopObserver struct{}

func (nopObserver) HandlerStarted(string) {}

func (nopObserver) HandlerFinished(string, HandlerStatus, time.Duration) {}
