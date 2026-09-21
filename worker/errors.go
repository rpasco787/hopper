package worker

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// The errors a caller can branch on. The lease-related ones are also the
// cancellation *causes* of a handler's context, so a handler that wants to know
// why it was cancelled asks context.Cause and compares with errors.Is.
var (
	// ErrNoHandlers is returned by Run when nothing was registered. A worker
	// with no handlers would subscribe to no queues, which the broker rejects,
	// so this is caught locally before dialing.
	ErrNoHandlers = errors.New("worker: no handlers registered")

	// ErrLeaseLost means the broker refused a call under this delivery's lease
	// token: the reaper took the job back, or someone else holds it now.
	ErrLeaseLost = errors.New("worker: lease lost")

	// ErrLeaseExpired means the SDK's own conservative deadline for this lease
	// passed. Unlike ErrLeaseLost it is a local conclusion, reached without
	// asking the broker, so it can be reported while the network is down.
	ErrLeaseExpired = errors.New("worker: lease expired")

	// ErrJobCancelled means a client cancelled the job while it was running.
	ErrJobCancelled = errors.New("worker: job cancelled")

	// ErrShuttingDown cancels handler contexts at the forced-shutdown deadline.
	ErrShuttingDown = errors.New("worker: shutting down")

	// ErrAbandoned is returned by Run when the shutdown deadline passed with
	// work still in flight. The jobs behind it are not lost: their leases
	// expire and the broker redelivers them under the normal retry policy.
	ErrAbandoned = errors.New("worker: shutdown deadline reached with work in flight")
)

// abandonedError carries what was still in flight at the deadline. The counts
// are separate because they call for different responses: handlers still
// running means the shutdown budget is too small for the slowest handler,
// whereas pending confirmations means jobs that very likely ran will be
// redelivered. It is unexported: callers branch with errors.Is(err,
// ErrAbandoned) and read the detail from the message.
type abandonedError struct {
	deliveries, handlers, acquiring, confirming int
}

func (e *abandonedError) Error() string {
	return fmt.Sprintf(
		"worker: shutdown deadline reached with %d active deliveries (%d running handlers, %d acquiring a lease, %d awaiting confirmation)",
		e.deliveries, e.handlers, e.acquiring, e.confirming)
}

func (e *abandonedError) Unwrap() error { return ErrAbandoned }

// maxErrorBytes bounds what the SDK sends as a Nack message. The column is
// unbounded text, but an unbounded error is a way to fill a disk with one
// looping handler, and last_error exists to be read by a human.
const maxErrorBytes = 4096

// truncationSuffix is counted inside maxErrorBytes, not added on top of it.
const truncationSuffix = "… (truncated)"

// sanitizeError makes a handler's error safe to store and to log.
//
// Three things can go wrong with arbitrary text on the way to Postgres. A NUL
// byte is valid UTF-8 and valid JSON but cannot be stored in a text column, so
// it would fail the Nack rather than the job. Invalid UTF-8 is rejected by the
// server encoding for the same reason. And unbounded length is a resource
// problem of its own. All three are the SDK's to fix, because the handler that
// produced the error is usually not the code that will read it.
func sanitizeError(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if len(s) <= maxErrorBytes {
		return s
	}
	// Cut back to a rune start so the result never ends in half a character.
	keep := maxErrorBytes - len(truncationSuffix)
	for keep > 0 && !utf8.RuneStart(s[keep]) {
		keep--
	}
	return s[:keep] + truncationSuffix
}
