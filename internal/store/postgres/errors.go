package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rpasco787/hopper/internal/store"
)

// wrap names the store operation that failed and, when the failure was
// PostgreSQL being unreachable rather than refusing the statement, marks it
// with store.ErrUnavailable so the broker can answer UNAVAILABLE instead of
// INTERNAL (docs/semantics.md §5). Every error this package returns from a
// database call goes through here; TestEveryStoreErrorGoesThroughWrap keeps
// it that way.
func wrap(op string, err error) error {
	if unavailable(err) {
		return fmt.Errorf("store: %s: %w: %w", op, store.ErrUnavailable, err)
	}
	return fmt.Errorf("store: %s: %w", op, err)
}

// unavailable reports whether err says the database could not be reached or
// dropped the connection, rather than rejecting the statement.
//
//   - A failed connection attempt, for any reason, including a server that is
//     starting up, shutting down or refusing connections. Nothing was sent.
//   - A server error in class 08 (connection exception), 57P01-57P03 (the
//     server is terminating this session, crashing, or cannot accept it yet)
//     or 53300 (too many connections).
//   - A transport failure on an open connection: a network error, EOF, or a
//     connection pgx has already closed.
//
// Cancellation and deadlines are not unavailability: they belong to the
// caller, and the broker reports them as such. Everything else — a constraint,
// invalid input, a serialization failure — is the database answering, and
// stays an internal error.
func unavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var connect *pgconn.ConnectError
	if errors.As(err, &connect) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P01", "57P02", "57P03", "53300":
			return true
		}
		return len(pgErr.Code) == 5 && pgErr.Code[:2] == "08"
	}
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, pgconn.ErrConnClosed)
}
