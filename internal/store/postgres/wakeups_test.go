package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/store"
)

// listener runs Listen in the background and collects what it reports.
type listener struct {
	woken  chan string
	done   chan error
	cancel context.CancelFunc
}

func listen(t *testing.T, s *Store) *listener {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	l := &listener{woken: make(chan string, 100), done: make(chan error, 1), cancel: cancel}
	go func() { l.done <- s.Listen(ctx, func(q string) { l.woken <- q }) }()
	t.Cleanup(cancel)
	return l
}

// next is the listener's next wakeup.
func (l *listener) next(t *testing.T) string {
	t.Helper()
	select {
	case q := <-l.woken:
		return q
	case err := <-l.done:
		t.Fatalf("listener stopped: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("no wakeup within 5s")
	}
	return ""
}

// stopped is the error Listen returned.
func (l *listener) stopped(t *testing.T) error {
	t.Helper()
	select {
	case err := <-l.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Listen did not return within 5s")
		return nil
	}
}

func listeningSessions(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`select count(*) from pg_stat_activity where query = 'listen hopper_available'`).Scan(&n))
	return n
}

func TestAnnounceReachesEveryListener(t *testing.T) {
	ctx := ctxT(t)
	s := newStore(t)
	a, b := listen(t, s), listen(t, s)
	require.Equal(t, "", a.next(t), "a new listener first wakes every queue")
	require.Equal(t, "", b.next(t))

	require.NoError(t, s.Announce(ctx, []string{"q1", "q2"}))
	for _, l := range []*listener{a, b} {
		require.ElementsMatch(t, []string{"q1", "q2"}, []string{l.next(t), l.next(t)})
	}
	require.NoError(t, s.Announce(ctx, nil), "announcing nothing is a no-op")
}

// A listening session must not outlive Listen: back in the pool it would keep
// collecting notifications for whoever borrowed it next.
func TestListenEndsWithItsContextAndKeepsNoSession(t *testing.T) {
	s := newStore(t)
	before := pool.Stat().AcquiredConns()
	l := listen(t, s)
	require.Equal(t, "", l.next(t))
	require.Equal(t, before+1, pool.Stat().AcquiredConns(), "Listen holds one connection")
	require.Equal(t, 1, listeningSessions(t))

	l.cancel()
	require.ErrorIs(t, l.stopped(t), context.Canceled)
	require.Equal(t, before, pool.Stat().AcquiredConns())
	require.Eventually(t, func() bool { return listeningSessions(t) == 0 },
		5*time.Second, 50*time.Millisecond, "the listening session was closed, not pooled")
}

// A restart or failover ends the listening session. Listen reports it as
// unavailability, so the broker retries rather than giving up.
func TestListenReportsATerminatedSession(t *testing.T) {
	ctx := ctxT(t)
	newStore(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["application_name"] = "hopper-listen"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)

	l := listen(t, New(p))
	require.Equal(t, "", l.next(t))
	_, err = pool.Exec(ctx,
		`select pg_terminate_backend(pid) from pg_stat_activity where application_name = 'hopper-listen'`)
	require.NoError(t, err)
	require.ErrorIs(t, l.stopped(t), store.ErrUnavailable)
}
