package integration

import (
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rpasco787/hopper/internal/scheduler"
)

// freezer is a TCP proxy to PostgreSQL that can go silent without closing
// anything. Once frozen it drops every byte both ways, passes on no close,
// and holds new connections open without dialing: a partitioned path, as seen
// from both ends. Passing on no close matters. A client that gives up and
// closes its socket must not tell PostgreSQL, or the server would end the
// session at once and this would test a reset, not silence.
// client/client_test.go has the same idea for a broker.
type freezer struct {
	lis    net.Listener
	target string
	frozen atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

func startFreezer(t *testing.T, target string) *freezer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &freezer{lis: lis, target: target}
	t.Cleanup(func() {
		_ = lis.Close()
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, c := range f.conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			f.track(c)
			if f.frozen.Load() {
				continue // held open and never answered, until Cleanup
			}
			go f.pipe(c)
		}
	}()
	return f
}

func (f *freezer) addr() string { return f.lis.Addr().String() }

func (f *freezer) track(c net.Conn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conns = append(f.conns, c)
}

func (f *freezer) pipe(client net.Conn) {
	server, err := net.Dial("tcp", f.target)
	if err != nil {
		_ = client.Close()
		return
	}
	f.track(server)
	forward := func(dst, src net.Conn) {
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if err != nil {
				if !f.frozen.Load() {
					_ = dst.Close()
				}
				return
			}
			if f.frozen.Load() {
				continue // dropped, as on a dead path
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
	}
	go forward(server, client)
	go forward(client, server)
}

// dsnVia is the shared database's DSN with its host replaced by addr and every
// session tagged as name.
func dsnVia(t *testing.T, addr, name string) string {
	t.Helper()
	u, err := url.Parse(dsnWithApplicationName(t, name))
	require.NoError(t, err)
	u.Host = addr
	return u.String()
}

// awaitLogAfter waits for p to log msg at or after index from, and returns
// that entry.
func awaitLogAfter(t *testing.T, p *workerProcess, from int, within time.Duration, msg string) processLog {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, e := range p.logSnapshot()[from:] {
			if e.Message == msg {
				return e
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%q not logged within %s\n%s", msg, within, p.output())
	return processLog{}
}

// startLeaderThenStandby starts a, waits until it leads and has fired, then
// starts b, so the leader is known. It returns the seeded tick.
func startLeaderThenStandby(t *testing.T, a func() *workerProcess, b func() *workerProcess) (*workerProcess, *workerProcess, time.Time) {
	t.Helper()
	ctx := ctxT(t)
	pa := a()
	var seed time.Time
	require.Eventually(t, func() bool {
		seed = firstCronProcTick(ctx)
		return !seed.IsZero() && heldBy(ctx, []string{"hopper-broker-a"})
	}, 10*time.Second, 20*time.Millisecond, "a must lead and fire\n%s", pa.output())
	pb := b()
	require.Equal(t, "hopper-broker-a", requireLeader(t, ctx).Name, "b started second and must not lead yet")
	return pa, pb, seed
}

// TestBrokerProcessGivesUpCronWhenItsDatabasePathGoesSilent is the silent
// leader (docs/semantics.md §13). The leader's path to PostgreSQL drops every
// packet and resets nothing. Its own Check or pass runs out of time, so it
// stops acting as leader before PostgreSQL can hand the role on; PostgreSQL
// ends its idle session after the TTL; the other broker takes the role and
// fires the tick pending at the partition, once.
func TestBrokerProcessGivesUpCronWhenItsDatabasePathGoesSilent(t *testing.T) {
	silentTTL := scheduler.DefaultLeaderTTL(processSchedulerInterval)
	binary := buildBrokerProcess(t)
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	cc := pool.Config().ConnConfig
	path := startFreezer(t, net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))))
	file := writeProcessCronFile(t, cronProcessFile)
	a, b, seed := startLeaderThenStandby(t,
		func() *workerProcess {
			return startBrokerProcessWithDSN(t, binary, dsnVia(t, path.addr(), "hopper-broker-a"), file)
		},
		func() *workerProcess { return startBrokerProcess(t, binary, "hopper-broker-b", file) })

	pending := procNextRunAt(t, ctx)
	from := len(a.logSnapshot())
	path.frozen.Store(true)
	frozenAt := time.Now()

	// The client half: a's term ends on a deadline of its own.
	ended := awaitLogAfter(t, a, from, silentTTL, "scheduler term ended")
	steppedDown := time.Since(frozenAt)
	require.Regexp(t, `deadline exceeded|timeout`, ended.Err, "a's term ended for another reason")

	// The server half: PostgreSQL ends a's silent session and b wins.
	successor, latency := awaitTakeover(t, ctx, []string{"hopper-broker-b"}, frozenAt)
	require.Equal(t, "hopper-broker-b", successor)
	require.LessOrEqual(t, latency, silentTTL+processSchedulerInterval+electionAllowance, "b took over late")
	require.Less(t, steppedDown, latency, "a was still leading when b took over")
	require.Eventually(t, func() bool {
		return cronProcJobAt(ctx, pending) && procNextRunAt(t, ctx).After(pending)
	}, 2*failoverBudget, 20*time.Millisecond,
		"b must fire the tick pending at the partition (%s)\n%s", pending, b.output())
	t.Logf("partitioned: a stepped down after %s, b took over after %s",
		steppedDown.Round(time.Millisecond), latency.Round(time.Millisecond))

	b.signal(t, syscall.SIGTERM)
	require.NoError(t, b.wait(t, 10*time.Second), "b exits cleanly\n%s", b.output())
	assertScheduleIntegrityWithin(t, ctx, "cron_proc", "proc-tick", silentTTL+failoverBudget, seed, pending)
}

// TestBrokerProcessGivesUpCronWhileStopped: a SIGSTOPped leader, the paused
// leader of docs/semantics.md §13, runs no statement at all and watches no
// deadline. PostgreSQL ends its idle session after the TTL and the other
// broker takes the role. When the leader resumes it finds its session gone
// and does not take the role back.
func TestBrokerProcessGivesUpCronWhileStopped(t *testing.T) {
	silentTTL := scheduler.DefaultLeaderTTL(processSchedulerInterval)
	binary := buildBrokerProcess(t)
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)

	file := writeProcessCronFile(t, cronProcessFile)
	a, b, seed := startLeaderThenStandby(t,
		func() *workerProcess { return startBrokerProcess(t, binary, "hopper-broker-a", file) },
		func() *workerProcess { return startBrokerProcess(t, binary, "hopper-broker-b", file) })

	pending := procNextRunAt(t, ctx)
	a.signal(t, syscall.SIGSTOP)
	t.Cleanup(func() { _ = a.cmd.Process.Signal(syscall.SIGCONT) })
	stoppedAt := time.Now()

	successor, latency := awaitTakeover(t, ctx, []string{"hopper-broker-b"}, stoppedAt)
	require.Equal(t, "hopper-broker-b", successor)
	require.LessOrEqual(t, latency, silentTTL+processSchedulerInterval+electionAllowance, "b took over late")
	require.GreaterOrEqual(t, latency, silentTTL-processSchedulerInterval-electionAllowance,
		"the role moved before a had been silent for the TTL, so something other than the timeout freed it")
	require.Eventually(t, func() bool {
		return cronProcJobAt(ctx, pending) && procNextRunAt(t, ctx).After(pending)
	}, 2*failoverBudget, 20*time.Millisecond,
		"b must fire the tick pending at the stop (%s)\n%s", pending, b.output())

	from := len(a.logSnapshot())
	a.signal(t, syscall.SIGCONT)
	awaitLogAfter(t, a, from, 5*time.Second, "scheduler term ended")
	require.Equal(t, "hopper-broker-b", requireLeader(t, ctx).Name, "a must not take the role back")
	t.Logf("stopped: b took over after %s", latency.Round(time.Millisecond))

	for _, p := range []*workerProcess{a, b} {
		p.signal(t, syscall.SIGTERM)
		require.NoError(t, p.wait(t, 10*time.Second), "clean exit\n%s", p.output())
	}
	assertScheduleIntegrityWithin(t, ctx, "cron_proc", "proc-tick", silentTTL+failoverBudget, seed, pending)
}
