//go:build k8s

package k8s

import (
	"bufio"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
	"github.com/rpasco787/hopper/internal/store"
	"github.com/rpasco787/hopper/test/accounting"
)

// The faults a soak injects.
const (
	faultWorkerKill    = "worker-kill"    // SIGKILL one worker's container; the kubelet restarts it in place
	faultBrokerRestart = "broker-restart" // kubectl rollout restart of the brokers
	faultFailover      = "failover"       // crash one shard's CloudNativePG primary
)

// soakSample is how often the timeline is sampled.
const soakSample = 30 * time.Second

// soakConfig is the HOPPER_SOAK_* knobs (doc.go).
type soakConfig struct {
	duration                   time.Duration
	rate                       float64 // enqueues offered per second
	inflight                   int     // calls in flight at most
	chunk                      int     // jobs per run name
	jobMS                      int
	conns                      int // client connections, spread over the brokers
	workerKillEvery            time.Duration
	brokerRestartEvery         time.Duration
	failoverAt                 float64 // fraction of the duration; 0 never
	failoverShard              int
	kill                       string // crictl-docker (kind) or crictl-debug (any cluster)
	debugNamespace, debugImage string
	drainTimeout               time.Duration
	prometheus                 string // namespace/service:port of the Prometheus with the brokers' metrics
}

func soakSettings(t *testing.T) soakConfig {
	t.Helper()
	dur := func(name, def string) time.Duration {
		v := setting(name, def)
		d, err := time.ParseDuration(v)
		require.NoError(t, err, "%s=%q", name, v)
		require.GreaterOrEqual(t, d, time.Duration(0), name)
		return d
	}
	float := func(name, def string) float64 {
		v := setting(name, def)
		f, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err, "%s=%q", name, v)
		return f
	}
	cfg := soakConfig{
		duration:           dur("HOPPER_SOAK_DURATION", "2h"),
		rate:               float("HOPPER_SOAK_RATE", "200"),
		inflight:           intSetting(t, "HOPPER_SOAK_INFLIGHT", 256),
		chunk:              intSetting(t, "HOPPER_SOAK_CHUNK", 1_000_000),
		jobMS:              intSetting(t, "HOPPER_SOAK_JOB_MS", 50),
		conns:              intSetting(t, "HOPPER_SOAK_CONNS", 4),
		workerKillEvery:    dur("HOPPER_SOAK_WORKER_KILL_EVERY", "3m"),
		brokerRestartEvery: dur("HOPPER_SOAK_BROKER_RESTART_EVERY", "15m"),
		failoverAt:         float("HOPPER_SOAK_FAILOVER_AT", "0.5"),
		failoverShard:      intSetting(t, "HOPPER_SOAK_FAILOVER_SHARD", 1),
		kill:               setting("HOPPER_SOAK_KILL", "crictl-docker"),
		debugNamespace:     setting("HOPPER_SOAK_DEBUG_NAMESPACE", "default"),
		debugImage:         setting("HOPPER_SOAK_DEBUG_IMAGE", "busybox:1.37.0"),
		drainTimeout:       dur("HOPPER_SOAK_DRAIN_TIMEOUT", "30m"),
		prometheus:         setting("HOPPER_SOAK_PROMETHEUS", "monitoring/prometheus-operated:9090"),
	}
	require.Positive(t, cfg.duration, "HOPPER_SOAK_DURATION")
	require.Positive(t, cfg.rate, "HOPPER_SOAK_RATE")
	require.True(t, cfg.failoverAt >= 0 && cfg.failoverAt < 1, "HOPPER_SOAK_FAILOVER_AT (%v) must be in [0, 1)", cfg.failoverAt)
	require.Contains(t, []string{"crictl-docker", "crictl-debug"}, cfg.kill, "HOPPER_SOAK_KILL")
	return cfg
}

// fault is one scheduled injection, at an offset from the start of production.
type fault struct {
	At   time.Duration
	Kind string
}

// faultSchedule lists a soak's faults by time: a worker kill every killEvery
// and a broker restart every restartEvery, up to and including the end of
// production, and one failover at failoverAt of the way through. Zero turns
// any of them off. Faults due at once run kill, restart, failover.
func faultSchedule(duration, killEvery, restartEvery time.Duration, failoverAt float64) []fault {
	var out []fault
	every := func(interval time.Duration, kind string) {
		for at := interval; interval > 0 && at <= duration; at += interval {
			out = append(out, fault{at, kind})
		}
	}
	every(killEvery, faultWorkerKill)
	every(restartEvery, faultBrokerRestart)
	if failoverAt > 0 && failoverAt < 1 {
		out = append(out, fault{time.Duration(float64(duration) * failoverAt), faultFailover})
	}
	rank := map[string]int{faultWorkerKill: 0, faultBrokerRestart: 1, faultFailover: 2}
	slices.SortStableFunc(out, func(a, b fault) int {
		return cmp.Or(cmp.Compare(a.At, b.At), cmp.Compare(rank[a.Kind], rank[b.Kind]))
	})
	return out
}

// TestFaultScheduleHasOneFailover needs no cluster.
func TestFaultScheduleHasOneFailover(t *testing.T) {
	m := func(minutes float64) time.Duration { return time.Duration(minutes * float64(time.Minute)) }
	require.Equal(t, []fault{
		{m(3), faultWorkerKill},
		{m(5), faultBrokerRestart},
		{m(6), faultWorkerKill},
		{m(7.5), faultFailover},
		{m(9), faultWorkerKill},
		{m(10), faultBrokerRestart},
		{m(12), faultWorkerKill},
		{m(15), faultWorkerKill},
		{m(15), faultBrokerRestart},
	}, faultSchedule(m(15), m(3), m(5), 0.5), "the kind rehearsal: 5 kills, 3 restarts, one failover at the halfway mark")

	count := func(fs []fault) map[string]int {
		n := map[string]int{}
		for _, f := range fs {
			n[f.Kind]++
		}
		return n
	}
	gke := faultSchedule(2*time.Hour, m(3), m(15), 0.5)
	require.Equal(t, map[string]int{faultWorkerKill: 40, faultBrokerRestart: 8, faultFailover: 1}, count(gke))
	require.True(t, slices.IsSortedFunc(gke, func(a, b fault) int { return cmp.Compare(a.At, b.At) }))
	require.Contains(t, gke, fault{time.Hour, faultFailover})

	require.Equal(t, map[string]int{faultWorkerKill: 5}, count(faultSchedule(m(15), m(3), 0, 0)),
		"zero turns a fault off")
	require.Empty(t, faultSchedule(m(2), m(3), m(5), 0), "nothing falls due in a run shorter than every interval")
}

// latencies is a histogram of call latencies with 5% buckets, from 10µs to
// a minute, so a p99 over millions of calls costs no memory per call.
type latencies struct {
	counts [340]int64
	n      int64
}

const latencyBase, latencyGrowth = 10 * time.Microsecond, 1.05

func (l *latencies) add(d time.Duration) {
	i := 0
	if d > latencyBase {
		i = int(math.Ceil(math.Log(float64(d)/float64(latencyBase)) / math.Log(latencyGrowth)))
	}
	l.counts[min(i, len(l.counts)-1)]++
	l.n++
}

// quantile returns the upper bound of the bucket holding the q-th call, or
// false when there were none.
func (l *latencies) quantile(q float64) (time.Duration, bool) {
	if l.n == 0 {
		return 0, false
	}
	rank := int64(math.Ceil(q * float64(l.n)))
	var seen int64
	for i, c := range l.counts {
		if seen += c; seen >= rank {
			return time.Duration(float64(latencyBase) * math.Pow(latencyGrowth, float64(i))), true
		}
	}
	return 0, false
}

// TestLatencyQuantileIsWithinABucket needs no cluster.
func TestLatencyQuantileIsWithinABucket(t *testing.T) {
	var l latencies
	_, ok := l.quantile(0.99)
	require.False(t, ok)
	for i := 1; i <= 1000; i++ {
		l.add(time.Duration(i) * time.Millisecond)
	}
	p99, ok := l.quantile(0.99)
	require.True(t, ok)
	require.InEpsilon(t, float64(990*time.Millisecond), float64(p99), 0.05)
	l.add(time.Hour)
	p100, _ := l.quantile(1)
	require.Greater(t, p100, 50*time.Second, "past the last bucket lands in it")
}

// portForward is one kubectl port-forward that never fails the test, for
// the soak's goroutines.
type portForward struct {
	addr   string
	cancel context.CancelFunc
	exited chan struct{}
}

func (c *cluster) portForward(target, remotePort string) (*portForward, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := c.cmd(ctx, "port-forward", target, ":"+remotePort)
	stdout, err := cmd.StdoutPipe()
	if err == nil {
		err = cmd.Start()
	}
	if err != nil {
		cancel()
		return nil, err
	}
	pf := &portForward{cancel: cancel, exited: make(chan struct{})}
	addr := make(chan string, 1)
	go func() {
		defer close(pf.exited)
		lines := bufio.NewScanner(stdout)
		for lines.Scan() { // kubectl prints a line per connection; read them all
			if m := forwarding.FindStringSubmatch(lines.Text()); m != nil {
				select {
				case addr <- "127.0.0.1:" + m[1]:
				default:
				}
			}
		}
		_ = cmd.Wait()
	}()
	select {
	case pf.addr = <-addr:
		return pf, nil
	case <-pf.exited:
		cancel()
		return nil, fmt.Errorf("port-forward %s exited before forwarding", target)
	case <-time.After(30 * time.Second):
		pf.close()
		return nil, fmt.Errorf("port-forward %s printed no local address in 30s", target)
	}
}

func (pf *portForward) close() { pf.cancel(); <-pf.exited }

func (pf *portForward) alive() bool {
	select {
	case <-pf.exited:
		return false
	default:
		return true
	}
}

// brokerProxy is a local address that always reaches a serving broker. A
// port-forward to a Service sticks to one pod and dies with it, which a
// rolling restart of the brokers would make permanent. The proxy forwards to
// one ready broker pod at a time, and as soon as that pod is deleted or stops
// being ready it forwards new connections to another, leaving open ones on
// the old forward until it ends.
type brokerProxy struct {
	c        *cluster
	selector string
	ln       net.Listener
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	mu       sync.Mutex
	pod      string
	cur      *portForward
	retired  []*portForward
	switches int
}

func startBrokerProxy(t *testing.T, c *cluster, selector string) *brokerProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	p := &brokerProxy{c: c, selector: selector, ln: ln, cancel: cancel}
	require.NoError(t, p.ensure(ctx))
	p.wg.Go(func() {
		for ctx.Err() == nil {
			_ = p.ensure(ctx)
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
		}
	})
	p.wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go p.serve(conn)
		}
	})
	t.Cleanup(p.stop)
	return p
}

func (p *brokerProxy) addr() string { return p.ln.Addr().String() }

// readyPods lists the pods matching selector that are ready and not being
// deleted.
func (c *cluster) readyPods(ctx context.Context, selector string) ([]string, error) {
	out, err := c.output(ctx, "get", "pods", "-l", selector, "-o",
		`jsonpath={range .items[*]}{.metadata.name}|{.metadata.deletionTimestamp}|{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}`)
	if err != nil {
		return nil, err
	}
	var ready []string
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Split(line, "|"); len(f) == 3 && f[1] == "" && f[2] == "True" {
			ready = append(ready, f[0])
		}
	}
	return ready, nil
}

// ensure moves the forward to a ready broker if the current one has ended or
// its pod is no longer ready.
func (p *brokerProxy) ensure(ctx context.Context) error {
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	ready, err := p.c.readyPods(qctx, p.selector)
	cancel()
	if err != nil {
		return err
	}
	p.mu.Lock()
	healthy := p.cur != nil && p.cur.alive() && slices.Contains(ready, p.pod)
	p.mu.Unlock()
	if healthy {
		return nil
	}
	if len(ready) == 0 {
		return errors.New("no ready broker")
	}
	pod := ready[rand.IntN(len(ready))]
	pf, err := p.c.portForward("pod/"+pod, "9090")
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.cur != nil {
		p.retired = append(p.retired, p.cur)
	}
	// A retired forward ends with its pod; the rest are closed at stop.
	p.retired = slices.DeleteFunc(p.retired, func(pf *portForward) bool { return !pf.alive() })
	p.pod, p.cur = pod, pf
	p.switches++
	p.mu.Unlock()
	return nil
}

func (p *brokerProxy) serve(conn net.Conn) {
	defer conn.Close()
	p.mu.Lock()
	cur := p.cur
	p.mu.Unlock()
	if cur == nil {
		return
	}
	up, err := net.DialTimeout("tcp", cur.addr, 5*time.Second)
	if err != nil {
		return
	}
	defer up.Close()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, up); done <- struct{}{} }()
	<-done
}

func (p *brokerProxy) stop() {
	p.cancel()
	_ = p.ln.Close()
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pf := range append(p.retired, p.cur) {
		if pf != nil {
			pf.close()
		}
	}
	p.retired, p.cur = nil, nil
}

// shardDB is one shard's database through a port-forward that is re-made
// whenever a query fails, as it does when the shard's primary is replaced.
// It never fails the test.
type shardDB struct {
	c     *cluster
	index int
	host  string // the Service the broker's DSN names
	port  string
	dsn   *url.URL // with the password
	mu    sync.Mutex
	pf    *portForward
	pool  *pgxpool.Pool
}

// shardDatabases reads the release's databases, in shard order, from the
// broker's ConfigMap: HOPPER_SHARD_DSNS, or HOPPER_DSN for one.
func shardDatabases(t *testing.T, c *cluster) []*shardDB {
	t.Helper()
	list := c.run("get", "configmap", c.release+"-broker", "-o", "jsonpath={.data.HOPPER_SHARD_DSNS}")
	if list == "" {
		list = c.run("get", "configmap", c.release+"-broker", "-o", "jsonpath={.data.HOPPER_DSN}")
	}
	encoded := c.run("get", "secret", c.release+"-postgres", "-o", "jsonpath={.data.password}")
	password, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	var dbs []*shardDB
	for i, dsn := range strings.Fields(list) {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		u.User = url.UserPassword(u.User.Username(), string(password))
		db := &shardDB{c: c, index: i, host: u.Hostname(), port: cmp.Or(u.Port(), "5432"), dsn: u}
		t.Cleanup(db.reset)
		dbs = append(dbs, db)
	}
	require.NotEmpty(t, dbs, "ConfigMap/%s-broker names no database", c.release)
	return dbs
}

func (s *shardDB) get(ctx context.Context) (*pgxpool.Pool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pool != nil && s.pf.alive() {
		return s.pool, nil
	}
	s.resetLocked()
	pf, err := s.c.portForward("svc/"+s.host, s.port)
	if err != nil {
		return nil, err
	}
	u := *s.dsn
	u.Host = pf.addr
	pool, err := pgxpool.New(ctx, u.String())
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		if pool != nil {
			pool.Close()
		}
		pf.close()
		return nil, fmt.Errorf("shard %d (%s): %w", s.index, s.host, err)
	}
	s.pf, s.pool = pf, pool
	return pool, nil
}

// do runs f on the shard's pool, and re-makes the forward next time if f fails.
func (s *shardDB) do(ctx context.Context, f func(*pgxpool.Pool) error) error {
	pool, err := s.get(ctx)
	if err == nil {
		if err = f(pool); err != nil {
			s.reset()
		}
	}
	return err
}

func (s *shardDB) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetLocked()
}

func (s *shardDB) resetLocked() {
	if s.pool != nil {
		s.pool.Close()
	}
	if s.pf != nil {
		s.pf.close()
	}
	s.pool, s.pf = nil, nil
}

// mustPool is get on the test's goroutine, retried for a minute.
func (s *shardDB) mustPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	var err error
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); time.Sleep(2 * time.Second) {
		var pool *pgxpool.Pool
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		pool, err = s.get(ctx)
		cancel()
		if err == nil {
			return pool
		}
	}
	require.NoError(t, err)
	return nil
}

// openLoop offers keyed sleep jobs at a fixed rate, whatever the brokers
// answer: a ticker starts a call every 1/rate, at most inflight at once, and
// a call that can't start then is counted as behind and never queued. A
// failed call is in doubt and is retried under its key until answered, as
// producer does, holding its slot. Each chunk of keys has its own run name,
// and every acknowledgement is appended to that chunk's file as it arrives,
// so memory stays flat and the evidence outlives a crashed harness.
type openLoop struct {
	clients  []*client.Client
	payload  json.RawMessage
	prefix   string // the run names' common prefix, soak-<stamp>
	chunk    int
	dir      string
	rate     float64
	inflight int
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	mu                        sync.Mutex
	files                     map[int]*ackFile
	window, all               latencies
	started, behind, acked    int
	deduped                   int
	windowStarted, windowAcks int
	failed                    map[string]int
	fileErr                   error
}

type ackFile struct {
	f *os.File
	w *bufio.Writer
}

// runName is chunk n's run name: its keys are <run>-<i>.
func (p *openLoop) runName(n int) string { return fmt.Sprintf("%s-%d", p.prefix, n) }

func startOpenLoop(clients []*client.Client, prefix, dir string, cfg soakConfig) *openLoop {
	ctx, cancel := context.WithCancel(context.Background())
	p := &openLoop{clients: clients, payload: json.RawMessage(fmt.Sprintf(`{"ms":%d}`, cfg.jobMS)),
		prefix: prefix, chunk: cfg.chunk, dir: dir, rate: cfg.rate, inflight: cfg.inflight, cancel: cancel,
		files: map[int]*ackFile{}, failed: map[string]int{}}
	slots := make(chan struct{}, p.inflight)
	p.wg.Go(func() {
		begin := time.Now()
		tick := time.NewTicker(max(time.Millisecond, time.Duration(float64(time.Second)/p.rate)))
		defer tick.Stop()
		due, next := 0, 0
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				for target := int(now.Sub(begin).Seconds() * p.rate); due < target; due++ {
					select {
					case slots <- struct{}{}:
						i := next
						next++
						p.mu.Lock()
						p.started++
						p.windowStarted++
						p.mu.Unlock()
						p.wg.Go(func() {
							defer func() { <-slots }()
							p.produce(ctx, i)
						})
					default:
						p.mu.Lock()
						p.behind++
						p.mu.Unlock()
					}
				}
			}
		}
	})
	p.wg.Go(func() { // flush acknowledgements every second
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				p.flush()
			}
		}
	})
	return p
}

func (p *openLoop) produce(ctx context.Context, i int) {
	n := i / p.chunk
	key := fmt.Sprintf("%s-%d", p.runName(n), i)
	cl := p.clients[i%len(p.clients)]
	for {
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, producerCall)
		res, err := cl.Enqueue(cctx, client.Job{Queue: "sleep", Payload: p.payload, MaxAttempts: 20, IdempotencyKey: key})
		cancel()
		if ctx.Err() != nil {
			return // stopping: this answer is not counted, and the key was never acknowledged
		}
		end := time.Now()
		p.mu.Lock()
		if err == nil {
			p.record(n, res, end)
			p.window.add(end.Sub(start))
			p.all.add(end.Sub(start))
			p.mu.Unlock()
			return
		}
		p.failed[status.Code(err).String()]++
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(producerBackoff):
		}
	}
}

// record appends one acknowledgement to its chunk's file: the job ID, when
// the answer arrived in Unix nanoseconds, and 1 when it was deduplicated.
// Called with mu held.
func (p *openLoop) record(n int, res client.Result, at time.Time) {
	p.acked++
	p.windowAcks++
	dedup := 0
	if res.Deduplicated {
		p.deduped++
		dedup = 1
	}
	f := p.files[n]
	if f == nil {
		file, err := os.Create(filepath.Join(p.dir, fmt.Sprintf("acked-%d.txt", n)))
		if err != nil {
			p.fileErr = cmp.Or(p.fileErr, err)
			return
		}
		f = &ackFile{f: file, w: bufio.NewWriter(file)}
		p.files[n] = f
	}
	if _, err := fmt.Fprintf(f.w, "%s,%d,%d\n", res.ID, at.UnixNano(), dedup); err != nil {
		p.fileErr = cmp.Or(p.fileErr, err)
	}
}

func (p *openLoop) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.files {
		if err := f.w.Flush(); err != nil {
			p.fileErr = cmp.Or(p.fileErr, err)
		}
	}
}

// stop ends production, waits for every call to return, and closes the files.
func (p *openLoop) stop() error {
	p.cancel()
	p.wg.Wait()
	p.flush()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, f := range p.files {
		p.fileErr = cmp.Or(p.fileErr, f.f.Close())
	}
	p.files = map[int]*ackFile{}
	return p.fileErr
}

// produced is one sample of the producer's window, which it then resets.
type producedWindow struct {
	started, acked, behind int
	p99                    time.Duration
	hasP99                 bool
}

func (p *openLoop) sample() producedWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	w := producedWindow{started: p.windowStarted, acked: p.windowAcks, behind: p.behind}
	w.p99, w.hasP99 = p.window.quantile(0.99)
	p.window, p.windowStarted, p.windowAcks = latencies{}, 0, 0
	return w
}

// readAcked reads one chunk's acknowledgements.
func readAcked(path string) ([]accounting.Acknowledged, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []accounting.Acknowledged
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ",")
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s: line %q", path, sc.Text())
		}
		id, err := uuid.Parse(fields[0])
		if err != nil {
			return nil, err
		}
		ns, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		out = append(out, accounting.Acknowledged{ID: id, At: time.Unix(0, ns)})
	}
	return out, sc.Err()
}

// soakEvent is one fault as events.csv records it.
type soakEvent struct {
	At      float64 // seconds from the start of production
	Kind    string
	Target  string
	Outcome string // ok, error or skipped
	Detail  string
}

// csvLog appends rows to a CSV file, flushing each, and never fails the test.
type csvLog struct {
	mu  sync.Mutex
	f   *os.File
	w   *csv.Writer
	err error
}

func newCSVLog(t *testing.T, path string, header []string) *csvLog {
	t.Helper()
	f, err := os.Create(path)
	require.NoError(t, err)
	l := &csvLog{f: f, w: csv.NewWriter(f)}
	l.write(header)
	return l
}

func (l *csvLog) write(row []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.err = cmp.Or(l.err, l.w.Write(row))
	l.w.Flush()
	l.err = cmp.Or(l.err, l.w.Error())
}

func (l *csvLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cmp.Or(l.err, l.f.Close())
}

// soak is one run's moving parts.
type soak struct {
	t        *testing.T
	c        *cluster
	cfg      soakConfig
	dbs      []*shardDB
	cnpg     bool
	workers  string // selector
	begin    time.Time
	events   *csvLog
	mu       sync.Mutex
	happened []soakEvent
}

func (s *soak) record(e soakEvent) {
	s.events.write([]string{strconv.FormatFloat(e.At, 'f', 1, 64), e.Kind, e.Target, e.Outcome, e.Detail})
	s.mu.Lock()
	s.happened = append(s.happened, e)
	s.mu.Unlock()
	s.t.Logf("fault at %.0fs: %s %s: %s %s", e.At, e.Kind, e.Target, e.Outcome, e.Detail)
}

// inject runs the schedule until it is done or ctx ends. It never fails the
// test: what each fault did is in events.csv.
func (s *soak) inject(ctx context.Context, schedule []fault) {
	for _, f := range schedule {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(s.begin.Add(f.At))):
		}
		e := soakEvent{At: time.Since(s.begin).Seconds(), Kind: f.Kind}
		var err error
		switch f.Kind {
		case faultWorkerKill:
			e.Target, e.Detail, err = s.killWorker(ctx)
		case faultBrokerRestart:
			e.Target, e.Detail, err = s.restartBrokers(ctx)
		case faultFailover:
			if !s.cnpg {
				e.Outcome, e.Target = "skipped", fmt.Sprintf("shard %d", s.cfg.failoverShard)
				e.Detail = "bundled mode: a shard has no standby to promote; run with postgres.mode=cloudnativepg"
				s.record(e)
				continue
			}
			e.Target, e.Detail, err = s.failover(ctx, s.cfg.failoverShard)
		}
		e.Outcome = "ok"
		if err != nil {
			e.Outcome, e.Detail = "error", strings.TrimSpace(e.Detail+" "+err.Error())
		}
		s.record(e)
	}
}

// container is one running container of a pod.
type container struct {
	pod, node, id string
	restarts      int
}

// runningContainer reads pod's container named name, or its first container
// when name is empty.
func (s *soak) runningContainer(ctx context.Context, pod, name string) (container, error) {
	sel := "0"
	if name != "" {
		sel = fmt.Sprintf(`?(@.name==%q)`, name)
	}
	out, err := s.c.output(ctx, "get", "pod", pod, "-o", fmt.Sprintf(
		`jsonpath={.spec.nodeName}|{.status.containerStatuses[%s].containerID}|{.status.containerStatuses[%s].restartCount}|{.status.containerStatuses[%s].state.running.startedAt}`,
		sel, sel, sel))
	if err != nil {
		return container{}, err
	}
	f := strings.Split(out, "|")
	if len(f) != 4 || f[3] == "" || f[1] == "" {
		return container{}, fmt.Errorf("pod %s has no running container %q: %q", pod, name, out)
	}
	n, err := strconv.Atoi(f[2])
	if err != nil {
		return container{}, err
	}
	_, id, _ := strings.Cut(f[1], "://")
	return container{pod: pod, node: f[0], id: id, restarts: n}, nil
}

// killContainer SIGKILLs a container through its node's runtime: crictl stop
// with no grace. crictl-docker reaches a kind node through Docker;
// crictl-debug reaches any node through a privileged kubectl debug pod.
func (s *soak) killContainer(ctx context.Context, ct container) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	switch s.cfg.kill {
	case "crictl-docker":
		out, err := exec.CommandContext(ctx, "docker", "exec", ct.node, "crictl", "stop", "--timeout", "0", ct.id).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("docker exec %s crictl stop: %w: %s", ct.node, err, strings.TrimSpace(string(out)))
		}
		return "crictl-docker", nil
	default: // crictl-debug
		// GKE's Container-Optimized OS keeps crictl in /home/kubernetes/bin.
		script := "PATH=$PATH:/home/kubernetes/bin crictl stop --timeout 0 " + ct.id
		cmd := exec.CommandContext(ctx, "kubectl", "--context", s.c.context, "-n", s.cfg.debugNamespace,
			"debug", "node/"+ct.node, "--profile=sysadmin", "--image="+s.cfg.debugImage, "--attach=true",
			"--", "chroot", "/host", "sh", "-c", script)
		out, err := cmd.CombinedOutput()
		if m := debugPod.FindSubmatch(out); m != nil {
			dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = exec.CommandContext(dctx, "kubectl", "--context", s.c.context, "-n", s.cfg.debugNamespace,
				"delete", "pod", string(m[1]), "--wait=false").Run()
			dcancel()
		}
		if err != nil {
			return "", fmt.Errorf("kubectl debug node/%s: %w: %s", ct.node, err, strings.TrimSpace(string(out)))
		}
		return "crictl-debug", nil
	}
}

var debugPod = regexp.MustCompile(`Creating debugging pod (\S+)`)

// killWorker SIGKILLs a random running worker and waits for the kubelet to
// restart it, which is how a kill shows it took.
func (s *soak) killWorker(ctx context.Context) (target, detail string, err error) {
	pods, err := s.c.readyPods(ctx, s.workers)
	if err != nil {
		return "", "", err
	}
	if len(pods) == 0 {
		return "", "", errors.New("no ready worker")
	}
	pod := pods[rand.IntN(len(pods))]
	ct, err := s.runningContainer(ctx, pod, "")
	if err != nil {
		return pod, "", err
	}
	how, err := s.killContainer(ctx, ct)
	detail = fmt.Sprintf("container %.12s on %s via %s", ct.id, ct.node, how)
	if err != nil {
		return pod, detail, err
	}
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline) && ctx.Err() == nil; time.Sleep(time.Second) {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		now, err := s.runningContainer(qctx, pod, "")
		cancel()
		if err == nil && now.restarts > ct.restarts {
			return pod, fmt.Sprintf("%s; restarted in place (restarts %d)", detail, now.restarts), nil
		}
	}
	return pod, detail, errors.New("the container was not restarted within 2m: the kill did not take")
}

func (s *soak) restartBrokers(ctx context.Context) (target, detail string, err error) {
	deploy := "deployment/" + s.c.release + "-broker"
	if _, err := s.c.output(ctx, "rollout", "restart", deploy); err != nil {
		return deploy, "", err
	}
	begun := time.Now()
	rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if _, err := s.c.output(rctx, "rollout", "status", deploy, "--timeout=9m"); err != nil {
		return deploy, "", err
	}
	return deploy, fmt.Sprintf("rolled out in %.0fs", time.Since(begun).Seconds()), nil
}

// shardCluster names shard i's CloudNativePG Cluster: <release>-db for one
// shard, <release>-db-<i> for more, as its read-write Service says.
func (s *soak) shardCluster(i int) (*replicatedDB, error) {
	if i < 0 || i >= len(s.dbs) {
		return nil, fmt.Errorf("shard %d of %d", i, len(s.dbs))
	}
	return &replicatedDB{c: s.c, name: strings.TrimSuffix(s.dbs[i].host, "-rw")}, nil
}

// failover crashes shard i's primary as Phase 2's proofs do (a SIGKILL
// through the node's runtime, then a forced delete so the operator promotes a
// standby rather than restart the instance) and waits for a new primary.
func (s *soak) failover(ctx context.Context, i int) (target, detail string, err error) {
	db, err := s.shardCluster(i)
	if err != nil {
		return "", "", err
	}
	st, err := db.status(ctx)
	if err != nil {
		return db.name, "", err
	}
	if !st.healthy() {
		return db.name, "", fmt.Errorf("Cluster/%s is not healthy before the crash: %+v", db.name, st)
	}
	ct, err := s.runningContainer(ctx, st.Primary, "postgres")
	if err != nil {
		return st.Primary, "", err
	}
	if _, err := s.killContainer(ctx, ct); err != nil {
		return st.Primary, "", err
	}
	crashed := time.Now()
	if _, err := s.c.output(ctx, "delete", "pod", st.Primary, "--grace-period=0", "--force", "--wait=false"); err != nil {
		return st.Primary, "", err
	}
	target = fmt.Sprintf("shard %d %s", i, st.Primary)
	for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline) && ctx.Err() == nil; time.Sleep(time.Second) {
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		now, err := db.status(qctx)
		cancel()
		if err == nil && now.Primary != "" && now.Primary != st.Primary && now.Primary == now.Target {
			promoted := time.Since(crashed)
			for healthy := time.Now().Add(10 * time.Minute); time.Now().Before(healthy) && ctx.Err() == nil; time.Sleep(2 * time.Second) {
				qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				h, err := db.status(qctx)
				cancel()
				if err == nil && h.healthy() {
					return target, fmt.Sprintf("%s promoted after %.1fs; healthy after %.0fs", now.Primary,
						promoted.Seconds(), time.Since(crashed).Seconds()), nil
				}
			}
			return target, fmt.Sprintf("%s promoted after %.1fs", now.Primary, promoted.Seconds()),
				errors.New("the Cluster was not healthy again within 10m")
		}
	}
	return target, "", errors.New("no standby was promoted within 3m")
}

// shardSample is one reading of one shard.
type shardSample struct {
	ok                  bool
	completed           int64
	tableBytes, deadTup int64
}

func (s *soak) sampleShard(ctx context.Context, db *shardDB) shardSample {
	var out shardSample
	err := db.do(ctx, func(pool *pgxpool.Pool) error {
		return pool.QueryRow(ctx, `select (select count(*) from jobs where state = 'completed'),
		       pg_total_relation_size('jobs'),
		       coalesce((select n_dead_tup from pg_stat_user_tables where relname = 'jobs'), 0)`).
			Scan(&out.completed, &out.tableBytes, &out.deadTup)
	})
	out.ok = err == nil
	return out
}

// pickupP99 asks the Prometheus that scrapes the brokers for the p99 of
// job_wait_seconds (due time to first lease) over the last minute, in ms.
func (s *soak) pickupP99(ctx context.Context, path string) (float64, bool) {
	query := fmt.Sprintf(`histogram_quantile(0.99, sum by (le) (rate(hopper_job_wait_seconds_bucket{namespace=%q, job=%q}[1m])))`,
		s.c.namespace, s.c.release+"-broker")
	raw, err := s.c.output(ctx, "get", "--raw", path+"?query="+url.QueryEscape(query))
	if err != nil {
		return 0, false
	}
	var resp struct {
		Status string
		Data   struct{ Result []struct{ Value [2]any } }
	}
	if json.Unmarshal([]byte(raw), &resp) != nil || resp.Status != "success" || len(resp.Data.Result) != 1 {
		return 0, false
	}
	v, _ := resp.Data.Result[0].Value[1].(string)
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) {
		return 0, false
	}
	return f * 1000, true
}

// prometheusPath is the API server's service proxy to HOPPER_SOAK_PROMETHEUS,
// or empty when it does not answer.
func (s *soak) prometheusPath(ctx context.Context) string {
	ns, svc, ok := strings.Cut(s.cfg.prometheus, "/")
	if !ok {
		return ""
	}
	name, port, _ := strings.Cut(svc, ":")
	path := fmt.Sprintf("/api/v1/namespaces/%s/services/%s:%s/proxy/api/v1/query", ns, name, cmp.Or(port, "9090"))
	if _, err := s.c.output(ctx, "get", "--raw", path+"?query=up"); err != nil {
		return ""
	}
	return path
}

// timelinePoint is one row of timeline.csv, kept for the report.
type timelinePoint struct {
	t             float64
	completedPerS float64
	hasCompleted  bool
	inProduction  bool
}

// sampleTimeline writes a row to timeline every soakSample until ctx ends.
func (s *soak) sampleTimeline(ctx context.Context, p *openLoop, timeline *csvLog, producing func() bool) []timelinePoint {
	promPath := s.prometheusPath(ctx)
	if promPath == "" {
		s.t.Logf("no Prometheus at %s: pickup p99 stays blank", s.cfg.prometheus)
	}
	type last struct {
		at        time.Time
		completed int64
		ok        bool
	}
	prev := make([]last, len(s.dbs))
	for i, db := range s.dbs {
		if sm := s.sampleShard(ctx, db); sm.ok {
			prev[i] = last{time.Now(), sm.completed, true}
		}
	}
	var points []timelinePoint
	prevAt := time.Now()
	for {
		select {
		case <-ctx.Done():
			return points
		case <-time.After(soakSample):
		}
		now := time.Now()
		w := p.sample()
		dt := now.Sub(prevAt).Seconds()
		prevAt = now
		row := []string{strconv.FormatFloat(now.Sub(s.begin).Seconds(), 'f', 0, 64),
			strconv.FormatFloat(float64(w.started)/dt, 'f', 1, 64),
			strconv.FormatFloat(float64(w.acked)/dt, 'f', 1, 64),
			strconv.Itoa(w.behind)}
		var rate float64
		all := true
		var shardCols []string
		for i, db := range s.dbs {
			sctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			sm := s.sampleShard(sctx, db)
			cancel()
			if !sm.ok {
				all = false
				shardCols = append(shardCols, "", "", "")
				continue
			}
			if prev[i].ok {
				rate += float64(sm.completed-prev[i].completed) / now.Sub(prev[i].at).Seconds()
			} else {
				all = false
			}
			prev[i] = last{now, sm.completed, true}
			shardCols = append(shardCols, strconv.FormatInt(sm.completed, 10),
				strconv.FormatInt(sm.tableBytes, 10), strconv.FormatInt(sm.deadTup, 10))
		}
		pt := timelinePoint{t: now.Sub(s.begin).Seconds(), completedPerS: rate, hasCompleted: all, inProduction: producing()}
		points = append(points, pt)
		row = append(row, blankUnless(all, strconv.FormatFloat(rate, 'f', 1, 64)))
		row = append(row, blankUnless(w.hasP99, strconv.FormatFloat(float64(w.p99)/float64(time.Millisecond), 'f', 2, 64)))
		pickup, ok := 0.0, false
		if promPath != "" {
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			pickup, ok = s.pickupP99(pctx, promPath)
			cancel()
		}
		row = append(row, blankUnless(ok, strconv.FormatFloat(pickup, 'f', 1, 64)))
		timeline.write(append(row, shardCols...))
	}
}

func blankUnless(ok bool, v string) string {
	if !ok {
		return ""
	}
	return v
}

// maxRSS is the process's peak resident set so far, in bytes.
func maxRSS() int64 {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return int64(ru.Maxrss) // bytes
	}
	return int64(ru.Maxrss) * 1024 // kilobytes
}

// chunkReport is one chunk's reconciliation.
type chunkReport struct {
	Chunk         int            `json:"chunk"`
	Run           string         `json:"run"`
	Jobs          int            `json:"jobs"`
	JobsPerShard  []int          `json:"jobs_per_shard"`
	Misplaced     int            `json:"rows_on_a_shard_their_id_does_not_name"`
	Acknowledged  int            `json:"acknowledged"`
	Lost          int            `json:"lost"`
	Completed     int            `json:"completed"`
	Attempts      int            `json:"attempts"`
	Unexplained   int            `json:"unexplained"`
	Repeats       map[string]int `json:"repeated_starts_by_cause"`
	MaxRSSAfter   int64          `json:"harness_max_rss_bytes_after"`
	ReconcileSecs float64        `json:"reconcile_s"`
}

// soakReport is report.json.
type soakReport struct {
	Ref               string         `json:"ref"`
	Dirty             bool           `json:"dirty"`
	BrokerImage       string         `json:"broker_image"`
	Context           string         `json:"context"`
	Mode              string         `json:"postgres_mode"`
	Shards            int            `json:"shards"`
	Workers           int            `json:"workers"`
	Brokers           int            `json:"brokers"`
	Rate              float64        `json:"offered_rate_per_s"`
	Inflight          int            `json:"inflight"`
	Chunk             int            `json:"chunk"`
	JobMS             int            `json:"job_ms"`
	Kill              string         `json:"kill_mode"`
	Duration          string         `json:"duration"`
	ProducedFor       float64        `json:"produced_for_s"`
	DrainedIn         float64        `json:"drained_in_s"`
	Started           int            `json:"calls_started"`
	Behind            int            `json:"behind"`
	Acknowledged      int            `json:"acknowledged_answers"`
	Deduplicated      int            `json:"deduplicated_answers"`
	FailedEnqueues    map[string]int `json:"failed_enqueues_by_code"`
	EnqueueP99Ms      float64        `json:"enqueue_p99_ms"`
	CompletedRateMean float64        `json:"completed_per_s_mean"`
	CompletedRateP99  float64        `json:"completed_per_s_p99"`
	Jobs              int            `json:"jobs"`
	Completed         int            `json:"completed"`
	Attempts          int            `json:"attempts"`
	RedeliveryRate    float64        `json:"redelivery_rate"`
	Lost              int            `json:"lost"`
	Unexplained       int            `json:"unexplained"`
	Misplaced         int            `json:"misplaced_rows"`
	Faults            map[string]int `json:"faults_by_kind"`
	FaultErrors       int            `json:"fault_errors"`
	Skipped           []string       `json:"skipped_faults,omitempty"`
	WorkerRestarts    int            `json:"worker_container_restarts"`
	ProxySwitches     int            `json:"broker_forward_switches"`
	MaxRSSBeforeParse int64          `json:"harness_max_rss_bytes_before_parse"`
	MaxRSSAfterParse  int64          `json:"harness_max_rss_bytes_after_parse"`
	LogBytes          int64          `json:"worker_log_bytes"`
	Chunks            []chunkReport  `json:"chunks"`
}

// TestSoak runs keyed jobs at a fixed offered rate for HOPPER_SOAK_DURATION
// while killing workers, restarting brokers and, on CloudNativePG, failing
// one shard over, then drains and holds every chunk of the run to zero lost
// and zero unexplained (doc.go has the knobs). Evidence goes to
// HOPPER_PROOF_DIR/<run>: events.csv, timeline.csv, acked-<n>.txt, the
// workers' logs, report.json, summary.md and three SVGs.
func TestSoak(t *testing.T) {
	cfg := soakSettings(t)
	c := newCluster(t)
	stamp := time.Now().UTC().Format("20060102T150405")
	prefix := "soak-" + stamp
	dir := filepath.Join(setting("HOPPER_PROOF_DIR", t.TempDir()), prefix)
	workerLogs := filepath.Join(dir, "logs", "workers")
	require.NoError(t, os.MkdirAll(workerLogs, 0o755))
	bg := context.Background()

	workers := "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=worker"
	brokers := "app.kubernetes.io/instance=" + c.release + ",app.kubernetes.io/component=broker"
	for _, d := range []string{"worker", "broker"} {
		c.run("rollout", "status", "deployment/"+c.release+"-"+d, "--timeout=5m")
	}
	dbs := shardDatabases(t, c)
	s := &soak{t: t, c: c, cfg: cfg, dbs: dbs, workers: workers, cnpg: strings.HasSuffix(dbs[0].host, "-rw")}
	rep := soakReport{Context: c.context, Shards: len(dbs), Mode: "bundled", Rate: cfg.rate, Inflight: cfg.inflight,
		Chunk: cfg.chunk, JobMS: cfg.jobMS, Kill: cfg.kill, Duration: cfg.duration.String(),
		BrokerImage: c.run("get", "deployment", c.release+"-broker", "-o", "jsonpath={.spec.template.spec.containers[0].image}")}
	if s.cnpg {
		rep.Mode = "cloudnativepg"
	}
	if out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output(); err == nil {
		rep.Ref = strings.TrimSpace(string(out))
	}
	rep.Dirty = exec.Command("git", "-C", "../..", "diff", "--quiet", "HEAD", "--", ".", ":!docs").Run() != nil
	wpods := c.livePods(workers)
	rep.Workers, rep.Brokers = len(wpods), len(c.livePods(brokers))
	if cfg.workerKillEvery > 0 {
		require.GreaterOrEqual(t, len(wpods), 2, "killing workers needs at least two")
	}
	if cfg.failoverAt > 0 && s.cnpg {
		_, err := s.shardCluster(cfg.failoverShard)
		require.NoError(t, err, "HOPPER_SOAK_FAILOVER_SHARD")
	}
	restartsBefore := restarts(c, workers)

	cp := startRestartCapture(c, workers, workerLogs)
	t.Cleanup(func() { _ = cp.stop() })
	require.Eventually(t, func() bool { return cp.attached(wpods) }, time.Minute, 200*time.Millisecond,
		"the workers' logs never attached")

	proxy := startBrokerProxy(t, c, brokers)
	var clients []*client.Client
	for range cfg.conns {
		cl, err := client.Dial(proxy.addr())
		require.NoError(t, err)
		t.Cleanup(func() { _ = cl.Close() })
		clients = append(clients, cl)
	}
	for _, db := range dbs {
		db.mustPool(t)
	}

	s.events = newCSVLog(t, filepath.Join(dir, "events.csv"), []string{"at_s", "kind", "target", "outcome", "detail"})
	header := []string{"t_s", "offered_per_s", "acked_per_s", "behind", "completed_per_s", "enqueue_p99_ms", "pickup_p99_ms"}
	for i := range dbs {
		header = append(header, fmt.Sprintf("shard%d_completed", i), fmt.Sprintf("shard%d_table_bytes", i), fmt.Sprintf("shard%d_dead_tup", i))
	}
	timeline := newCSVLog(t, filepath.Join(dir, "timeline.csv"), header)
	schedule := faultSchedule(cfg.duration, cfg.workerKillEvery, cfg.brokerRestartEvery, cfg.failoverAt)
	t.Logf("soak %s: %s at %.0f jobs/s offered, %d shards (%s), %d workers, %d brokers, %d faults scheduled; evidence in %s",
		prefix, cfg.duration, cfg.rate, len(dbs), rep.Mode, rep.Workers, rep.Brokers, len(schedule), dir)

	s.begin = time.Now()
	p := startOpenLoop(clients, prefix, dir, cfg)
	t.Cleanup(func() { _ = p.stop() })
	var producing sync.Mutex
	stillProducing := true
	sampleCtx, stopSampling := context.WithCancel(bg)
	sampled := make(chan []timelinePoint, 1)
	go func() {
		sampled <- s.sampleTimeline(sampleCtx, p, timeline, func() bool {
			producing.Lock()
			defer producing.Unlock()
			return stillProducing
		})
	}()
	faultCtx, stopFaults := context.WithCancel(bg)
	injected := make(chan struct{})
	go func() { defer close(injected); s.inject(faultCtx, schedule) }()
	t.Cleanup(func() { stopFaults(); <-injected; stopSampling() })

	time.Sleep(time.Until(s.begin.Add(cfg.duration)))
	<-injected // the faults due at the end run under load too
	require.NoError(t, p.stop(), "the acknowledgement files")
	producing.Lock()
	stillProducing = false
	producing.Unlock()
	stopped := time.Now()
	rep.ProducedFor = stopped.Sub(s.begin).Seconds()
	p.mu.Lock()
	rep.Started, rep.Behind, rep.Acknowledged, rep.Deduplicated = p.started, p.behind, p.acked, p.deduped
	rep.FailedEnqueues = copyCounts(p.failed)
	if q, ok := p.all.quantile(0.99); ok {
		rep.EnqueueP99Ms = float64(q) / float64(time.Millisecond)
	}
	chunks := p.started/cfg.chunk + 1
	p.mu.Unlock()
	t.Logf("production stopped after %.0fs: %d calls started, %d behind, %d answers (%d deduplicated), failures %v",
		rep.ProducedFor, rep.Started, rep.Behind, rep.Acknowledged, rep.Deduplicated, rep.FailedEnqueues)

	// Drain: every job of the run, acknowledged or abandoned in doubt, ends.
	open := func() (int64, error) {
		var total int64
		for _, db := range dbs {
			var n int64
			ctx, cancel := context.WithTimeout(bg, time.Minute)
			err := db.do(ctx, func(pool *pgxpool.Pool) error {
				return pool.QueryRow(ctx, `select count(*) from jobs where idempotency_key like $1 || '-%'
				                           and state not in ('completed', 'dead', 'cancelled')`, prefix).Scan(&n)
			})
			cancel()
			if err != nil {
				return 0, err
			}
			total += n
		}
		return total, nil
	}
	var left int64 = -1
	for deadline := time.Now().Add(cfg.drainTimeout); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
		if n, err := open(); err == nil {
			if left = n; n == 0 {
				break
			}
		}
	}
	rep.DrainedIn = time.Since(stopped).Seconds()
	stopSampling()
	points := <-sampled
	require.NoError(t, timeline.close(), "timeline.csv")
	require.NoError(t, s.events.close(), "events.csv")
	require.Zero(t, left, "run %s did not drain within %s", prefix, cfg.drainTimeout)

	require.NoError(t, cp.stop(), "worker log capture")
	cp.snapshot(c.livePods(workers))
	rep.WorkerRestarts = restarts(c, workers) - restartsBefore
	proxy.mu.Lock()
	rep.ProxySwitches = proxy.switches
	proxy.mu.Unlock()

	// The timeline's completed rate while producing.
	var rates []float64
	for _, pt := range points {
		if pt.inProduction && pt.hasCompleted {
			rates = append(rates, pt.completedPerS)
		}
	}
	if len(rates) > 0 {
		var sum float64
		for _, r := range rates {
			sum += r
		}
		rep.CompletedRateMean = sum / float64(len(rates))
		slices.Sort(rates)
		rep.CompletedRateP99 = rates[min(len(rates)-1, int(math.Ceil(0.99*float64(len(rates))))-1)]
	}

	// Reconcile chunk by chunk, against every worker log.
	rep.MaxRSSBeforeParse = maxRSS()
	paths, err := filepath.Glob(filepath.Join(workerLogs, "*.log"))
	require.NoError(t, err)
	for _, path := range paths {
		if fi, err := os.Stat(path); err == nil {
			rep.LogBytes += fi.Size()
		}
	}
	logs := readLogs(t, workerLogs)
	rep.MaxRSSAfterParse = maxRSS()
	repeats := map[string]int{}
	for n := range chunks {
		begun := time.Now()
		run := p.runName(n)
		cr := chunkReport{Chunk: n, Run: run, Repeats: map[string]int{}}
		var rows []accounting.Row
		for i, db := range dbs {
			shardRows := loadRows(t, db.mustPool(t), run)
			for _, r := range shardRows {
				if at, err := store.ShardOf(r.ID, len(dbs)); err != nil || at != i {
					cr.Misplaced++
				}
			}
			cr.JobsPerShard = append(cr.JobsPerShard, len(shardRows))
			rows = append(rows, shardRows...)
		}
		ackPath := filepath.Join(dir, fmt.Sprintf("acked-%d.txt", n))
		acked, err := readAcked(ackPath)
		if errors.Is(err, os.ErrNotExist) {
			// No call of this chunk was answered: the last chunk, begun just
			// before production stopped. Its rows, if any, were in doubt.
			if len(rows) == 0 {
				continue
			}
			err = nil
		}
		require.NoError(t, err)
		dur := accounting.CheckDurability(acked, rows)
		rc := accounting.Reconcile(rows, logs)
		problems := rc.AttemptProblems()
		cr.Jobs, cr.Acknowledged, cr.Lost, cr.Completed = rc.Jobs, dur.Acknowledged, len(dur.Lost), rc.Completed
		cr.Unexplained = len(problems)
		for _, r := range rows {
			cr.Attempts += r.Attempts
		}
		for _, r := range rc.Repeats {
			cr.Repeats[cmp.Or(r.Cause, "unexplained")]++
			repeats[cmp.Or(r.Cause, "unexplained")]++
		}
		cr.MaxRSSAfter = maxRSS()
		cr.ReconcileSecs = time.Since(begun).Seconds()
		if len(dur.Lost) > 0 {
			writeJSON(t, filepath.Join(dir, fmt.Sprintf("lost-%d.json", n)), dur.Lost)
		}
		if len(problems) > 0 {
			writeJSON(t, filepath.Join(dir, fmt.Sprintf("problems-%d.json", n)), problems[:min(len(problems), 1000)])
		}
		rep.Chunks = append(rep.Chunks, cr)
		rep.Jobs += cr.Jobs
		rep.Completed += cr.Completed
		rep.Attempts += cr.Attempts
		rep.Lost += cr.Lost
		rep.Unexplained += cr.Unexplained
		rep.Misplaced += cr.Misplaced
		t.Logf("chunk %d (%s): %d jobs %v by shard, %d acknowledged, %d lost, %d completed, %d attempts, %d unexplained, repeats %v; "+
			"harness max RSS %.0f MiB; %.1fs", n, run, cr.Jobs, cr.JobsPerShard, cr.Acknowledged, cr.Lost, cr.Completed,
			cr.Attempts, cr.Unexplained, cr.Repeats, float64(cr.MaxRSSAfter)/(1<<20), cr.ReconcileSecs)
	}
	if rep.Jobs > 0 {
		rep.RedeliveryRate = float64(rep.Attempts-rep.Jobs) / float64(rep.Jobs)
	}
	rep.Faults = map[string]int{}
	s.mu.Lock()
	for _, e := range s.happened {
		switch e.Outcome {
		case "ok":
			rep.Faults[e.Kind]++
		case "skipped":
			rep.Skipped = append(rep.Skipped, e.Kind+": "+e.Detail)
		default:
			rep.FaultErrors++
		}
	}
	s.mu.Unlock()

	// Evidence first, so a failed requirement below still leaves it.
	writeJSON(t, filepath.Join(dir, "report.json"), rep)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "summary.md"), []byte(soakSummary(rep, repeats)), 0o644))
	if out, err := exec.Command("python3", "../../scripts/soak_chart.py", dir).CombinedOutput(); err != nil {
		t.Errorf("scripts/soak_chart.py: %v: %s", err, out)
	}
	t.Logf("soak %s: %d jobs in %.0fs (+%.0fs drain), completed/s mean %.1f p99 %.1f, enqueue p99 %.1fms, behind %d; "+
		"faults %v (%d errors, skipped %v), %d worker restarts; lost %d, unexplained %d, misplaced %d, redelivery rate %.4f; "+
		"harness max RSS %.0f MiB after parsing %.0f MiB of worker logs",
		prefix, rep.Jobs, rep.ProducedFor, rep.DrainedIn, rep.CompletedRateMean, rep.CompletedRateP99, rep.EnqueueP99Ms, rep.Behind,
		rep.Faults, rep.FaultErrors, rep.Skipped, rep.WorkerRestarts, rep.Lost, rep.Unexplained, rep.Misplaced, rep.RedeliveryRate,
		float64(maxRSS())/(1<<20), float64(rep.LogBytes)/(1<<20))

	require.Positive(t, rep.Jobs, "the soak enqueued nothing")
	require.Zero(t, rep.Lost, "acknowledged jobs missing (lost-<n>.json)")
	require.Zero(t, rep.Unexplained, "every repeated start must be explained and every job completed (problems-<n>.json)")
	require.Equal(t, rep.Jobs, rep.Completed)
	require.Zero(t, rep.Misplaced, "a row lives on a shard its ID does not name")
	require.Zero(t, rep.FaultErrors, "a fault failed to inject (events.csv)")
	for _, f := range schedule {
		if f.Kind != faultFailover || s.cnpg {
			require.Positive(t, rep.Faults[f.Kind], "no %s took", f.Kind)
		}
	}
	require.GreaterOrEqual(t, rep.WorkerRestarts, rep.Faults[faultWorkerKill], "every killed worker restarted")
}

func copyCounts(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// soakSummary is summary.md.
func soakSummary(r soakReport, repeats map[string]int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Soak %s\n\n", cmp.Or(r.Ref, "(unknown ref)"))
	dirty := ""
	if r.Dirty {
		dirty = " (dirty tree)"
	}
	fmt.Fprintf(&b, "Ref %s%s, broker image `%s`, context `%s`: %d shards (%s), %d brokers, %d workers; "+
		"%.0f jobs/s offered open loop (at most %d in flight), %d ms jobs, chunks of %d.\n\n",
		r.Ref, dirty, r.BrokerImage, r.Context, r.Shards, r.Mode, r.Brokers, r.Workers, r.Rate, r.Inflight, r.JobMS, r.Chunk)
	row := func(k, v string) { fmt.Fprintf(&b, "| %s | %s |\n", k, v) }
	b.WriteString("| | |\n|---|---|\n")
	row("jobs", strconv.Itoa(r.Jobs))
	row("duration", fmt.Sprintf("%s planned; produced for %.0f s, drained in %.0f s", r.Duration, r.ProducedFor, r.DrainedIn))
	row("completed/s", fmt.Sprintf("mean %.1f, p99 %.1f (30 s samples while producing)", r.CompletedRateMean, r.CompletedRateP99))
	row("enqueue p99", fmt.Sprintf("%.1f ms", r.EnqueueP99Ms))
	row("behind", fmt.Sprintf("%d of %d offered calls could not start", r.Behind, r.Behind+r.Started))
	row("failed enqueue calls", fmt.Sprint(r.FailedEnqueues))
	kinds := slices.Sorted(func(yield func(string) bool) {
		for k := range r.Faults {
			if !yield(k) {
				return
			}
		}
	})
	var faults []string
	for _, k := range kinds {
		faults = append(faults, fmt.Sprintf("%s %d", k, r.Faults[k]))
	}
	row("faults", strings.Join(faults, ", ")+fmt.Sprintf("; %d errors", r.FaultErrors))
	for _, s := range r.Skipped {
		row("skipped", s)
	}
	row("worker container restarts", strconv.Itoa(r.WorkerRestarts))
	row("lost", strconv.Itoa(r.Lost))
	row("unexplained", strconv.Itoa(r.Unexplained))
	row("rows on the wrong shard", strconv.Itoa(r.Misplaced))
	row("redelivery rate", fmt.Sprintf("%.4f ((Σ attempts − jobs) / jobs = (%d − %d) / %d)", r.RedeliveryRate, r.Attempts, r.Jobs, r.Jobs))
	row("repeated starts by cause", fmt.Sprint(repeats))
	row("harness max RSS", fmt.Sprintf("%.0f MiB before parsing logs, %.0f MiB after parsing %.0f MiB of them",
		float64(r.MaxRSSBeforeParse)/(1<<20), float64(r.MaxRSSAfterParse)/(1<<20), float64(r.LogBytes)/(1<<20)))
	b.WriteString("\n| chunk | jobs | by shard | acknowledged | lost | unexplained | attempts | max RSS after |\n|---|---|---|---|---|---|---|---|\n")
	for _, ch := range r.Chunks {
		fmt.Fprintf(&b, "| %d | %d | %v | %d | %d | %d | %d | %.0f MiB |\n", ch.Chunk, ch.Jobs, ch.JobsPerShard,
			ch.Acknowledged, ch.Lost, ch.Unexplained, ch.Attempts, float64(ch.MaxRSSAfter)/(1<<20))
	}
	b.WriteString("\nevents.csv lists every fault; timeline.csv every 30 s sample (throughput.svg, pickup-p99.svg, table.svg).\n")
	return b.String()
}
