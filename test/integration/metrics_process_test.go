package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/metrics"
)

func scrape(t *testing.T, addr string) map[string]*io_prometheus_client.MetricFamily {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)
	return families
}

func metricHasLabels(family *io_prometheus_client.MetricFamily, want map[string]string) bool {
	if family == nil {
		return false
	}
	for _, metric := range family.GetMetric() {
		got := make(map[string]string, len(metric.GetLabel()))
		for _, label := range metric.GetLabel() {
			got[label.GetName()] = label.GetValue()
		}
		matches := true
		for name, value := range want {
			if got[name] != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func TestBrokerProcessFailsWhenMetricsPortTaken(t *testing.T) {
	binary := buildBrokerProcess(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	p := startWorkerProcess(t, binary,
		"-dsn", pool.Config().ConnConfig.ConnString(),
		"-listen", "127.0.0.1:0",
		"-metrics-listen", lis.Addr().String(),
		"-scheduler=false")
	err = p.wait(t, 10*time.Second)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, p.output())
	for _, event := range p.logSnapshot() {
		require.NotEqual(t, "broker listening", event.Message)
	}
}

func TestBrokerProcessShutdownClosesMetricsAfterExhaustedDrain(t *testing.T) {
	binary := buildBrokerProcess(t)
	p := startBrokerProcess(t, binary, "hopper-metrics-shutdown", "",
		"-scheduler=false", "-shutdown-timeout=200ms")
	brokerLog, _ := p.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	metricsLog, _ := p.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(brokerLog.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	stream, err := hopperv1.NewHopperServiceClient(conn).LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"q"}, WorkerId: "metrics-shutdown", Credit: 1,
		}},
	}))

	started := time.Now()
	p.signal(t, syscall.SIGTERM)
	require.NoError(t, p.wait(t, 3*time.Second), p.output())
	require.Less(t, time.Since(started), 3*time.Second)
	probe, err := net.DialTimeout("tcp", metricsLog.Addr, 250*time.Millisecond)
	if probe != nil {
		probe.Close()
	}
	require.Error(t, err, "metrics listener remained open after process shutdown")
}

func TestBrokerReplicasReportTheSameDepth(t *testing.T) {
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)
	binary := buildBrokerProcess(t)
	a := startBrokerProcess(t, binary, "hopper-depth-a", "",
		"-scheduler=false", "-metrics-queues=m")
	b := startBrokerProcess(t, binary, "hopper-depth-b", "",
		"-scheduler=false", "-metrics-queues=m")
	aBroker, _ := a.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	aMetrics, _ := a.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })
	bMetrics, _ := b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })
	client := newClientAt(t, aBroker.Addr)
	for range 7 {
		_, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "m"}})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return scrapedValue(t, aMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}) == 7 &&
			scrapedValue(t, bMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}) == 7
	}, 12*time.Second, 100*time.Millisecond)
	require.Zero(t, scrapedValue(t, aMetrics.Addr, "hopper_jobs_dead_total", map[string]string{"queue": "m"}))
	require.Zero(t, scrapedValue(t, bMetrics.Addr, "hopper_jobs_dead_total", map[string]string{"queue": "m"}))
	require.GreaterOrEqual(t, scrapedValue(t, aMetrics.Addr, "grpc_server_handled_total", map[string]string{
		"grpc_method": "Enqueue", "grpc_code": "OK", "grpc_type": "unary",
	}), float64(7))
	require.Zero(t, scrapedValue(t, bMetrics.Addr, "grpc_server_handled_total", map[string]string{
		"grpc_method": "Enqueue", "grpc_code": "OK", "grpc_type": "unary",
	}))

	stopBrokerProcess(t, a)
	stopBrokerProcess(t, b)
}

func TestStaleBrokerKeepsItsLastSnapshot(t *testing.T) {
	ctx := ctxT(t)
	_, err := pool.Exec(ctx, "truncate jobs, cron_schedules")
	require.NoError(t, err)
	binary := buildBrokerProcess(t)
	proxy := startTCPProxy(t, pool.Config().ConnConfig.Host, pool.Config().ConnConfig.Port)
	a := startBrokerProcess(t, binary, "hopper-stale-a", "",
		"-scheduler=false", "-metrics-queues=m")
	b := startBrokerWithDSN(t, binary, dsnThroughProxy(t, proxy.Addr()), "hopper-stale-b",
		"-scheduler=false", "-metrics-queues=m")
	aBroker, _ := a.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	aMetrics, _ := a.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })
	bMetrics, _ := b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "metrics listening" })
	client := newClientAt(t, aBroker.Addr)
	for range 5 {
		_, err := client.Enqueue(ctx, &hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "m"}})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return scrapedValue(t, aMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}) == 5 &&
			scrapedValue(t, bMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}) == 5
	}, 12*time.Second, 100*time.Millisecond)
	bSampledAt := scrapedValue(t, bMetrics.Addr, "hopper_queue_depth_last_sample_timestamp_seconds", nil)

	proxy.Stop()
	stream, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&hopperv1.LeaseJobsRequest{
		Msg: &hopperv1.LeaseJobsRequest_Subscribe{Subscribe: &hopperv1.Subscription{
			Queues: []string{"m"}, WorkerId: "stale-drainer", Credit: 5,
		}},
	}))
	for range 5 {
		leased, err := stream.Recv()
		require.NoError(t, err)
		_, err = client.Ack(ctx, &hopperv1.AckRequest{
			Id: leased.GetJob().GetId(), LeaseId: leased.GetJob().GetLeaseId(),
		})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return scrapedValue(t, aMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}) == 0
	}, 12*time.Second, 100*time.Millisecond)
	aSampledAt := scrapedValue(t, aMetrics.Addr, "hopper_queue_depth_last_sample_timestamp_seconds", nil)
	require.Greater(t, aSampledAt, bSampledAt)
	require.Eventually(t, func() bool {
		return time.Since(time.Unix(int64(bSampledAt), 0)) > metrics.DepthFreshness
	}, metrics.DepthFreshness+5*time.Second, 100*time.Millisecond)
	require.Equal(t, float64(5), scrapedValue(t, bMetrics.Addr, "hopper_queue_depth", map[string]string{"queue": "m", "state": "available"}))
	require.Equal(t, bSampledAt, scrapedValue(t, bMetrics.Addr, "hopper_queue_depth_last_sample_timestamp_seconds", nil))
	_, _ = b.waitForLog(t, 10*time.Second, func(e processLog) bool { return e.Message == "queue depth sample failed" })
	select {
	case <-b.waitDone:
		t.Fatalf("stale broker exited instead of keeping its last snapshot: %v\n%s", b.waitErr, b.output())
	default:
	}

	stopBrokerProcess(t, a)
	stopBrokerProcess(t, b)
}

func scrapedValue(t *testing.T, addr, name string, labels map[string]string) float64 {
	t.Helper()
	family := scrape(t, addr)[name]
	require.NotNil(t, family, name)
	for _, metric := range family.GetMetric() {
		if !metricHasAllLabels(metric, labels) {
			continue
		}
		switch {
		case metric.Gauge != nil:
			return metric.GetGauge().GetValue()
		case metric.Counter != nil:
			return metric.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s%v not found at %s", name, labels, addr)
	return 0
}

func metricHasAllLabels(metric *io_prometheus_client.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return len(metric.GetLabel()) == 0
	}
	for name, want := range labels {
		found := false
		for _, pair := range metric.GetLabel() {
			if pair.GetName() == name && pair.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func stopBrokerProcess(t *testing.T, p *workerProcess) {
	t.Helper()
	select {
	case <-p.waitDone:
		return
	default:
	}
	p.signal(t, syscall.SIGTERM)
	require.NoError(t, p.wait(t, 10*time.Second), p.output())
}

func startBrokerWithDSN(t *testing.T, binary, dsn, appName string, extraArgs ...string) *workerProcess {
	t.Helper()
	args := []string{
		"-dsn", dsn, "-listen", "127.0.0.1:0", "-metrics-listen", "127.0.0.1:0",
		"-scheduler-interval", processSchedulerInterval.String(), "-shutdown-timeout", "2s", "-log-level", "info",
	}
	args = append(args, extraArgs...)
	p := startWorkerProcess(t, binary, args...)
	p.waitForLog(t, 30*time.Second, func(e processLog) bool { return e.Message == "broker listening" })
	return p
}

func dsnThroughProxy(t *testing.T, proxyAddr string) string {
	t.Helper()
	u, err := url.Parse(pool.Config().ConnConfig.ConnString())
	require.NoError(t, err)
	u.Host = proxyAddr
	return u.String()
}

type tcpProxy struct {
	lis    net.Listener
	target string
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	done   chan struct{}
	once   sync.Once
}

func startTCPProxy(t *testing.T, host string, port uint16) *tcpProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &tcpProxy{
		lis: lis, target: net.JoinHostPort(host, fmt.Sprint(port)),
		conns: make(map[net.Conn]struct{}), done: make(chan struct{}),
	}
	go p.accept()
	t.Cleanup(p.Stop)
	return p
}

func (p *tcpProxy) Addr() string { return p.lis.Addr().String() }

func (p *tcpProxy) accept() {
	defer close(p.done)
	for {
		clientConn, err := p.lis.Accept()
		if err != nil {
			return
		}
		serverConn, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = clientConn.Close()
			continue
		}
		p.track(clientConn, serverConn)
		go p.pipe(clientConn, serverConn)
	}
}

func (p *tcpProxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range conns {
		p.conns[conn] = struct{}{}
	}
}

func (p *tcpProxy) pipe(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = io.Copy(a, b) }()
	go func() { defer wg.Done(); _, _ = io.Copy(b, a) }()
	wg.Wait()
	_ = a.Close()
	_ = b.Close()
	p.mu.Lock()
	delete(p.conns, a)
	delete(p.conns, b)
	p.mu.Unlock()
}

func (p *tcpProxy) Stop() {
	p.once.Do(func() {
		_ = p.lis.Close()
		p.mu.Lock()
		for conn := range p.conns {
			_ = conn.Close()
		}
		p.mu.Unlock()
		<-p.done
	})
}
