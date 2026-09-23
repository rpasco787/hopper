package integration

import (
	"context"
	"net"
	"net/http"
	"os/exec"
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
