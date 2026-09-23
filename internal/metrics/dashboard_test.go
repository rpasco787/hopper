package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

const dashboardPath = "../../deploy/grafana/dashboards/queue.json"

type dashboardTarget struct {
	Datasource struct {
		UID string `json:"uid"`
	} `json:"datasource"`
	Expr string `json:"expr"`
}

// loadDashboard returns the dashboard's uid and every panel target.
func loadDashboard(t *testing.T) (string, []dashboardTarget) {
	t.Helper()
	raw, err := os.ReadFile(dashboardPath)
	require.NoError(t, err)
	var dash struct {
		UID    string `json:"uid"`
		Panels []struct {
			Title   string            `json:"title"`
			Targets []dashboardTarget `json:"targets"`
		} `json:"panels"`
	}
	require.NoError(t, json.Unmarshal(raw, &dash))
	var targets []dashboardTarget
	for _, p := range dash.Panels {
		require.NotEmpty(t, p.Targets, "panel %q has no query", p.Title)
		targets = append(targets, p.Targets...)
	}
	require.NotEmpty(t, targets)
	return dash.UID, targets
}

// TestDashboardReferencesOnlyRealMetrics is a spelling check: every metric the
// dashboard names must be a family some hopper process actually exports, so a
// rename in code fails here instead of rendering as an empty panel.
func TestDashboardReferencesOnlyRealMetrics(t *testing.T) {
	uid, targets := loadDashboard(t)
	require.Equal(t, "hopper-queue", uid)

	reg := NewRegistry()
	b, err := NewBroker(reg, []string{"q"})
	require.NoError(t, err)
	// The sample timestamp is exported only after a successful sample.
	require.NoError(t, b.UpdateDepth(nil, time.Now()))
	_, err = NewWorker(reg, []string{"q"})
	require.NoError(t, err)
	grpcMet, err := NewGRPCServerMetrics(reg)
	require.NoError(t, err)
	srv := grpc.NewServer()
	hopperv1.RegisterHopperServiceServer(srv, hopperv1.UnimplementedHopperServiceServer{})
	grpcMet.InitializeMetrics(srv)

	families, err := reg.Gather()
	require.NoError(t, err)
	exported := make(map[string]bool, len(families))
	for _, f := range families {
		exported[f.GetName()] = true
	}

	ident := regexp.MustCompile(`\b(hopper_|grpc_server_)[a-z_]+`)
	suffix := regexp.MustCompile(`_(bucket|sum|count)$`)
	for _, target := range targets {
		require.Equal(t, "prometheus", target.Datasource.UID, "expr %q", target.Expr)
		names := ident.FindAllString(target.Expr, -1)
		require.NotEmpty(t, names, "expr %q names no hopper or gRPC metric", target.Expr)
		for _, name := range names {
			family := suffix.ReplaceAllString(name, "")
			require.True(t, exported[family], "expr %q references %q, which no registry exports", target.Expr, family)
		}
	}
}

// TestDashboardQueryShape enforces the aggregation rules of docs/semantics.md
// §14: depth is replicated, so it is read from fresh brokers only and deduped
// with max, never summed raw; quantiles keep queue; rates track the scrape.
// deploy/prometheus/queries_test.yml copies these expressions; keep in sync.
func TestDashboardQueryShape(t *testing.T) {
	_, targets := loadDashboard(t)

	depthGauge := regexp.MustCompile(`hopper_queue_depth([^_a-z]|$)`)
	freshness := fmt.Sprintf("hopper_queue_depth_last_sample_timestamp_seconds < %g", DepthFreshness.Seconds())
	hopperQuantile := regexp.MustCompile(`histogram_quantile\([^,]+, sum by \(le, queue[,)][^(]*\(rate\(hopper_`)
	rate := regexp.MustCompile(`\brate\(`)
	rateWindow := regexp.MustCompile(`\[\$__rate_interval\]\)`)

	for _, target := range targets {
		expr := target.Expr
		if depthGauge.MatchString(expr) {
			require.Contains(t, expr, freshness, "depth read without the freshness filter")
		}
		require.NotContains(t, expr, "sum(hopper_queue_depth", "replicated gauge summed raw")
		require.NotContains(t, expr, "sum by (queue) (hopper_queue_depth", "replicated gauge summed raw")
		if strings.Contains(expr, "histogram_quantile") && strings.Contains(expr, "hopper_") {
			require.Regexp(t, hopperQuantile, expr, "quantile over a hopper histogram must aggregate by (le, queue")
		}
		require.Equal(t, len(rate.FindAllString(expr, -1)), len(rateWindow.FindAllString(expr, -1)),
			"every rate must use $__rate_interval: %q", expr)
	}
}
