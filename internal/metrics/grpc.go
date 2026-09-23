package metrics

import (
	"fmt"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

// NewGRPCServerMetrics returns the standard gRPC server metrics with the
// handling-time histogram enabled on RPCBuckets, registered with reg. Add its
// interceptors to the server, then call InitializeMetrics(server) so every
// method graphs as zero before its first call. LeaseJobs's handling time is
// stream lifetime, not latency; dashboards filter grpc_type="unary".
func NewGRPCServerMetrics(reg prometheus.Registerer) (*grpcprom.ServerMetrics, error) {
	m := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(grpcprom.WithHistogramBuckets(RPCBuckets)),
	)
	if err := reg.Register(m); err != nil {
		return nil, fmt.Errorf("metrics: register grpc: %w", err)
	}
	return m, nil
}
