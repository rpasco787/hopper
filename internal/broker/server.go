package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// Server is a grpc.Server with hopper's services registered and a shutdown
// sequence that actually terminates.
type Server struct {
	grpc    *grpc.Server
	service *Service
	health  *health.Server
}

// NewServer registers the hopper service, the standard health service and
// server reflection. opts are applied before the server's own, which puts
// caller interceptors outermost — gRPC runs chained interceptors in the order
// they were added — so an instrumentation interceptor sees every RPC,
// including the Unavailable a drain produces. The server's own message-size
// limits come after opts and therefore win.
//
// Reflection is on deliberately. It is what lets grpcurl, grpcui and Postman
// call this server with no .proto file in hand, which turns "can I try the
// API" from a codegen exercise into one command. It exposes the service
// descriptors only — never data — and it is how the Session 13 README's
// quickstart works.
func NewServer(st store.Store, cfg Config, opts ...grpc.ServerOption) *Server {
	svc := NewService(st, cfg)
	g := grpc.NewServer(slices.Concat(opts, []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes), grpc.MaxSendMsgSize(MaxMessageBytes),
		grpc.ChainStreamInterceptor(svc.drainStreams),
	})...)

	hopperv1.RegisterHopperServiceServer(g, svc)

	h := health.NewServer()
	// The empty service name is the overall-process check, which is what
	// Kubernetes' grpc readiness probe uses by default.
	h.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	h.SetServingStatus(hopperv1.HopperService_ServiceDesc.ServiceName,
		grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(g, h)

	reflection.Register(g)

	return &Server{grpc: g, service: svc, health: h}
}

// GetServiceInfo reports the registered services. It makes *Server a
// reflection.ServiceInfoProvider, which is what an interceptor library needs
// to pre-create per-method series before the first call.
func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo { return s.grpc.GetServiceInfo() }

// Serve blocks until the server stops. A clean shutdown returns nil.
func (s *Server) Serve(lis net.Listener) error {
	if err := s.grpc.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("broker: serve: %w", err)
	}
	return nil
}

// Shutdown drains in three steps, in this order:
//
//  1. Report NOT_SERVING, so a load balancer or readiness probe stops sending
//     new work here before anything is torn down.
//  2. Drain the service, so lease streams return Unavailable and their workers
//     reconnect to another replica. This is the step GracefulStop cannot do
//     for us: it waits for in-flight RPCs, and a lease stream would wait
//     forever. Auxiliary streams (health Watch, reflection) are ended the same
//     way by drainStreams.
//  3. GracefulStop, which finishes the unary calls already in flight.
//
// If ctx expires during step 3 we stop hard. A client mid-Ack then sees a
// broken connection and retries; that is survivable because Ack is idempotent
// against the same lease_id (docs/semantics.md §5).
func (s *Server) Shutdown(ctx context.Context) error {
	s.health.Shutdown() // every registered service becomes NOT_SERVING
	s.service.Drain()

	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.grpc.Stop()
		<-stopped
		return fmt.Errorf("broker: shutdown: %w", ctx.Err())
	}
}

// drainServerStream gives an auxiliary handler a context the interceptor can
// cancel independently of the transport's.
type drainServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *drainServerStream) Context() context.Context { return s.ctx }

// drainStreams ends the streams the broker does not own — health Watch and
// reflection — when the service drains. Neither has a reason of its own to
// finish, so without this GracefulStop would wait on them exactly as it would
// on a lease stream.
//
// The child handler runs in its own goroutine so the outer one can return the
// moment the drain starts; returning is what makes gRPC cancel the transport
// under a Recv or Send the child may be blocked in. The child gets a cancelled
// context and a buffered result channel, and is abandoned rather than joined.
//
// hopper.v1 methods are passed straight through. LeaseJobs owns its drain path
// (Service.LeaseJobs) and is tested on a bare server, and the unary mutations
// must not be abandoned mid-flight: those finish during GracefulStop, with a
// hard Stop at the shutdown deadline.
func (s *Service) drainStreams(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if strings.HasPrefix(info.FullMethod, "/"+hopperv1.HopperService_ServiceDesc.ServiceName+"/") {
		return handler(srv, stream)
	}
	draining := status.Error(codes.Unavailable, "broker is shutting down")

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	select {
	case <-s.shutdown:
		return draining
	default:
	}

	done := make(chan error, 1)
	s.auxActive.Add(1)
	go func() {
		defer s.auxActive.Add(-1)
		done <- handler(srv, &drainServerStream{ServerStream: stream, ctx: ctx})
	}()
	select {
	case err := <-done:
		return err
	case <-s.shutdown:
		return draining
	case <-ctx.Done():
		return toStatus(ctx.Err())
	}
}
