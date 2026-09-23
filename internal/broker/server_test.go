package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
	"github.com/rpasco787/hopper/internal/store"
)

// startServer runs a real Server on a loopback port and returns it with a
// client connection. Cleanup stops it hard, so a test that already shut it
// down loses nothing.
func startServer(t *testing.T, st store.Store, cfg Config, opts ...grpc.ServerOption) (*Server, *grpc.ClientConn) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := NewServer(st, cfg, opts...)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
		srv.grpc.Stop()
	})
	return srv, conn
}

type recordedRPC struct {
	method string
	err    error
}

type rpcRecorder struct {
	mu      sync.Mutex
	unary   []recordedRPC
	streams []recordedRPC
}

func (r *rpcRecorder) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	r.mu.Lock()
	r.unary = append(r.unary, recordedRPC{method: info.FullMethod, err: err})
	r.mu.Unlock()
	return resp, err
}

func (r *rpcRecorder) streamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, stream)
	r.mu.Lock()
	r.streams = append(r.streams, recordedRPC{method: info.FullMethod, err: err})
	r.mu.Unlock()
	return err
}

func (r *rpcRecorder) sawUnary(method string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.unary {
		if call.method == method {
			return true
		}
	}
	return false
}

func (r *rpcRecorder) sawStream(method string, code codes.Code) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range r.streams {
		if call.method == method && status.Code(call.err) == code {
			return true
		}
	}
	return false
}

func TestServerOptionsWrapEveryRPC(t *testing.T) {
	t.Parallel()

	rec := &rpcRecorder{}
	srv, conn := startServer(t, &fakeStore{
		getJobFn: func(context.Context, uuid.UUID) (store.Job, error) {
			return sampleJob("q"), nil
		},
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return nil, nil
		},
	}, Config{PollInterval: 5 * time.Millisecond},
		grpc.ChainUnaryInterceptor(rec.unaryInterceptor),
		grpc.ChainStreamInterceptor(rec.streamInterceptor))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := hopperv1.NewHopperServiceClient(conn)
	_, err := client.GetJob(ctx, &hopperv1.GetJobRequest{Id: uuid.NewString()})
	require.NoError(t, err)

	leases, err := client.LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, leases.Send(subscribeMsg(1)))

	watch, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	require.NoError(t, err)

	require.NoError(t, srv.Shutdown(ctx))
	require.Eventually(t, func() bool {
		return rec.sawUnary("/hopper.v1.HopperService/GetJob") &&
			rec.sawStream("/hopper.v1.HopperService/LeaseJobs", codes.Unavailable) &&
			rec.sawStream("/grpc.health.v1.Health/Watch", codes.Unavailable)
	}, time.Second, 5*time.Millisecond)
}

func TestServerReportsItsServices(t *testing.T) {
	t.Parallel()

	srv := NewServer(&fakeStore{}, Config{})
	t.Cleanup(srv.grpc.Stop)
	services := srv.GetServiceInfo()
	require.Contains(t, services, "hopper.v1.HopperService")
	require.Len(t, services["hopper.v1.HopperService"].Methods, 9)
	require.Contains(t, services, "grpc.health.v1.Health")
}

func TestServerOptionsCannotRaiseTheMessageCeiling(t *testing.T) {
	t.Parallel()

	_, conn := startServer(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			return sampleJob(p.Queue), false, nil
		},
	}, Config{}, grpc.MaxRecvMsgSize(64<<20))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := hopperv1.NewHopperServiceClient(conn).Enqueue(ctx, &hopperv1.EnqueueRequest{
		Job: &hopperv1.JobSpec{Queue: "q", Payload: make([]byte, 5<<20)},
	})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestServerServesHealthAndTheService(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := NewServer(&fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			return sampleJob(p.Queue), true, nil
		},
	}, Config{})

	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Health is what the Kubernetes readiness probe will call in Session 8.
	health, err := grpc_health_v1.NewHealthClient(conn).Check(ctx,
		&grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, health.GetStatus())

	_, err = hopperv1.NewHopperServiceClient(conn).Enqueue(ctx,
		&hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
	require.NoError(t, err)

	// Shutdown must return, and Serve must return nil rather than an error.
	require.NoError(t, srv.Shutdown(ctx))
	require.NoError(t, <-served)
}

// TestServerShutdownEndsLeaseStreams is the property that makes a rolling
// deploy safe. GracefulStop waits for in-flight RPCs; a lease stream has no
// reason of its own to end, so without Drain the broker would hang until
// SIGKILL and every in-flight job would be abandoned mid-handler.
func TestServerShutdownEndsLeaseStreams(t *testing.T) {
	t.Parallel()

	srv, conn := startServer(t, &fakeStore{
		leaseFn: func(context.Context, []string, int, time.Duration, string) ([]store.Job, error) {
			return nil, nil
		},
	}, Config{PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := hopperv1.NewHopperServiceClient(conn).LeaseJobs(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(subscribeMsg(1)))

	// LeaseJobs owns its drain path and runs on the RPC's own goroutine. If
	// drainStreams wrapped it, it would be counted here as an abandoned child.
	require.Never(t, func() bool { return srv.service.auxActive.Load() != 0 },
		200*time.Millisecond, 10*time.Millisecond, "lease streams bypass drainStreams")

	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(ctx) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return: the lease stream was never told to drain")
	}
}

// drainedCleanly accepts how a drained auxiliary stream may look to its
// client: an explicit Unavailable, a clean EOF, or the transport closing under
// it, which gRPC also reports as Unavailable.
func drainedCleanly(err error) bool {
	return errors.Is(err, io.EOF) || status.Code(err) == codes.Unavailable
}

// TestServerShutdownEndsAuxiliaryStreams: health Watch (for the process and for
// the named service) and a reflection stream are all open when Shutdown runs.
// None has a reason to end, so GracefulStop would wait on them forever.
func TestServerShutdownEndsAuxiliaryStreams(t *testing.T) {
	t.Parallel()

	srv, conn := startServer(t, &fakeStore{}, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	services := []string{"", hopperv1.HopperService_ServiceDesc.ServiceName}
	var watches []grpc_health_v1.Health_WatchClient
	for _, name := range services {
		w, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx,
			&grpc_health_v1.HealthCheckRequest{Service: name})
		require.NoError(t, err)
		first, err := w.Recv()
		require.NoError(t, err)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, first.GetStatus(), "service %q", name)
		watches = append(watches, w)
	}

	refl, err := grpc_reflection_v1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	require.NoError(t, err)
	require.NoError(t, refl.Send(&grpc_reflection_v1.ServerReflectionRequest{
		MessageRequest: &grpc_reflection_v1.ServerReflectionRequest_ListServices{ListServices: "*"},
	}))
	_, err = refl.Recv()
	require.NoError(t, err, "reflection lists the services before shutdown")

	require.Eventually(t, func() bool { return srv.service.auxActive.Load() == 3 },
		2*time.Second, 5*time.Millisecond, "all three auxiliary handlers are running")

	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not return: an auxiliary stream was never ended")
	}

	// Every watch ends. A NOT_SERVING update may arrive first; anything that is
	// not a drained-stream error is a failure.
	for i, w := range watches {
		var err error
		for err == nil {
			_, err = w.Recv()
		}
		require.Truef(t, drainedCleanly(err), "watch %q ended with %v", services[i], err)
	}
	_, err = refl.Recv()
	require.True(t, drainedCleanly(err), "reflection ended with %v", err)

	// Both statuses, read from the health server itself since the listener is gone.
	for _, name := range services {
		resp, err := srv.health.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: name})
		require.NoError(t, err)
		require.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp.GetStatus(), "service %q", name)
	}

	// The child handlers were abandoned, not joined; gRPC cancelling their
	// contexts is what lets them go.
	require.Eventually(t, func() bool { return srv.service.auxActive.Load() == 0 },
		5*time.Second, 5*time.Millisecond, "auxiliary handlers must exit after the drain")
}

// TestServerShutdownWaitsForInFlightUnary: a unary mutation already running is
// finished, not abandoned, by a graceful shutdown.
func TestServerShutdownWaitsForInFlightUnary(t *testing.T) {
	t.Parallel()

	entered, release := make(chan struct{}), make(chan struct{})
	srv, conn := startServer(t, &fakeStore{
		enqueueFn: func(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
			close(entered)
			<-release
			return sampleJob(p.Queue), true, nil
		},
	}, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	enqueued := make(chan error, 1)
	go func() {
		_, err := hopperv1.NewHopperServiceClient(conn).Enqueue(ctx,
			&hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
		enqueued <- err
	}()
	waitFor(t, entered, "the Enqueue handler to start")

	shutdown := make(chan error, 1)
	go func() { shutdown <- srv.Shutdown(ctx) }()
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned (%v) while a unary call was still running", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-enqueued, "the in-flight call completes")
	require.NoError(t, <-shutdown)
}

// TestServerShutdownDeadlineStopsHard: when the shutdown context expires with a
// unary call still running, Shutdown reports the deadline and the connection is
// torn down under the client.
func TestServerShutdownDeadlineStopsHard(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	srv, conn := startServer(t, &fakeStore{
		enqueueFn: func(ctx context.Context, _ store.EnqueueParams) (store.Job, bool, error) {
			close(entered)
			<-ctx.Done() // a store call that honours cancellation, as pgx does
			return store.Job{}, false, ctx.Err()
		},
	}, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	enqueued := make(chan error, 1)
	go func() {
		_, err := hopperv1.NewHopperServiceClient(conn).Enqueue(ctx,
			&hopperv1.EnqueueRequest{Job: &hopperv1.JobSpec{Queue: "q"}})
		enqueued <- err
	}()
	waitFor(t, entered, "the Enqueue handler to start")

	shutdownCtx, cancelShutdown := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShutdown()
	err := srv.Shutdown(shutdownCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case err := <-enqueued:
		require.Error(t, err)
		require.Contains(t, []codes.Code{codes.Unavailable, codes.Canceled}, status.Code(err),
			"the client sees the connection go, not a success")
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight call was not terminated by the hard stop")
	}
}
