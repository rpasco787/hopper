package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	hopperv1 "github.com/rpasco787/hopper/gen/hopper/v1"
)

// newTestClient serves svc over an in-memory listener and returns a client for
// it. bufconn is a real grpc.Server and a real ClientConn over a pipe rather
// than a socket, so everything this package does on the wire — codec,
// interceptors, streaming, status codes — is exercised for real, with no port
// to collide on and no Docker.
//
// opts are extra server options, which the lifecycle tests use to watch
// handlers from the server side.
func newTestClient(t *testing.T, svc *Service, opts ...grpc.ServerOption) hopperv1.HopperServiceClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(append([]grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMessageBytes),
		grpc.MaxSendMsgSize(MaxMessageBytes),
	}, opts...)...)
	hopperv1.RegisterHopperServiceServer(srv, svc)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("test server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		// Drain first: Stop would sever live lease streams, and a test that
		// asserts on how a stream ends deserves the real shutdown path.
		svc.Drain()
		srv.Stop()
		lis.Close()
	})
	return hopperv1.NewHopperServiceClient(conn)
}

// newTestService is the two-liner most tests actually want.
func newTestService(t *testing.T, st *fakeStore) (hopperv1.HopperServiceClient, *Service) {
	t.Helper()
	svc := NewService(st, Config{PollInterval: 5 * time.Millisecond})
	return newTestClient(t, svc), svc
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
