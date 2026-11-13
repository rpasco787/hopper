package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/internal/broker"
	"github.com/rpasco787/hopper/internal/store"
)

func TestSpecMarshalsAStructPayload(t *testing.T) {
	t.Parallel()

	type payload struct {
		Order int    `json:"order"`
		Note  string `json:"note"`
	}
	at := time.Now().Add(time.Hour)
	got, err := spec(Job{
		Queue: "orders", Payload: payload{Order: 7, Note: "x"}, Priority: -3,
		RunAt: at, MaxAttempts: 9, IdempotencyKey: "k",
	})
	require.NoError(t, err)
	require.Equal(t, "orders", got.GetQueue())
	require.JSONEq(t, `{"order":7,"note":"x"}`, string(got.GetPayload()))
	require.Equal(t, int32(-3), got.GetPriority())
	require.Equal(t, int32(9), got.GetMaxAttempts())
	require.Equal(t, at.UnixMicro(), got.GetRunAt().AsTime().UnixMicro())
	require.Equal(t, "k", got.GetIdempotencyKey())
}

// TestSpecLeavesUnsetFieldsAbsent: an absent field is how the broker knows to
// apply its default, and a zero value sent as if it were a choice would not be.
func TestSpecLeavesUnsetFieldsAbsent(t *testing.T) {
	t.Parallel()

	got, err := spec(Job{Queue: "q"})
	require.NoError(t, err)
	require.Nil(t, got.GetPayload(), "a nil payload is absent, not the JSON null")
	require.Nil(t, got.GetRunAt())
	require.Nil(t, got.IdempotencyKey)
}

func TestSpecSendsRawJSONUntouched(t *testing.T) {
	t.Parallel()

	got, err := spec(Job{Queue: "q", Payload: json.RawMessage(`{"a":  1}`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1}`, string(got.GetPayload()))
}

func TestSpecReturnsMarshalErrors(t *testing.T) {
	t.Parallel()

	_, err := spec(Job{Queue: "q", Payload: make(chan int)})
	require.ErrorContains(t, err, "marshal payload")

	_, err = spec(Job{Queue: "q", Payload: json.RawMessage(`{`)})
	require.ErrorContains(t, err, "marshal payload", "malformed raw JSON is caught before the wire")
}

// TestEnqueueBatchNamesTheBadItem needs no broker: the failure happens before
// any RPC, so a zero Client is enough and proves nothing was sent.
func TestEnqueueBatchNamesTheBadItem(t *testing.T) {
	t.Parallel()

	_, err := (&Client{}).EnqueueBatch(context.Background(), []Job{
		{Queue: "a"},
		{Queue: "b", Payload: make(chan int)},
	})
	require.ErrorContains(t, err, "jobs[1]")
}

func TestWrapPreservesStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		code     codes.Code
		sentinel error
	}{
		{codes.NotFound, ErrNotFound},
		{codes.FailedPrecondition, ErrTerminal},
	} {
		err := wrap("test", status.Error(tc.code, "detail"))
		require.ErrorIs(t, err, tc.sentinel)
		require.Equal(t, tc.code, status.Code(err))
	}
}

// Every other code passes through with its status intact and matches neither
// sentinel, so a caller's errors.Is(err, ErrNotFound) cannot fire by accident.
func TestWrapPassesOtherCodesThrough(t *testing.T) {
	t.Parallel()

	for _, code := range []codes.Code{codes.Aborted, codes.Internal, codes.ResourceExhausted, codes.Unavailable, codes.InvalidArgument} {
		err := wrap("test", status.Error(code, "detail"))
		require.Equal(t, code, status.Code(err))
		require.NotErrorIs(t, err, ErrNotFound)
		require.NotErrorIs(t, err, ErrTerminal)
	}
}

// enqueueOnlyStore serves just enough of the store for the Dial tests. Any
// other method panics on the nil embedded interface, which is the assertion.
type enqueueOnlyStore struct{ store.Store }

func (enqueueOnlyStore) Enqueue(_ context.Context, p store.EnqueueParams) (store.Job, bool, error) {
	now := time.Now().UTC()
	return store.Job{ID: uuid.New(), Queue: p.Queue, State: store.StateAvailable, RunAt: now, CreatedAt: now, UpdatedAt: now}, true, nil
}

func (enqueueOnlyStore) GetJob(context.Context, uuid.UUID) (store.Job, error) {
	return store.Job{}, store.ErrNotFound
}

func serve(t *testing.T, st store.Store) string {
	t.Helper()
	return serveWith(t, st, broker.Config{})
}

// serveWith is serve with a broker config and extra server options.
func serveWith(t *testing.T, st store.Store, cfg broker.Config, opts ...grpc.ServerOption) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := broker.NewServer(st, cfg, opts...)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return lis.Addr().String()
}

// TestDialDefaultsToPlaintextAlongsideOtherOptions: an option that has nothing
// to do with credentials must not knock out the default. Without a transport
// credential grpc.NewClient refuses to build, so a working call proves the
// default survived.
func TestDialDefaultsToPlaintextAlongsideOtherOptions(t *testing.T) {
	t.Parallel()

	c, err := Dial(serve(t, enqueueOnlyStore{}), grpc.WithUserAgent("test"))
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.Enqueue(ctx, Job{Queue: "q"})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, res.ID)
	require.False(t, res.Deduplicated)

	_, err = c.GetJob(ctx, uuid.New())
	require.ErrorIs(t, err, ErrNotFound, "the broker's NotFound reaches the caller as the sentinel")
}

// recordingCreds is a transport credential that notes it was used and then
// fails the handshake. Reaching it at all proves the caller's credentials, not
// the plaintext default, were the ones Dial ended up with.
type recordingCreds struct {
	credentials.TransportCredentials // nil; only what grpc calls before a handshake is implemented
	handshakes                       chan struct{}
}

func (r *recordingCreds) ClientHandshake(_ context.Context, _ string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn.Close()
	select {
	case r.handshakes <- struct{}{}:
	default:
	}
	return nil, nil, context.Canceled
}
func (r *recordingCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "recording"}
}
func (r *recordingCreds) Clone() credentials.TransportCredentials { return r }

func TestDialLetsExplicitCredentialsOverrideTheDefault(t *testing.T) {
	t.Parallel()

	// A bare TCP listener: it accepts and does nothing. Plaintext would get no
	// answer from it; only the caller's credentials ever see the connection.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	creds := &recordingCreds{handshakes: make(chan struct{}, 1)}
	c, err := Dial(lis.Addr().String(), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.GetJob(ctx, uuid.New())
	require.Error(t, err, "the handshake was refused, so the call cannot succeed")

	select {
	case <-creds.handshakes:
	default:
		t.Fatal("the caller's credentials were never used: the plaintext default won")
	}
}

// countingStore answers GetJob with NotFound and counts the calls, so a test
// can tell which broker served them.
type countingStore struct {
	store.Store
	calls atomic.Int32
}

func (s *countingStore) GetJob(context.Context, uuid.UUID) (store.Job, error) {
	s.calls.Add(1)
	return store.Job{}, store.ErrNotFound
}

// twoBrokers resolves one target to both addresses, the way DNS resolves a
// headless Service or a shared Compose alias to every broker.
func twoBrokers(a, b string) (string, grpc.DialOption) {
	r := manual.NewBuilderWithScheme("hoppertest")
	r.InitialState(resolver.State{Endpoints: []resolver.Endpoint{
		{Addresses: []resolver.Address{{Addr: a}}},
		{Addresses: []resolver.Address{{Addr: b}}},
	}})
	return "hoppertest:///brokers", grpc.WithResolvers(r)
}

// TestDialSpreadsCallsAcrossEveryResolvedBroker: brokers are stateless, so a
// producer should use all of them. Under pick_first every call goes to the
// first address forever.
func TestDialSpreadsCallsAcrossEveryResolvedBroker(t *testing.T) {
	t.Parallel()

	a, b := &countingStore{}, &countingStore{}
	target, resolve := twoBrokers(serve(t, a), serve(t, b))
	c, err := Dial(target, resolve)
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Round robin skips a broker until its connection is ready, so keep
	// calling until both have answered.
	require.Eventually(t, func() bool {
		if _, err := c.GetJob(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
			return false
		}
		return a.calls.Load() > 0 && b.calls.Load() > 0
	}, 5*time.Second, 5*time.Millisecond, "every call went to one broker")
}

// connEvents counts transports opened and closed. As a server option it sees
// the broker's side; as a dial option, the client's.
type connEvents struct{ begins, ends atomic.Int32 }

func (c *connEvents) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context   { return ctx }
func (c *connEvents) HandleRPC(context.Context, stats.RPCStats)                         {}
func (c *connEvents) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (c *connEvents) HandleConn(_ context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		c.begins.Add(1)
	case *stats.ConnEnd:
		c.ends.Add(1)
	}
}

// TestIdleConnectionSurvivesKeepalive: Dial pings an idle broker every 10s.
// gRPC's default enforcement forgives the first ping after the server last
// wrote, counts each later early ping as a strike, and answers the third
// strike (the fourth ping, about 40s in) with GOAWAY "too_many_pings". The
// broker's own policy must allow the SDK's rate, so the connection has to
// survive a full minute, well past that point. -short skips it.
func TestIdleConnectionSurvivesKeepalive(t *testing.T) {
	if testing.Short() {
		t.Skip("waits a minute through keepalive pings")
	}
	t.Parallel()

	events := &connEvents{}
	c, err := Dial(serveWith(t, enqueueOnlyStore{}, broker.Config{}, grpc.StatsHandler(events)))
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.GetJob(ctx, uuid.New())
	require.ErrorIs(t, err, ErrNotFound)
	require.Never(t, func() bool { return events.ends.Load() > 0 }, time.Minute, 500*time.Millisecond,
		"the broker closed an idle connection that was only pinging")
	require.Equal(t, int32(1), events.begins.Load())
}

// freezer is a TCP proxy that can stop forwarding without closing anything,
// which is how a powered-off broker or a dropped network path looks to the
// client: no FIN, no RST, only silence.
type freezer struct {
	lis    net.Listener
	target string
	frozen atomic.Bool
}

func startFreezer(t *testing.T, target string) *freezer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &freezer{lis: lis, target: target}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go f.pipe(c)
		}
	}()
	return f
}

func (f *freezer) addr() string { return f.lis.Addr().String() }

func (f *freezer) pipe(client net.Conn) {
	server, err := net.Dial("tcp", f.target)
	if err != nil {
		client.Close()
		return
	}
	forward := func(dst, src net.Conn) {
		defer dst.Close()
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if err != nil {
				return
			}
			if f.frozen.Load() {
				continue // dropped, as on a dead path; the proxy is never thawed
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
	}
	go forward(server, client)
	go forward(client, server)
}

// TestSilentBrokerIsNoticedInAboutFifteenSeconds: the path to the broker
// goes silent mid-connection. The client's next ping, 10s after its last
// read, gets no answer, and it closes the connection 5s later. -short skips it.
func TestSilentBrokerIsNoticedInAboutFifteenSeconds(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a keepalive timeout")
	}
	t.Parallel()

	proxy := startFreezer(t, serve(t, enqueueOnlyStore{}))
	events := &connEvents{}
	c, err := Dial(proxy.addr(), grpc.WithStatsHandler(events))
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = c.GetJob(ctx, uuid.New())
	require.ErrorIs(t, err, ErrNotFound)
	proxy.frozen.Store(true)
	frozenAt := time.Now()
	require.Eventually(t, func() bool { return events.ends.Load() > 0 }, 25*time.Second, 100*time.Millisecond,
		"the client never noticed the silent broker")
	took := time.Since(frozenAt)
	t.Logf("silent broker noticed after %s", took.Round(100*time.Millisecond))
	require.Less(t, took, 17*time.Second, "Time + Timeout is 15s")
}

// TestConnectionAgeFindsANewBroker: the name resolves to broker A, and B is
// added later, as when brokers scale up. The resolver reports B only when the
// client asks it to resolve again. A gRPC client asks when a connection closes
// or receives GOAWAY. Nothing fails here, so without a connection age the
// client never asks and B never gets a call. The first subtest is the control
// that shows this.
func TestConnectionAgeFindsANewBroker(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		age   time.Duration
		finds bool
	}{
		{"without a connection age", 0, false},
		{"with a connection age", 200 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, b := &countingStore{}, &countingStore{}
			addrA := serveWith(t, a, broker.Config{MaxConnectionAge: tc.age})
			addrB := serve(t, b)

			first := resolver.Endpoint{Addresses: []resolver.Address{{Addr: addrA}}}
			added := resolver.Endpoint{Addresses: []resolver.Address{{Addr: addrB}}}
			r := manual.NewBuilderWithScheme("hoppertest")
			r.InitialState(resolver.State{Endpoints: []resolver.Endpoint{first}})
			r.ResolveNowCallback = func(resolver.ResolveNowOptions) {
				// Not inline: updating state from inside ResolveNow can
				// deadlock the channel.
				go r.UpdateState(resolver.State{Endpoints: []resolver.Endpoint{first, added}})
			}
			c, err := Dial("hoppertest:///brokers", grpc.WithResolvers(r))
			require.NoError(t, err)
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reachedB := func() bool {
				_, _ = c.GetJob(ctx, uuid.New())
				return b.calls.Load() > 0
			}
			if tc.finds {
				require.Eventually(t, reachedB, 5*time.Second, 10*time.Millisecond,
					"the added broker never got a call")
			} else {
				require.Never(t, reachedB, time.Second, 10*time.Millisecond,
					"the client found the added broker without being told to look")
			}
		})
	}
}
