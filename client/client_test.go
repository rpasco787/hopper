package client

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
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
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := broker.NewServer(st, broker.Config{})
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
