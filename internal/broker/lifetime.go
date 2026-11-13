package broker

import (
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Keepalive. The SDKs (client.Dial and the worker) ping every 10s, with or
// without an RPC in flight. gRPC's default enforcement allows one ping per
// five minutes and none on an idle connection: it forgives the first ping
// after the server last wrote, counts each later early ping as a strike, and
// answers the third strike with GOAWAY "too_many_pings". The floor here must
// stay below the SDKs' interval.
const (
	minClientPingInterval = 5 * time.Second
	// The broker pings a client it has heard nothing from, so a worker that
	// vanished without closing its connection has its lease stream closed in
	// about 40s instead of holding it, and a slot in the stream gauge,
	// indefinitely. A client that pings is never pinged.
	serverPingInterval = 30 * time.Second
	serverPingTimeout  = 10 * time.Second
)

// lifetimeOptions are the server options that bound how long a connection
// lives and how a dead peer is noticed.
func lifetimeOptions(cfg Config) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             minClientPingInterval,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    serverPingInterval,
			Timeout: serverPingTimeout,
		}),
	}
}

// jitter spreads d by ±10%, as gRPC does for MaxConnectionAge, so streams one
// restart opened together do not all age out, and reconnect, together.
func jitter(d time.Duration) time.Duration {
	return d + time.Duration((rand.Float64()*0.2-0.1)*float64(d))
}
