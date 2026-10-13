package main

import (
	"net/http"
	"net/http/pprof"
	"time"
)

// newDebugServer serves the runtime profiles, for profiling a broker under
// load (scripts/bench.sh PROFILE=1). It is off unless -debug-listen is set.
//
// The handlers are registered on a private mux rather than by importing
// net/http/pprof for its side effect, which would put them on
// http.DefaultServeMux for anything else in the process to serve.
// /debug/pprof/cmdline is deliberately absent: it prints the command line, and
// a DSN passed with -dsn carries the database password.
//
// There is no write timeout: a CPU profile or trace streams for as long as
// the caller asked, and pprof refuses a duration longer than the server's
// write timeout.
func newDebugServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index) // index, heap, goroutine, allocs, block, mutex
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("POST /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
}
