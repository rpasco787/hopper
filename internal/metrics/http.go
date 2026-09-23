package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewHTTPServer serves g on GET /metrics and nothing else. The caller owns the
// listener and calls Shutdown; the endpoint is plaintext and unauthenticated,
// exposing queue names and counts but never payloads (docs/semantics.md §14).
func NewHTTPServer(g prometheus.Gatherer) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
