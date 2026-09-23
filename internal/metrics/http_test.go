package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPServerServesMetricsOnly(t *testing.T) {
	t.Parallel()
	srv := NewHTTPServer(NewRegistry())
	httpSrv := httptest.NewServer(srv.Handler)
	t.Cleanup(httpSrv.Close)

	resp, err := http.Get(httpSrv.URL + "/metrics")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), "go_goroutines")

	resp, err = http.Get(httpSrv.URL + "/")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/metrics", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestNewGRPCServerMetricsReturnsRegistrationError(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	_, err := NewGRPCServerMetrics(reg)
	require.NoError(t, err)
	_, err = NewGRPCServerMetrics(reg)
	require.ErrorContains(t, err, "metrics: register grpc")
}
