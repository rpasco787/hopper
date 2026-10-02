package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebugServerServesProfilesButNotTheCommandLine(t *testing.T) {
	srv := httptest.NewServer(newDebugServer().Handler)
	t.Cleanup(srv.Close)

	get := func(path string) (int, string) {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode, string(body)
	}

	code, body := get("/debug/pprof/")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "goroutine")
	code, body = get("/debug/pprof/goroutine?debug=1")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "goroutine profile")
	code, _ = get("/debug/pprof/profile?seconds=1")
	require.Equal(t, http.StatusOK, code)

	// The command line can carry -dsn, and with it the database password.
	code, _ = get("/debug/pprof/cmdline")
	require.Equal(t, http.StatusNotFound, code)
	code, _ = get("/metrics")
	require.Equal(t, http.StatusNotFound, code)
}
