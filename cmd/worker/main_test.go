package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	workersdk "github.com/rpasco787/hopper/worker"
)

var workerEnvironment = []string{
	"HOPPER_BROKER",
	"HOPPER_WORKER_ID",
	"HOPPER_CONCURRENCY",
	"HOPPER_LEASE_DURATION",
	"HOPPER_SHUTDOWN_TIMEOUT",
	"HOPPER_LOG_LEVEL",
	"HOPPER_METRICS_LISTEN",
}

func clearWorkerEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range workerEnvironment {
		value, existed := os.LookupEnv(key)
		require.NoError(t, os.Unsetenv(key))
		t.Cleanup(func() {
			if existed {
				require.NoError(t, os.Setenv(key, value))
			} else {
				require.NoError(t, os.Unsetenv(key))
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	clearWorkerEnvironment(t)
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, config{
		broker: "127.0.0.1:9090", concurrency: 8, leaseDuration: 0,
		shutdownTimeout: 30 * time.Second, logLevel: "info",
	}, c)
}

func TestConfigReadsEveryEnvironmentVariable(t *testing.T) {
	clearWorkerEnvironment(t)
	for key, value := range map[string]string{
		"HOPPER_BROKER":           "broker.internal:9090",
		"HOPPER_WORKER_ID":        "worker-9",
		"HOPPER_CONCURRENCY":      "17",
		"HOPPER_LEASE_DURATION":   "45s",
		"HOPPER_SHUTDOWN_TIMEOUT": "9s",
		"HOPPER_LOG_LEVEL":        "debug",
	} {
		t.Setenv(key, value)
	}
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, config{
		broker: "broker.internal:9090", id: "worker-9", concurrency: 17,
		leaseDuration: 45 * time.Second, shutdownTimeout: 9 * time.Second, logLevel: "debug",
	}, c)
}

func TestConfigFlagsOverrideInvalidEnvironment(t *testing.T) {
	clearWorkerEnvironment(t)
	t.Setenv("HOPPER_CONCURRENCY", "many")
	_, err := parseConfig(nil)
	require.ErrorContains(t, err, "HOPPER_CONCURRENCY")

	c, err := parseConfig([]string{"-concurrency", "3"})
	require.NoError(t, err)
	require.Equal(t, 3, c.concurrency)
}

func TestMetricsListenConfiguration(t *testing.T) {
	clearWorkerEnvironment(t)
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Empty(t, c.metricsListen)

	t.Setenv("HOPPER_METRICS_LISTEN", "127.0.0.1:9191")
	c, err = parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9191", c.metricsListen)

	c, err = parseConfig([]string{"-metrics-listen", "127.0.0.1:9292"})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9292", c.metricsListen)

	_, err = parseConfig([]string{"-metrics-listen", "nohost"})
	require.EqualError(t, err, "invalid metrics-listen address")
}

func TestConfigValidation(t *testing.T) {
	clearWorkerEnvironment(t)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"positionals", []string{"extra"}, "unexpected positional"},
		{"empty broker", []string{"-broker", ""}, "broker"},
		{"zero concurrency", []string{"-concurrency", "0"}, "concurrency"},
		{"negative concurrency", []string{"-concurrency", "-1"}, "concurrency"},
		{"too much concurrency", []string{"-concurrency", "65537"}, "maximum"},
		{"negative lease", []string{"-lease-duration", "-1s"}, "lease-duration"},
		{"negative shutdown", []string{"-shutdown-timeout", "-1s"}, "shutdown-timeout"},
		{"bad log level", []string{"-log-level", "loud"}, "log level"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(tc.args)
			require.ErrorContains(t, err, tc.want)
		})
	}
	for _, args := range [][]string{
		{"-lease-duration", "0"},
		{"-lease-duration", "1ns"},
		{"-shutdown-timeout", "0"},
		{"-concurrency", "65536"},
	} {
		_, err := parseConfig(args)
		require.NoError(t, err, args)
	}
}

func TestDurationPayloadDefaultsAndBoundaries(t *testing.T) {
	for _, tc := range []struct {
		payload string
		want    time.Duration
	}{
		{`{}`, 100 * time.Millisecond},
		{`{"ms":0}`, 0},
		{`{"ms":250}`, 250 * time.Millisecond},
	} {
		d, err := workDuration(workJob(tc.payload, 1))
		require.NoError(t, err)
		require.Equal(t, tc.want, d)
	}
	_, err := workDuration(workJob(`{"ms":-1}`, 1))
	require.ErrorContains(t, err, "non-negative")
	_, err = workDuration(workJob(`{"ms":9223372036855}`, 1))
	require.ErrorContains(t, err, "overflows")
}

func TestSleepAndCPUBurnHonorCancellation(t *testing.T) {
	for name, handler := range map[string]workersdk.HandlerFunc{
		"sleep": sleepHandler,
		"cpu":   cpuBurnHandler,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := handler(ctx, workJob(`{"ms":1000}`, 1))
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestFailNTimesUsesDeliveryAttempt(t *testing.T) {
	require.Error(t, failNTimesHandler(context.Background(), workJob(`{"fail":2}`, 1)))
	require.Error(t, failNTimesHandler(context.Background(), workJob(`{"fail":2}`, 2)))
	require.NoError(t, failNTimesHandler(context.Background(), workJob(`{"fail":2}`, 3)))
	require.NoError(t, failNTimesHandler(context.Background(), workJob(`{}`, 1)))
	err := failNTimesHandler(context.Background(), workJob(`{"fail":-1}`, 1))
	require.ErrorContains(t, err, "non-negative")
}

func TestRunReturnsWorkerErrorsAndCleanStops(t *testing.T) {
	clearWorkerEnvironment(t)
	boom := errors.New("worker stopped badly")
	var stdout bytes.Buffer
	err := runWithContext(context.Background(), nil, &stdout,
		func(context.Context, *workersdk.Worker) error { return boom })
	require.ErrorIs(t, err, boom)

	stdout.Reset()
	err = runWithContext(context.Background(), nil, &stdout,
		func(context.Context, *workersdk.Worker) error { return nil })
	require.NoError(t, err)
	require.Contains(t, stdout.String(), `"worker_id"`)
}

func TestWorkerServesMetricsUntilStartReturns(t *testing.T) {
	clearWorkerEnvironment(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	started := make(chan struct{})
	release := make(chan struct{})
	var stdout bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runWithContext(context.Background(), []string{"-metrics-listen", "127.0.0.1:0"}, &stdout,
			func(context.Context, *workersdk.Worker) error {
				close(started)
				<-release
				return nil
			}, lis)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker start function was not called")
	}

	body := getMetrics(t, addr)
	require.Contains(t, body, "hopper_worker_inflight_jobs")
	require.Contains(t, body, "hopper_worker_handler_seconds")
	require.Contains(t, stdout.String(), `"msg":"metrics listening"`)
	require.Contains(t, stdout.String(), fmt.Sprintf(`"addr":%q`, addr))

	close(release)
	require.NoError(t, <-done)
	require.Eventually(t, func() bool { return connectionRefused(addr) }, time.Second, 10*time.Millisecond)
}

func TestWorkerMetricsClosedWhenStartFails(t *testing.T) {
	clearWorkerEnvironment(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	want := errors.New("broker is unreachable")
	err = runWithContext(context.Background(), []string{"-metrics-listen", "127.0.0.1:0"}, io.Discard,
		func(context.Context, *workersdk.Worker) error { return want }, lis)
	require.ErrorIs(t, err, want)
	require.Eventually(t, func() bool { return connectionRefused(addr) }, time.Second, 10*time.Millisecond)
}

func TestWorkerStopsWhenMetricsServerDies(t *testing.T) {
	clearWorkerEnvironment(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runWithContext(context.Background(), []string{"-metrics-listen", "127.0.0.1:0"}, io.Discard,
			func(ctx context.Context, _ *workersdk.Worker) error {
				close(started)
				<-ctx.Done()
				close(cancelled)
				return nil
			}, lis)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("worker start function was not called")
	}
	require.NoError(t, lis.Close())
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("metrics server failure did not cancel the worker context")
	}
	require.Error(t, <-done)
}

func getMetrics(t *testing.T, addr string) string {
	t.Helper()
	var body string
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		body = string(data)
		return true
	}, 2*time.Second, 10*time.Millisecond)
	return body
}

func connectionRefused(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
	if err == nil {
		_ = conn.Close()
	}
	return err != nil
}

func TestStartupBeforeBrokerAndCancellationDrainsPromptly(t *testing.T) {
	clearWorkerEnvironment(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runWithContext(ctx, []string{
			"-broker", addr, "-shutdown-timeout", "500ms",
		}, &bytes.Buffer{}, func(ctx context.Context, w *workersdk.Worker) error { return w.Run(ctx) })
	}()
	time.Sleep(150 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Less(t, time.Since(started), time.Second)
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop while reconnecting")
	}
}

func workJob(payload string, attempt int32) workersdk.Job {
	return workersdk.Job{ID: uuid.New(), Payload: []byte(payload), Attempt: attempt}
}

func TestDurationOverflowFixtureFitsInt64(t *testing.T) {
	require.Less(t, int64(math.MaxInt64/int64(time.Millisecond)), int64(9223372036855))
}
