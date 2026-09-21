package main

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net"
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
