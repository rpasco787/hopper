package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These tests set environment variables, so none of them may be parallel.

func TestConfigDefaults(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, "postgres://unused", c.dsn)
	require.Equal(t, "127.0.0.1:9090", c.listen, "loopback unless the operator opts out")
	require.Equal(t, "127.0.0.1:9091", c.metricsListen)
	require.Empty(t, c.debugListen, "profiling is opt-in")
	require.Empty(t, c.metricsQueues)
	require.Equal(t, "info", c.logLevel)
	require.Equal(t, 30*time.Second, c.leaseDuration)
	require.Equal(t, time.Second, c.reaperInterval)
	require.Equal(t, 250*time.Millisecond, c.pollInterval)
	require.Equal(t, 24*time.Hour, c.retention)
	require.Equal(t, 30*time.Second, c.shutdownTimeout)
	require.Equal(t, 32, c.maxLeaseBatch)
	require.Equal(t, 16, c.dbMaxConns)
	require.True(t, c.scheduler, "every broker contends for the scheduler unless told not to")
	require.Equal(t, 5*time.Second, c.schedulerInterval)
	require.Empty(t, c.cronFile)
	require.Nil(t, c.schedules, "no file leaves cron_schedules unmanaged")
}

// TestConfigReadsEveryEnvironmentVariable: each documented variable reaches its
// field, so a rename in one place cannot silently disconnect the other.
func TestConfigReadsEveryEnvironmentVariable(t *testing.T) {
	for k, v := range map[string]string{
		"HOPPER_DSN":                "postgres://env",
		"HOPPER_LISTEN":             "127.0.0.1:1234",
		"HOPPER_METRICS_LISTEN":     "127.0.0.1:1235",
		"HOPPER_DEBUG_LISTEN":       "127.0.0.1:1236",
		"HOPPER_METRICS_QUEUES":     "alpha, beta",
		"HOPPER_LOG_LEVEL":          "debug",
		"HOPPER_LEASE_DURATION":     "45s",
		"HOPPER_REAPER_INTERVAL":    "2s",
		"HOPPER_POLL_INTERVAL":      "75ms",
		"HOPPER_RETENTION":          "72h",
		"HOPPER_SHUTDOWN_TIMEOUT":   "9s",
		"HOPPER_MAX_LEASE_BATCH":    "7",
		"HOPPER_DB_MAX_CONNS":       "12",
		"HOPPER_SCHEDULER":          "false",
		"HOPPER_SCHEDULER_INTERVAL": "3s",
	} {
		t.Setenv(k, v)
	}
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, config{
		dsn: "postgres://env", listen: "127.0.0.1:1234", metricsListen: "127.0.0.1:1235", debugListen: "127.0.0.1:1236",
		metricsQueues: []string{"alpha", "beta"}, logLevel: "debug",
		leaseDuration: 45 * time.Second, reaperInterval: 2 * time.Second,
		pollInterval: 75 * time.Millisecond, retention: 72 * time.Hour,
		shutdownTimeout: 9 * time.Second, maxLeaseBatch: 7, dbMaxConns: 12,
		scheduler: false, schedulerInterval: 3 * time.Second,
	}, c)
}

func TestConfigMetricsListen(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	t.Setenv("HOPPER_METRICS_LISTEN", "127.0.0.1:9191")
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9191", c.metricsListen)

	c, err = parseConfig([]string{"-metrics-listen", "127.0.0.1:9292"})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9292", c.metricsListen, "the flag wins over the environment")

	c, err = parseConfig([]string{"-metrics-listen="})
	require.NoError(t, err)
	require.Empty(t, c.metricsListen, "empty disables metrics")

	_, err = parseConfig([]string{"-metrics-listen", "nohost"})
	require.EqualError(t, err, "invalid metrics-listen address")
	require.NotContains(t, err.Error(), "nohost")
}

func TestConfigDebugListen(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	c, err := parseConfig([]string{"-debug-listen", "127.0.0.1:6060"})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:6060", c.debugListen)

	_, err = parseConfig([]string{"-debug-listen", "nohost"})
	require.EqualError(t, err, "invalid debug-listen address")
}

// The pool has one knob. A DSN that sets it too is refused, without echoing
// the DSN, which carries the password.
func TestConfigRefusesAPoolSizeInTheDSN(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://u:hunter2@h/db?pool_max_conns=5")
	_, err := parseConfig(nil)
	require.EqualError(t, err, "set -db-max-conns rather than pool_max_conns in the DSN")
	_, err = parseConfig([]string{"-db-max-conns", "3000000000"})
	require.ErrorContains(t, err, "pool_max_conns", "the DSN is checked first")

	t.Setenv("HOPPER_DSN", "postgres://unused")
	_, err = parseConfig([]string{"-db-max-conns", "3000000000"})
	require.EqualError(t, err, "db-max-conns exceeds 2147483647")
}

// A pool no larger than the connections held for a whole term leaves none for
// queries, and every lease, heartbeat and ack would wait forever. The
// wakeup listener holds one in every broker, and the scheduler role one more
// in whichever broker leads.
func TestConfigLeavesAConnectionForQueries(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	for _, tc := range []struct {
		args []string
		err  string
	}{
		{args: []string{"-db-max-conns", "2"}, err: "db-max-conns must be at least 3 with -scheduler=true"},
		{args: []string{"-db-max-conns", "3"}},
		{args: []string{"-scheduler=false", "-db-max-conns", "1"}, err: "db-max-conns must be at least 2 with -scheduler=false"},
		{args: []string{"-scheduler=false", "-db-max-conns", "2"}},
	} {
		c, err := parseConfig(tc.args)
		if tc.err != "" {
			require.EqualError(t, err, tc.err, "%v", tc.args)
			continue
		}
		require.NoError(t, err, "%v", tc.args)
		require.Equal(t, tc.args[len(tc.args)-1], strconv.Itoa(c.dbMaxConns))
	}
}

func TestConfigMetricsQueues(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	c, err := parseConfig([]string{"-metrics-queues", "a, b"})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, c.metricsQueues)

	tooMany := make([]string, MaxMetricsQueues+1)
	for i := range tooMany {
		tooMany[i] = strings.Repeat("q", i+1)
	}
	for _, tc := range []struct {
		name, raw, want string
	}{
		{"empty entry", "a,,b", "metrics-queues contains an empty entry"},
		{"duplicate", "secret,secret", "metrics-queues contains a duplicate"},
		{"reserved", "_other", "metrics-queues contains the reserved queue"},
		{"too many", strings.Join(tooMany, ","), "metrics-queues exceeds the maximum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig([]string{"-metrics-queues", tc.raw})
			require.ErrorContains(t, err, tc.want)
			require.NotContains(t, err.Error(), tc.raw)
		})
	}
}

func TestAwaitServing(t *testing.T) {
	t.Parallel()
	t.Run("metrics error", func(t *testing.T) {
		want := errors.New("metrics stopped")
		metricsErr := make(chan error, 1)
		metricsErr <- want
		err, served := awaitServing(context.Background(), make(chan error), metricsErr)
		require.ErrorIs(t, err, want)
		require.False(t, served)
	})
	t.Run("clean metrics close is ignored", func(t *testing.T) {
		serveErr := make(chan error, 1)
		metricsErr := make(chan error, 1)
		metricsErr <- http.ErrServerClosed
		serveErr <- nil
		err, served := awaitServing(context.Background(), serveErr, metricsErr)
		require.NoError(t, err)
		require.True(t, served)
	})
	t.Run("nil metrics channel never fires", func(t *testing.T) {
		serveErr := make(chan error, 1)
		serveErr <- nil
		err, served := awaitServing(context.Background(), serveErr, nil)
		require.NoError(t, err)
		require.True(t, served)
	})
	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err, served := awaitServing(ctx, make(chan error), make(chan error))
		require.NoError(t, err)
		require.False(t, served)
	})
}

func TestFinalServingErrorPreservesMetricsFailure(t *testing.T) {
	t.Parallel()
	metricsFailure := errors.New("metrics stopped")
	grpcDone := make(chan error, 1)
	grpcDone <- nil

	err := finalServingError(metricsFailure, false, grpcDone, nil, nil)

	require.ErrorIs(t, err, metricsFailure)
}

func TestConfigRejectsNonPositiveKnobs(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	for _, name := range []string{"lease-duration", "reaper-interval", "poll-interval", "retention", "shutdown-timeout", "scheduler-interval", "max-lease-batch", "db-max-conns"} {
		for _, value := range []string{"0", "-1"} {
			if name != "max-lease-batch" && name != "db-max-conns" {
				value += "s"
			}
			_, err := parseConfig([]string{"-" + name, value})
			require.ErrorContains(t, err, "must be positive", "-%s %s", name, value)
		}
	}
}

// TestConfigRejectsNonPositiveEnvironment: the same rule applies whichever way
// the value arrives, since a rejected retention must never reach the janitor.
func TestConfigRejectsNonPositiveEnvironment(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	t.Setenv("HOPPER_RETENTION", "-24h")
	_, err := parseConfig(nil)
	require.ErrorContains(t, err, "retention must be positive")
}

func TestConfigFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	t.Setenv("HOPPER_REAPER_INTERVAL", "invalid")
	_, err := parseConfig(nil)
	require.ErrorContains(t, err, "HOPPER_REAPER_INTERVAL")
	c, err := parseConfig([]string{"-reaper-interval", "25ms"})
	require.NoError(t, err)
	require.Equal(t, 25*time.Millisecond, c.reaperInterval)
	require.Equal(t, "127.0.0.1:9090", c.listen)
}

func TestConfigInvalidEnvironmentNamesTheVariableNotTheValue(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://user:hunter2@host/db")
	t.Setenv("HOPPER_MAX_LEASE_BATCH", "lots")
	_, err := parseConfig(nil)
	require.ErrorContains(t, err, "HOPPER_MAX_LEASE_BATCH")
	require.NotContains(t, err.Error(), "hunter2")
}

func TestConfigRequiresADSN(t *testing.T) {
	t.Setenv("HOPPER_DSN", "")
	_, err := parseConfig(nil)
	require.ErrorContains(t, err, "HOPPER_DSN")
}

func TestConfigRejectsBadLogLevelBeforeAnythingStarts(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	_, err := parseConfig([]string{"-log-level", "shouting"})
	require.ErrorContains(t, err, "invalid log level")
	for _, ok := range []string{"debug", "info", "warn", "error", "INFO"} {
		_, err := parseConfig([]string{"-log-level", ok})
		require.NoError(t, err, ok)
	}
}

func TestConfigRejectsPositionalArguments(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	_, err := parseConfig([]string{"extra"})
	require.ErrorContains(t, err, "unexpected positional arguments")
}

// writeCronFile writes body to a fresh cron file and returns its path.
func writeCronFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schedules.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

const validCronFile = `{"schedules": [
	{"name": "heartbeat", "queue": "sleep", "cron": "@every 10s", "payload": {"ms": 50}},
	{"name": "burn", "queue": "cpu_burn", "cron": "*/5 * * * *", "timezone": "America/New_York"}
]}`

func TestConfigSchedulerCanBeDisabled(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	c, err := parseConfig([]string{"-scheduler=false"})
	require.NoError(t, err)
	require.False(t, c.scheduler)
}

func TestConfigSchedulerFlagsOverrideEnvironment(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	t.Setenv("HOPPER_SCHEDULER_INTERVAL", "invalid")
	t.Setenv("HOPPER_CRON_FILE", filepath.Join(t.TempDir(), "absent.json"))
	flagFile := writeCronFile(t, validCronFile)
	c, err := parseConfig([]string{"-scheduler-interval", "250ms", "-cron-file", flagFile})
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, c.schedulerInterval)
	require.Equal(t, flagFile, c.cronFile)
	require.Len(t, c.schedules, 2)
}

func TestConfigReadsTheCronFileFromTheEnvironment(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	path := writeCronFile(t, validCronFile)
	t.Setenv("HOPPER_CRON_FILE", path)
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, path, c.cronFile)
	require.Len(t, c.schedules, 2)
	require.Equal(t, "heartbeat", c.schedules[0].Name)
	require.Equal(t, "sleep", c.schedules[0].Queue)
	require.JSONEq(t, `{"ms":50}`, string(c.schedules[0].Payload))
	require.Equal(t, "UTC", c.schedules[0].Timezone)
	require.Equal(t, "burn", c.schedules[1].Name)
	require.Equal(t, "America/New_York", c.schedules[1].Timezone)
}

// TestConfigRejectsABadCronFile: the file is loaded by parseConfig itself, so
// a missing or malformed file fails before any pool or listener exists — the
// fast, loud failure a bad ConfigMap should produce.
func TestConfigRejectsABadCronFile(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	_, err := parseConfig([]string{"-cron-file", filepath.Join(t.TempDir(), "absent.json")})
	require.ErrorIs(t, err, os.ErrNotExist)

	bad := writeCronFile(t, `{"schedules": [{"name": "broken", "queue": "q", "cron": "not a cron"}]}`)
	_, err = parseConfig([]string{"-cron-file", bad})
	require.ErrorContains(t, err, `"broken"`)
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9090": true,
		"[::1]:9090":     true,
		"localhost:9090": true,
		"0.0.0.0:9090":   false,
		":9090":          false,
		"[::]:9090":      false,
		"10.1.2.3:9090":  false,
		"not an address": false,
	} {
		require.Equal(t, want, isLoopback(addr), addr)
	}
}
