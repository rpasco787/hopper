package main

import (
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
	require.Equal(t, "info", c.logLevel)
	require.Equal(t, 30*time.Second, c.leaseDuration)
	require.Equal(t, time.Second, c.reaperInterval)
	require.Equal(t, 250*time.Millisecond, c.pollInterval)
	require.Equal(t, 24*time.Hour, c.retention)
	require.Equal(t, 30*time.Second, c.shutdownTimeout)
	require.Equal(t, 32, c.maxLeaseBatch)
}

// TestConfigReadsEveryEnvironmentVariable: each documented variable reaches its
// field, so a rename in one place cannot silently disconnect the other.
func TestConfigReadsEveryEnvironmentVariable(t *testing.T) {
	for k, v := range map[string]string{
		"HOPPER_DSN":              "postgres://env",
		"HOPPER_LISTEN":           "127.0.0.1:1234",
		"HOPPER_LOG_LEVEL":        "debug",
		"HOPPER_LEASE_DURATION":   "45s",
		"HOPPER_REAPER_INTERVAL":  "2s",
		"HOPPER_POLL_INTERVAL":    "75ms",
		"HOPPER_RETENTION":        "72h",
		"HOPPER_SHUTDOWN_TIMEOUT": "9s",
		"HOPPER_MAX_LEASE_BATCH":  "7",
	} {
		t.Setenv(k, v)
	}
	c, err := parseConfig(nil)
	require.NoError(t, err)
	require.Equal(t, config{
		dsn: "postgres://env", listen: "127.0.0.1:1234", logLevel: "debug",
		leaseDuration: 45 * time.Second, reaperInterval: 2 * time.Second,
		pollInterval: 75 * time.Millisecond, retention: 72 * time.Hour,
		shutdownTimeout: 9 * time.Second, maxLeaseBatch: 7,
	}, c)
}

func TestConfigRejectsNonPositiveKnobs(t *testing.T) {
	t.Setenv("HOPPER_DSN", "postgres://unused")
	for _, name := range []string{"lease-duration", "reaper-interval", "poll-interval", "retention", "shutdown-timeout", "max-lease-batch"} {
		for _, value := range []string{"0", "-1"} {
			if name != "max-lease-batch" {
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
