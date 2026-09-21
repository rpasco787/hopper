package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rpasco787/hopper/client"
)

type fakeProducer struct {
	batches [][]client.Job
	closed  int
	enqueue func(context.Context, []client.Job, int) ([]client.Result, error)
}

func (f *fakeProducer) EnqueueBatch(ctx context.Context, jobs []client.Job) ([]client.Result, error) {
	copyOfJobs := append([]client.Job(nil), jobs...)
	f.batches = append(f.batches, copyOfJobs)
	if f.enqueue != nil {
		return f.enqueue(ctx, copyOfJobs, len(f.batches))
	}
	return make([]client.Result, len(jobs)), nil
}

func (f *fakeProducer) Close() error { f.closed++; return nil }

func clearHopperctlEnvironment(t *testing.T) {
	t.Helper()
	value, existed := os.LookupEnv("HOPPER_BROKER")
	require.NoError(t, os.Unsetenv("HOPPER_BROKER"))
	t.Cleanup(func() {
		if existed {
			require.NoError(t, os.Setenv("HOPPER_BROKER", value))
		} else {
			require.NoError(t, os.Unsetenv("HOPPER_BROKER"))
		}
	})
}

func TestEnqueueMultipleBatchesKeepsStableIndexes(t *testing.T) {
	clearHopperctlEnvironment(t)
	p := &fakeProducer{}
	p.enqueue = func(_ context.Context, jobs []client.Job, call int) ([]client.Result, error) {
		out := make([]client.Result, len(jobs))
		for i := range out {
			out[i].Deduplicated = (call+i)%2 == 0
		}
		return out, nil
	}
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{
		"enqueue", "-queue", "q", "-payload", `{"x":1}`, "-n", "5", "-batch", "2",
		"-priority", "-32768", "-max-attempts", "2147483647", "-key-prefix", "bulk",
	}, &stdout, &stderr, func(target string) (producer, error) {
		require.Equal(t, "127.0.0.1:9090", target)
		return p, nil
	})
	require.NoError(t, err)
	require.Empty(t, stderr.String())
	require.Equal(t, []int{2, 2, 1}, []int{len(p.batches[0]), len(p.batches[1]), len(p.batches[2])})

	var keys []string
	for _, batch := range p.batches {
		for _, job := range batch {
			keys = append(keys, job.IdempotencyKey)
			require.Equal(t, "q", job.Queue)
			require.JSONEq(t, `{"x":1}`, string(job.Payload.(json.RawMessage)))
			require.Equal(t, int16(-32768), job.Priority)
			require.Equal(t, int32(math.MaxInt32), job.MaxAttempts)
		}
	}
	require.Equal(t, []string{"bulk-0", "bulk-1", "bulk-2", "bulk-3", "bulk-4"}, keys)
	require.Contains(t, stdout.String(), "acknowledged=5")
	require.Contains(t, stdout.String(), "deduplicated=2")
	require.Equal(t, 1, p.closed)
}

func TestValidationHappensBeforeDial(t *testing.T) {
	clearHopperctlEnvironment(t)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"missing command", nil, "usage"},
		{"unknown command", []string{"dlq"}, "DLQ operations are not available"},
		{"positionals", []string{"enqueue", "extra"}, "unexpected positional"},
		{"empty broker", []string{"enqueue", "-broker", "", "-queue", "q"}, "broker"},
		{"empty queue", []string{"enqueue"}, "queue"},
		{"zero n", []string{"enqueue", "-queue", "q", "-n", "0"}, "n must be positive"},
		{"zero batch", []string{"enqueue", "-queue", "q", "-batch", "0"}, "batch"},
		{"large batch", []string{"enqueue", "-queue", "q", "-batch", "1001"}, "batch"},
		{"bad json", []string{"enqueue", "-queue", "q", "-payload", "{"}, "valid JSON"},
		{"priority low", []string{"enqueue", "-queue", "q", "-priority", "-32769"}, "priority"},
		{"priority high", []string{"enqueue", "-queue", "q", "-priority", "32768"}, "priority"},
		{"max attempts low", []string{"enqueue", "-queue", "q", "-max-attempts", "-1"}, "max-attempts"},
		{"max attempts high", []string{"enqueue", "-queue", "q", "-max-attempts", "2147483648"}, "max-attempts"},
		{"zero timeout", []string{"enqueue", "-queue", "q", "-timeout", "0"}, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialed := false
			var stdout, stderr bytes.Buffer
			err := runWithContext(context.Background(), tc.args, &stdout, &stderr,
				func(string) (producer, error) { dialed = true; return &fakeProducer{}, nil })
			require.ErrorContains(t, err, tc.want)
			require.False(t, dialed)
		})
	}
}

func TestNumericBoundariesParseWithoutNarrowingFirst(t *testing.T) {
	clearHopperctlEnvironment(t)
	for _, args := range [][]string{
		{"-queue", "q", "-priority", "-32768"},
		{"-queue", "q", "-priority", "32767"},
		{"-queue", "q", "-max-attempts", "2147483647"},
		{"-queue", "q", "-batch", "1"},
		{"-queue", "q", "-batch", "1000"},
		{"-queue", "q", "-n", "9223372036854775807"},
	} {
		_, err := parseEnqueueConfig(args)
		require.NoError(t, err, args)
	}
}

func TestPartialFailureReportsConfirmedAndAmbiguousRange(t *testing.T) {
	clearHopperctlEnvironment(t)
	p := &fakeProducer{enqueue: func(_ context.Context, jobs []client.Job, call int) ([]client.Result, error) {
		if call == 1 {
			return make([]client.Result, len(jobs)), nil
		}
		return nil, status.Error(codes.Unavailable, "lost response")
	}}
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{
		"enqueue", "-queue", "q", "-n", "5", "-batch", "2", "-key-prefix", "retryable",
	}, &stdout, &stderr, func(string) (producer, error) { return p, nil })
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Len(t, p.batches, 2, "an ambiguous batch is never retried automatically")
	require.Contains(t, stderr.String(), "2 confirmed")
	require.Contains(t, stderr.String(), "indexes [2,4)")
	require.Contains(t, stderr.String(), "writes may have committed")
	require.Contains(t, stderr.String(), `same -key-prefix "retryable"`)
	require.Equal(t, 1, p.closed)
}

func TestFailureWithoutKeysDoesNotInventAResumeKey(t *testing.T) {
	clearHopperctlEnvironment(t)
	p := &fakeProducer{enqueue: func(context.Context, []client.Job, int) ([]client.Result, error) {
		return nil, errors.New("unknown outcome")
	}}
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{"enqueue", "-queue", "q"},
		&stdout, &stderr, func(string) (producer, error) { return p, nil })
	require.Error(t, err)
	require.Contains(t, stderr.String(), "no stable key prefix was supplied")
	require.Contains(t, stderr.String(), "do not blindly retry")
	require.NotContains(t, stderr.String(), "generated")
}

func TestResourceExhaustedAdvisesFutureSmallerBatchesWithoutRetry(t *testing.T) {
	clearHopperctlEnvironment(t)
	p := &fakeProducer{enqueue: func(context.Context, []client.Job, int) ([]client.Result, error) {
		return nil, status.Error(codes.ResourceExhausted, "response too large")
	}}
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{
		"enqueue", "-queue", "q", "-n", "100", "-batch", "100", "-key-prefix", "fat",
	}, &stdout, &stderr, func(string) (producer, error) { return p, nil })
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Len(t, p.batches, 1)
	require.Contains(t, stderr.String(), "smaller -batch for future attempts")
	require.Contains(t, stderr.String(), "may have committed")
}

func TestOverallTimeoutCancelsTheCurrentBatch(t *testing.T) {
	clearHopperctlEnvironment(t)
	p := &fakeProducer{enqueue: func(ctx context.Context, _ []client.Job, _ int) ([]client.Result, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	var stdout, stderr bytes.Buffer
	err := runWithContext(context.Background(), []string{
		"enqueue", "-queue", "q", "-timeout", "50ms",
	}, &stdout, &stderr, func(string) (producer, error) { return p, nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, p.batches, 1)
	require.Equal(t, 1, p.closed)
}

func TestBrokerEnvironmentAndFlagPrecedence(t *testing.T) {
	clearHopperctlEnvironment(t)
	t.Setenv("HOPPER_BROKER", "env:9090")
	c, err := parseEnqueueConfig([]string{"-queue", "q"})
	require.NoError(t, err)
	require.Equal(t, "env:9090", c.broker)
	c, err = parseEnqueueConfig([]string{"-broker", "flag:9090", "-queue", "q"})
	require.NoError(t, err)
	require.Equal(t, "flag:9090", c.broker)
}

func TestUsageNamesUnavailableDLQOperations(t *testing.T) {
	require.Contains(t, usageText, "DLQ operations are not available")
}

func TestDefaultTimeoutIsOneMinute(t *testing.T) {
	clearHopperctlEnvironment(t)
	c, err := parseEnqueueConfig([]string{"-queue", "q"})
	require.NoError(t, err)
	require.Equal(t, time.Minute, c.timeout)
}
