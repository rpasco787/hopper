package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lockedLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedLogBuffer) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.b.Bytes())
}

func workerJSONLogger() (*slog.Logger, *lockedLogBuffer) {
	buf := &lockedLogBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func workerJSONLogs(t *testing.T, buf *lockedLogBuffer) []map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(buf.snapshot()))
	var lines []map[string]any
	for dec.More() {
		var line map[string]any
		require.NoError(t, dec.Decode(&line))
		lines = append(lines, line)
	}
	return lines
}

func workerLog(lines []map[string]any, message string) map[string]any {
	for _, line := range lines {
		if line["msg"] == message {
			return line
		}
	}
	return nil
}

func TestMalformedDeliveryLogsRawLeaseID(t *testing.T) {
	t.Parallel()
	logger, buf := workerJSONLogger()
	pool := newCapacity(1, logger)
	require.Equal(t, 1, pool.takeAll())
	s := &session{pool: pool, log: logger}
	s.reserve(1)
	job := newFixture("q").proto()
	badLeaseID := "not-a-uuid"
	job.LeaseId = &badLeaseID

	require.NoError(t, s.accept(job))
	line := workerLog(workerJSONLogs(t, buf), "discarding a malformed delivery")
	require.NotNil(t, line)
	require.Equal(t, badLeaseID, line["lease_id"])
}

func TestHandlerFinishedLogsHandlerTime(t *testing.T) {
	ctx := waitCtx(t)
	f := startFake(t)
	logger, buf := workerJSONLogger()
	r, stream := runHandler(t, ctx, f, Options{Logger: logger}, "q", func(context.Context, Job) error {
		time.Sleep(35 * time.Millisecond)
		return nil
	})

	require.NoError(t, stream.deliver(ctx, newFixture("q").proto()))
	awaitCalls(t, ctx, f, "Ack", 1)
	require.NoError(t, r.stopAndWait(t))
	line := workerLog(workerJSONLogs(t, buf), "handler finished")
	require.NotNil(t, line)
	handlerMS, ok := line["handler_ms"].(float64)
	require.True(t, ok)
	durationMS, ok := line["duration_ms"].(float64)
	require.True(t, ok)
	require.GreaterOrEqual(t, handlerMS, float64(30))
	require.LessOrEqual(t, handlerMS, durationMS)
}
