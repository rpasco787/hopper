//go:build javalog

package accounting

// The Java worker's logs, judged by the same reconciliation as the Go
// worker's (Phase 3b; docs/design/java-worker-sdk.md, "Logs").
// scripts/java-smoke.sh sets the inputs; without them the tests skip.

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestJavaWorkerLog reconciles one hopper-example-worker container's whole
// log against the run's rows.
//
//	HOPPER_JAVALOG_LOG   the container's `docker logs` output
//	HOPPER_JAVALOG_ROWS  `psql -At -F,` rows: id,state,attempts,lease_id
//	HOPPER_JAVALOG_MODE  graceful: Problems (a SIGTERM caught handlers running);
//	                     undisturbed: AttemptProblems (nacked retries allowed)
func TestJavaWorkerLog(t *testing.T) {
	logPath, rowsPath := os.Getenv("HOPPER_JAVALOG_LOG"), os.Getenv("HOPPER_JAVALOG_ROWS")
	if logPath == "" || rowsPath == "" {
		t.Skip("set HOPPER_JAVALOG_LOG and HOPPER_JAVALOG_ROWS (scripts/java-smoke.sh)")
	}
	l := parseLogFile(t, logPath)
	require.Zero(t, l.Unparsed, "every line the Java worker printed must be a JSON record")
	rows := readRowsFile(t, rowsPath)
	require.NotEmpty(t, rows)

	rep := Reconcile(rows, []Log{l})
	t.Logf("jobs %d, completed %d, processes %d, redelivered %d, restarted %d, unstarted nacks %d, repeats %d, shutdowns %+v",
		rep.Jobs, rep.Completed, len(rep.Processes), len(rep.Extras), rep.Restarted, rep.UnstartedNacks,
		len(rep.Repeats), rep.Shutdowns)
	require.Equal(t, rep.Jobs, rep.Completed, "every job completed")
	switch mode := os.Getenv("HOPPER_JAVALOG_MODE"); mode {
	case "graceful":
		require.Empty(t, rep.Problems())
	case "undisturbed":
		require.Empty(t, rep.AttemptProblems())
		require.Empty(t, rep.Shutdowns, "nothing shut the worker down")
	default:
		t.Fatalf("HOPPER_JAVALOG_MODE %q: want graceful or undisturbed", mode)
	}
}

// TestJavaWorkerStopsACancelledJobAtItsFirstRenewal checks the cancellation
// case live: a job cancelled while its handler runs is stopped by the first
// renewal, G/3 after the handler started, and nothing is confirmed for it.
//
//	HOPPER_JAVALOG_LOG        the container's `docker logs` output
//	HOPPER_JAVALOG_CANCELLED  the job's id
//	HOPPER_JAVALOG_GRANT      its lease, as a Go duration (Compose: 30s)
func TestJavaWorkerStopsACancelledJobAtItsFirstRenewal(t *testing.T) {
	logPath, id := os.Getenv("HOPPER_JAVALOG_LOG"), os.Getenv("HOPPER_JAVALOG_CANCELLED")
	if logPath == "" || id == "" {
		t.Skip("set HOPPER_JAVALOG_LOG and HOPPER_JAVALOG_CANCELLED (scripts/java-smoke.sh)")
	}
	grant, err := time.ParseDuration(os.Getenv("HOPPER_JAVALOG_GRANT"))
	require.NoError(t, err)
	l := parseLogFile(t, logPath)

	var started, rejected time.Time
	var lease string
	for _, line := range l.Lines {
		if line.JobID != id {
			continue
		}
		switch line.Msg {
		case msgJobStarted:
			started, lease = line.Time, line.LeaseID
		case "heartbeat rejected; cancelling the handler":
			require.Equal(t, lease, line.LeaseID, "the rejection is about the lease that started")
			rejected = line.Time
		case msgJobConfirmed:
			t.Fatalf("lease %s was confirmed (%s) after its job was cancelled", line.LeaseID, line.Operation)
		}
	}
	require.False(t, started.IsZero(), "the handler never started")
	require.False(t, rejected.IsZero(), "no renewal was refused")
	after := rejected.Sub(started)
	t.Logf("stopped %s after the handler started (G/3 = %s)", after, grant/3)
	require.GreaterOrEqual(t, after, grant/3-time.Second)
	require.LessOrEqual(t, after, grant/3+2*time.Second)
}

func parseLogFile(t *testing.T, path string) Log {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	l, err := Parse("java-worker", f)
	require.NoError(t, err)
	return l
}

func readRowsFile(t *testing.T, path string) []Row {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var rows []Row
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		require.Len(t, parts, 4, "row %q", line)
		id, err := uuid.Parse(parts[0])
		require.NoError(t, err)
		attempts, err := strconv.Atoi(parts[2])
		require.NoError(t, err)
		lease := uuid.Nil
		if parts[3] != "" {
			lease, err = uuid.Parse(parts[3])
			require.NoError(t, err)
		}
		rows = append(rows, Row{ID: id, State: parts[1], Attempts: attempts, LeaseID: lease})
	}
	require.NoError(t, sc.Err())
	return rows
}
