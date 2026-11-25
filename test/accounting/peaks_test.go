//go:build peaks

package accounting

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPeaks judges saved worker stdout, the space-separated paths in
// HOPPER_PEAKS, with ConcurrencyProblems. No process may hold more deliveries
// open than the concurrency its "worker starting" line names, and each must
// have held at least HOPPER_PEAKS_WANT open at once. It is how Phase 3d shows a
// worker running 10,000 handlers at the same time (docs/bench/java.md):
//
//	HOPPER_PEAKS="$PWD/a.stdout $PWD/b.stdout" HOPPER_PEAKS_WANT=10000 \
//	  go test -tags peaks -count=1 -run TestPeaks -v ./test/accounting
func TestPeaks(t *testing.T) {
	paths := strings.Fields(os.Getenv("HOPPER_PEAKS"))
	if len(paths) == 0 {
		t.Skip("HOPPER_PEAKS names no worker log")
	}
	want, err := strconv.Atoi(os.Getenv("HOPPER_PEAKS_WANT"))
	require.NoError(t, err, "HOPPER_PEAKS_WANT")
	var logs []Log
	for _, p := range paths {
		f, err := os.Open(p)
		require.NoError(t, err)
		l, err := Parse(filepath.Base(p), f)
		require.NoError(t, f.Close())
		require.NoError(t, err)
		require.Zero(t, l.Unparsed, "%s holds lines that are not JSON log records", p)
		logs = append(logs, l)
	}
	peaks, problems := ConcurrencyProblems(logs)
	require.Empty(t, problems)
	require.NotEmpty(t, peaks)
	for _, pk := range peaks {
		t.Logf("%s process %d: %d deliveries open at once, of concurrency %d", pk.Pod, pk.Process, pk.Max, pk.Concurrency)
		require.GreaterOrEqual(t, pk.Max, want, "%s process %d", pk.Pod, pk.Process)
	}
}
