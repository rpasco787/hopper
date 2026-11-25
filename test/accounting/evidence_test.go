//go:build evidence

package accounting

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEvidence judges one conformance scenario's saved evidence again,
// offline: HOPPER_EVIDENCE names a directory a live scenario wrote
// (test/chaos, tag conformance). It is how each check is shown able to fail:
// damage a copy, judge it (docs/design/conformance.md).
func TestEvidence(t *testing.T) {
	dir := os.Getenv("HOPPER_EVIDENCE")
	if dir == "" {
		t.Skip("set HOPPER_EVIDENCE to a scenario's evidence directory")
	}
	e, ev, err := ReadEvidence(dir)
	require.NoError(t, err)
	p := Judge(e, ev)
	for _, s := range p {
		t.Log(s)
	}
	require.Empty(t, p, "%s: %s", e.Scenario, dir)
}
