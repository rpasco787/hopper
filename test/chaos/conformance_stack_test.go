//go:build conformance

package chaos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTheChaosSuiteStillRunsTheGoWorkerFromTheTree: with no worker image
// named, the stack's files and the images TestMain builds are exactly what
// they were before Phase 3c, so a chaos run is unchanged (D6).
func TestTheChaosSuiteStillRunsTheGoWorkerFromTheTree(t *testing.T) {
	require.Equal(t, []string{"-f", "../../docker-compose.yml", "-f", "compose.yaml"}, stackFiles(""))
	require.Equal(t, []string{"broker", "broker-standby", "worker"}, buildServices(""))
}

// TestANamedWorkerImageIsRunNotBuilt: a named image is merged over the stack
// as the worker service and never built; the Go worker is built beside it as
// worker-go, for the mixed fleet.
func TestANamedWorkerImageIsRunNotBuilt(t *testing.T) {
	const image = "hopper-example-worker:dev"
	require.Equal(t, []string{"-f", "../../docker-compose.yml", "-f", "compose.yaml", "-f", "worker-image.yaml"}, stackFiles(image))
	require.Equal(t, []string{"broker", "broker-standby", "worker-go"}, buildServices(image))
	require.NotContains(t, buildServices(image), "worker")
}
