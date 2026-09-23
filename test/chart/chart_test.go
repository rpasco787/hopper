// Package chart checks the Helm chart's rendered output: the invariants that
// keep a Kubernetes deployment correct but that no test of the binaries can
// see, such as a grace period too short for the drain it has to contain. It
// needs Helm 4 on PATH, as test/integration needs Docker.
package chart

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

const chartDir = "../../deploy/helm/hopper"

// canonicalHelm is the Helm that renders the committed deploy/k8s/hopper.yaml.
// Any Helm 4 runs these tests, but another version may render the same chart
// differently, and the drift test then says so.
const canonicalHelm = "v4.3.0"

var helmVersion string // `helm version --short`, e.g. "v4.3.0+g…"

func TestMain(m *testing.M) {
	out, err := exec.Command("helm", "version", "--short").Output()
	helmVersion = strings.TrimSpace(string(out))
	if err != nil || !strings.HasPrefix(helmVersion, "v4.") {
		fmt.Fprintf(os.Stderr, "test/chart needs Helm 4 on PATH (brew install helm): %q %v\n", out, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// helmTemplate renders the chart as release "hopper" with extra flags.
func helmTemplate(args ...string) (stdout []byte, stderr string, err error) {
	cmd := exec.Command("helm", append([]string{"template", "hopper", chartDir, "--kube-version", "1.35.8"}, args...)...)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	stdout, err = cmd.Output()
	return stdout, errBuf.String(), err
}

// render returns every rendered object keyed "Kind/name".
func render(t *testing.T, args ...string) map[string]map[string]any {
	t.Helper()
	out, stderr, err := helmTemplate(args...)
	require.NoError(t, err, stderr)
	objects := map[string]map[string]any{}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var obj map[string]any
		if err := dec.Decode(&obj); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		if obj == nil {
			continue // an empty document between separators
		}
		key := fmt.Sprintf("%v/%v", obj["kind"], dig(obj, "metadata", "name"))
		require.NotContains(t, objects, key, "rendered twice")
		objects[key] = obj
	}
	return objects
}

// dig walks maps by string key and slices by int index, returning nil for a missing step.
func dig(v any, path ...any) any {
	for _, step := range path {
		switch k := step.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			s, ok := v.([]any)
			if !ok || k < 0 || k >= len(s) {
				return nil
			}
			v = s[k]
		default:
			panic(fmt.Sprintf("dig: path step %v is %T", step, step))
		}
	}
	return v
}

// podSpec returns the pod spec of a Pod, Deployment or StatefulSet.
func podSpec(t *testing.T, obj map[string]any) map[string]any {
	t.Helper()
	var spec any
	if obj["kind"] == "Pod" {
		spec = obj["spec"]
	} else {
		spec = dig(obj, "spec", "template", "spec")
	}
	m, ok := spec.(map[string]any)
	require.True(t, ok, "%v/%v has no pod spec", obj["kind"], dig(obj, "metadata", "name"))
	return m
}

// hasPodSpec reports whether obj is a kind that carries a pod spec.
func hasPodSpec(obj map[string]any) bool {
	switch obj["kind"] {
	case "Pod", "Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet":
		return true
	}
	return false
}

// containers returns initContainers followed by containers.
func containers(spec map[string]any) []map[string]any {
	var out []map[string]any
	for _, field := range []string{"initContainers", "containers"} {
		list, _ := spec[field].([]any)
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				out = append(out, m)
			}
		}
	}
	return out
}

// container returns the named container or fails.
func container(t *testing.T, spec map[string]any, name string) map[string]any {
	t.Helper()
	for _, c := range containers(spec) {
		if c["name"] == name {
			return c
		}
	}
	require.FailNow(t, "no container named "+name)
	return nil
}

// env returns a container's env entries by name (each the raw entry map).
func env(c map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	list, _ := c["env"].([]any)
	for _, e := range list {
		if m, ok := e.(map[string]any); ok {
			out[fmt.Sprint(m["name"])] = m
		}
	}
	return out
}

// object returns the rendered object key ("Kind/name") or fails.
func object(t *testing.T, objects map[string]map[string]any, key string) map[string]any {
	t.Helper()
	obj, ok := objects[key]
	require.True(t, ok, "%s was not rendered; have %v", key, keys(objects))
	return obj
}

func keys(objects map[string]map[string]any) []string {
	out := make([]string, 0, len(objects))
	for k := range objects {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func TestPostgresIsAStatefulSetWithItsOwnVolume(t *testing.T) {
	objects := render(t)
	sts := object(t, objects, "StatefulSet/hopper-postgres")
	require.Equal(t, "hopper-postgres", dig(sts, "spec", "serviceName"))
	require.Equal(t, 1, dig(sts, "spec", "replicas"))

	claim := dig(sts, "spec", "volumeClaimTemplates", 0)
	require.Equal(t, "1Gi", dig(claim, "spec", "resources", "requests", "storage"))
	require.Equal(t, []any{"ReadWriteOnce"}, dig(claim, "spec", "accessModes"))

	spec := podSpec(t, sts)
	pg := container(t, spec, "postgres")
	mounted := false
	for _, m := range pg["volumeMounts"].([]any) {
		if dig(m, "name") == dig(claim, "metadata", "name") && dig(m, "mountPath") == "/var/lib/postgresql/data" {
			mounted = true
		}
	}
	require.True(t, mounted, "StatefulSet/hopper-postgres: the claim is not mounted at /var/lib/postgresql/data")
	require.Equal(t, "/var/lib/postgresql/data/pgdata", env(pg)["PGDATA"]["value"])
	require.Equal(t, map[string]any{"name": "hopper-postgres", "key": "password"},
		dig(env(pg)["POSTGRES_PASSWORD"], "valueFrom", "secretKeyRef"))

	for _, field := range []string{"runAsUser", "runAsGroup", "fsGroup"} {
		require.Equal(t, 70, dig(spec, "securityContext", field), field)
	}
	require.Contains(t, dig(pg, "readinessProbe", "exec", "command", 0), "pg_isready")
	require.Nil(t, pg["livenessProbe"], "StatefulSet/hopper-postgres: a database gets no liveness probe")

	svc := object(t, objects, "Service/hopper-postgres")
	require.Equal(t, "None", dig(svc, "spec", "clusterIP"))
}

func TestPodsMeetTheRestrictedStandard(t *testing.T) {
	objects := render(t)
	checked := 0
	for key, obj := range objects {
		if !hasPodSpec(obj) {
			continue
		}
		checked++
		spec := podSpec(t, obj)
		require.Equal(t, false, spec["automountServiceAccountToken"], key)
		require.Equal(t, false, spec["enableServiceLinks"], key)
		require.Equal(t, true, dig(spec, "securityContext", "runAsNonRoot"), key)
		require.Equal(t, "RuntimeDefault", dig(spec, "securityContext", "seccompProfile", "type"), key)
		require.NotEqual(t, 0, dig(spec, "securityContext", "runAsUser"), key)
		cs := containers(spec)
		require.NotEmpty(t, cs, key)
		for _, c := range cs {
			where := fmt.Sprintf("%s container %v", key, c["name"])
			sc := c["securityContext"]
			require.Equal(t, false, dig(sc, "allowPrivilegeEscalation"), where)
			require.Equal(t, []any{"ALL"}, dig(sc, "capabilities", "drop"), where)
			require.NotEqual(t, true, dig(sc, "privileged"), where)
			require.NotEqual(t, 0, dig(sc, "runAsUser"), where)
			if c["name"] != "postgres" {
				require.Equal(t, true, dig(sc, "readOnlyRootFilesystem"), where)
			}
		}
	}
	require.NotZero(t, checked, "no pod specs rendered")
}

func TestSelectorsNeverIncludeChangingLabels(t *testing.T) {
	objects := render(t)
	want := []string{"app.kubernetes.io/component", "app.kubernetes.io/instance", "app.kubernetes.io/name"}
	workloadSelectors := map[string]any{} // component -> matchLabels
	for key, obj := range objects {
		if obj["kind"] != "Deployment" && obj["kind"] != "StatefulSet" {
			continue
		}
		sel, ok := dig(obj, "spec", "selector", "matchLabels").(map[string]any)
		require.True(t, ok, "%s has no matchLabels", key)
		var got []string
		for k := range sel {
			got = append(got, k)
		}
		slices.Sort(got)
		require.Equal(t, want, got, key)
		workloadSelectors[fmt.Sprint(sel["app.kubernetes.io/component"])] = sel
	}
	require.NotEmpty(t, workloadSelectors)
	for key, obj := range objects {
		if obj["kind"] != "Service" {
			continue
		}
		sel, ok := dig(obj, "spec", "selector").(map[string]any)
		require.True(t, ok, "%s has no selector", key)
		workload, ok := workloadSelectors[fmt.Sprint(sel["app.kubernetes.io/component"])]
		require.True(t, ok, "%s selects no workload", key)
		require.Equal(t, workload, sel, key)
	}
}

func TestPasswordLivesOnlyInTheSecret(t *testing.T) {
	const canary = "canary-7f3a"
	objects := render(t, "--set", "postgres.password="+canary)
	secret := object(t, objects, "Secret/hopper-postgres")
	require.Equal(t, canary, dig(secret, "stringData", "password"))
	for key, obj := range objects {
		if key == "Secret/hopper-postgres" {
			continue
		}
		out, err := yaml.Marshal(obj)
		require.NoError(t, err)
		require.NotContains(t, string(out), canary, "%s contains the PostgreSQL password", key)
	}
}
