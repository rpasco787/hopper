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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/rpasco787/hopper/internal/scheduler"
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
	// postgres, broker, worker and the smoke test: a template that stops
	// rendering must not quietly shrink this check.
	require.Equal(t, 4, checked, "pod specs checked")
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

func TestBrokerConfig(t *testing.T) {
	objects := render(t)
	require.Equal(t, map[string]any{
		"HOPPER_DSN":              "postgres://hopper@hopper-postgres:5432/hopper?sslmode=disable",
		"HOPPER_LISTEN":           ":9090",
		"HOPPER_METRICS_LISTEN":   ":9091",
		"HOPPER_METRICS_QUEUES":   "sleep,fail_n_times,cpu_burn",
		"HOPPER_LEASE_DURATION":   "30s",
		"HOPPER_LOG_LEVEL":        "info",
		"HOPPER_SCHEDULER":        "true",
		"HOPPER_SHUTDOWN_TIMEOUT": "30s",
	}, dig(object(t, objects, "ConfigMap/hopper-broker"), "data"))

	deploy := object(t, objects, "Deployment/hopper-broker")
	require.Equal(t, 2, dig(deploy, "spec", "replicas"))
	require.Equal(t, "RollingUpdate", dig(deploy, "spec", "strategy", "type"))
	require.Equal(t, 1, dig(deploy, "spec", "strategy", "rollingUpdate", "maxSurge"))
	require.Equal(t, 0, dig(deploy, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
	broker := container(t, podSpec(t, deploy), "broker")
	require.Equal(t, "ghcr.io/rpasco787/hopper-broker:0.1.0", broker["image"])
	require.Equal(t, "IfNotPresent", broker["imagePullPolicy"])
	require.Equal(t, []any{map[string]any{"configMapRef": map[string]any{"name": "hopper-broker"}}}, broker["envFrom"])
}

func TestPGPasswordCompletesThePasswordlessDSN(t *testing.T) {
	objects := render(t)
	dsn := dig(object(t, objects, "ConfigMap/hopper-broker"), "data", "HOPPER_DSN").(string)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	_, hasPassword := u.User.Password()
	require.False(t, hasPassword, "the DSN in ConfigMap/hopper-broker carries a password")

	broker := container(t, podSpec(t, object(t, objects, "Deployment/hopper-broker")), "broker")
	require.Equal(t, map[string]any{"name": "hopper-postgres", "key": "password"},
		dig(env(broker)["PGPASSWORD"], "valueFrom", "secretKeyRef"))

	// Decision 6 depends on pgx filling a missing password from PGPASSWORD.
	t.Setenv("PGPASSWORD", "canary-7f3a")
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	require.Equal(t, "canary-7f3a", cfg.ConnConfig.Password)
	require.Equal(t, "hopper-postgres", cfg.ConnConfig.Host)
}

func TestBrokerProbesUseGRPCHealth(t *testing.T) {
	spec := podSpec(t, object(t, render(t), "Deployment/hopper-broker"))
	broker := container(t, spec, "broker")
	for probe, want := range map[string]map[string]any{
		"startupProbe":   {"periodSeconds": 2, "failureThreshold": 30},
		"readinessProbe": {"periodSeconds": 5},
		"livenessProbe":  {"periodSeconds": 10, "failureThreshold": 3},
	} {
		require.Equal(t, 9090, dig(broker, probe, "grpc", "port"), probe)
		for field, v := range want {
			require.Equal(t, v, dig(broker, probe, field), "%s.%s", probe, field)
		}
	}

	wait := container(t, spec, "wait-for-postgres")
	require.Equal(t, "postgres:16-alpine", wait["image"])
	require.Equal(t, 70, dig(wait, "securityContext", "runAsUser"))
	require.Equal(t, []any{map[string]any{"configMapRef": map[string]any{"name": "hopper-broker"}}}, wait["envFrom"])
	require.Contains(t, fmt.Sprint(wait["command"]), `pg_isready -d "$HOPPER_DSN"`)
}

// requireGraceHolds checks that a component's preStop sleep, its own shutdown
// budget and 5s for exit all fit inside its grace period.
func requireGraceHolds(t *testing.T, objects map[string]map[string]any, name string) {
	t.Helper()
	spec := podSpec(t, object(t, objects, "Deployment/hopper-"+name))
	grace := dig(spec, "terminationGracePeriodSeconds").(int)
	preStop := dig(container(t, spec, name), "lifecycle", "preStop", "sleep", "seconds").(int)
	budget, err := time.ParseDuration(dig(object(t, objects, "ConfigMap/hopper-"+name), "data", "HOPPER_SHUTDOWN_TIMEOUT").(string))
	require.NoError(t, err)
	// The grace countdown starts at deletion and includes the preStop hook.
	require.LessOrEqual(t, time.Duration(preStop)*time.Second+budget+5*time.Second, time.Duration(grace)*time.Second,
		"Deployment/hopper-%s", name)
}

func TestShutdownBudgetsFitTheirGracePeriods(t *testing.T) {
	objects := render(t)
	requireGraceHolds(t, objects, "broker")
	requireGraceHolds(t, objects, "worker")
}

func TestChartRejectsGracePeriodsTooShortForTheirDrain(t *testing.T) {
	_, stderr, err := helmTemplate("--set", "broker.terminationGracePeriodSeconds=39")
	require.Error(t, err)
	require.Contains(t, stderr, "broker: terminationGracePeriodSeconds (39) must be at least preStopSleepSeconds + shutdownTimeoutSeconds + 5 = 40")
	render(t, "--set", "broker.terminationGracePeriodSeconds=40")
	_, stderr, err = helmTemplate("--set", "broker.shutdownTimeoutSeconds=60")
	require.Error(t, err, "the guard must read the budget, not only the grace period")
	require.Contains(t, stderr, "broker: terminationGracePeriodSeconds (45)")

	_, stderr, err = helmTemplate("--set", "worker.terminationGracePeriodSeconds=54")
	require.Error(t, err)
	require.Contains(t, stderr, "worker: terminationGracePeriodSeconds (54) must be at least preStopSleepSeconds + shutdownTimeoutSeconds + 5 = 55")
	render(t, "--set", "worker.terminationGracePeriodSeconds=55")
	_, stderr, err = helmTemplate("--set", "worker.preStopSleepSeconds=20")
	require.Error(t, err, "the guard must read the preStop sleep")
	require.Contains(t, stderr, "worker: terminationGracePeriodSeconds (60)")
}

func checksum(t *testing.T, objects map[string]map[string]any, key string) string {
	t.Helper()
	sum, ok := dig(object(t, objects, key), "spec", "template", "metadata", "annotations", "checksum/config").(string)
	require.True(t, ok, "%s has no checksum/config annotation", key)
	return sum
}

func TestConfigChangesRollPods(t *testing.T) {
	base := render(t)
	debug := render(t, "--set", "broker.logLevel=debug")
	require.NotEqual(t, checksum(t, base, "Deployment/hopper-broker"), checksum(t, debug, "Deployment/hopper-broker"))
	require.Equal(t, checksum(t, base, "Deployment/hopper-worker"), checksum(t, debug, "Deployment/hopper-worker"),
		"a broker setting must not roll the workers")

	wider := render(t, "--set", "worker.concurrency=16")
	require.NotEqual(t, checksum(t, base, "Deployment/hopper-worker"), checksum(t, wider, "Deployment/hopper-worker"))
	require.Equal(t, checksum(t, base, "Deployment/hopper-broker"), checksum(t, wider, "Deployment/hopper-broker"),
		"a worker setting must not roll the brokers")
}

func TestBrokerServiceAndDisruptionBudget(t *testing.T) {
	objects := render(t)
	svc := object(t, objects, "Service/hopper-broker")
	require.Equal(t, "ClusterIP", dig(svc, "spec", "type"))
	require.ElementsMatch(t, []any{
		map[string]any{"name": "grpc", "port": 9090, "targetPort": "grpc"},
		map[string]any{"name": "metrics", "port": 9091, "targetPort": "metrics"},
	}, dig(svc, "spec", "ports"))
	require.Equal(t, dig(object(t, objects, "Deployment/hopper-broker"), "spec", "selector", "matchLabels"),
		dig(svc, "spec", "selector"))

	require.Equal(t, 1, dig(object(t, objects, "PodDisruptionBudget/hopper-broker"), "spec", "maxUnavailable"))
	require.NotContains(t, render(t, "--set", "broker.replicas=1"), "PodDisruptionBudget/hopper-broker",
		"a budget over one replica blocks every drain")
}

func TestCronSchedulesReachTheBroker(t *testing.T) {
	objects := render(t)
	require.NotContains(t, objects, "ConfigMap/hopper-broker-cron")
	require.NotContains(t, dig(object(t, objects, "ConfigMap/hopper-broker"), "data"), "HOPPER_CRON_FILE")
	spec := podSpec(t, object(t, objects, "Deployment/hopper-broker"))
	require.Nil(t, spec["volumes"])

	objects = render(t, "--set-json", `broker.cronSchedules=[`+
		`{"name":"tick","queue":"sleep","cron":"@every 10s","payload":{"ms":50}},`+
		`{"name":"ny","queue":"cpu_burn","cron":"*/5 * * * *","timezone":"America/New_York","max_attempts":3}]`)
	require.Equal(t, "/etc/hopper/schedules.json",
		dig(object(t, objects, "ConfigMap/hopper-broker"), "data", "HOPPER_CRON_FILE"))
	spec = podSpec(t, object(t, objects, "Deployment/hopper-broker"))
	require.Equal(t, []any{map[string]any{"name": "cron", "configMap": map[string]any{"name": "hopper-broker-cron"}}},
		spec["volumes"])
	require.Equal(t, []any{map[string]any{"name": "cron", "mountPath": "/etc/hopper", "readOnly": true}},
		container(t, spec, "broker")["volumeMounts"])

	// The chart's JSON must be the shape the broker parses.
	path := filepath.Join(t.TempDir(), "schedules.json")
	body := dig(object(t, objects, "ConfigMap/hopper-broker-cron"), "data", "schedules.json").(string)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	defs, err := scheduler.LoadFile(path)
	require.NoError(t, err)
	var names []string
	for _, d := range defs {
		names = append(names, d.Name)
	}
	require.ElementsMatch(t, []string{"tick", "ny"}, names)
}

func TestWorkerConfig(t *testing.T) {
	objects := render(t)
	require.Equal(t, map[string]any{
		"HOPPER_BROKER":           "hopper-broker:9090",
		"HOPPER_CONCURRENCY":      "8",
		"HOPPER_SHUTDOWN_TIMEOUT": "45s",
		"HOPPER_LOG_LEVEL":        "info",
		"HOPPER_METRICS_LISTEN":   ":9092",
	}, dig(object(t, objects, "ConfigMap/hopper-worker"), "data"))

	deploy := object(t, objects, "Deployment/hopper-worker")
	require.Equal(t, 1, dig(deploy, "spec", "replicas"))
	require.Equal(t, "RollingUpdate", dig(deploy, "spec", "strategy", "type"))
	require.Equal(t, 1, dig(deploy, "spec", "strategy", "rollingUpdate", "maxSurge"))
	require.Equal(t, 0, dig(deploy, "spec", "strategy", "rollingUpdate", "maxUnavailable"))
	spec := podSpec(t, deploy)
	require.Len(t, containers(spec), 1, "Deployment/hopper-worker has no init containers")
	worker := container(t, spec, "worker")
	require.Equal(t, "ghcr.io/rpasco787/hopper-worker:0.1.0", worker["image"])
	require.Equal(t, []any{map[string]any{"configMapRef": map[string]any{"name": "hopper-worker"}}}, worker["envFrom"])
	require.Equal(t, []any{map[string]any{"name": "metrics", "containerPort": 9092}}, worker["ports"])
	for _, probe := range []string{"startupProbe", "readinessProbe", "livenessProbe"} {
		require.Nil(t, worker[probe], "Deployment/hopper-worker: %s (Decision 5: workers have none)", probe)
	}
}

func TestImagesShareOneTag(t *testing.T) {
	images := func(objects map[string]map[string]any) map[string]string {
		out := map[string]string{}
		for _, key := range []string{"Deployment/hopper-broker", "Deployment/hopper-worker", "Pod/hopper-smoke", "StatefulSet/hopper-postgres"} {
			for _, c := range containers(podSpec(t, object(t, objects, key))) {
				out[fmt.Sprint(c["name"])] = fmt.Sprint(c["image"])
			}
		}
		return out
	}
	for tag, args := range map[string][]string{"0.1.0": nil, "abc": {"--set", "image.tag=abc"}} {
		got := images(render(t, args...))
		require.Equal(t, map[string]string{
			"broker":            "ghcr.io/rpasco787/hopper-broker:" + tag,
			"worker":            "ghcr.io/rpasco787/hopper-worker:" + tag,
			"smoke":             "ghcr.io/rpasco787/hopperctl:" + tag,
			"wait-for-postgres": "postgres:16-alpine",
			"postgres":          "postgres:16-alpine",
		}, got, "tag %s", tag)
	}
}

func TestSmokeTestHook(t *testing.T) {
	pod := object(t, render(t), "Pod/hopper-smoke")
	require.Equal(t, "test", dig(pod, "metadata", "annotations", "helm.sh/hook"))
	require.Equal(t, "before-hook-creation", dig(pod, "metadata", "annotations", "helm.sh/hook-delete-policy"))
	spec := podSpec(t, pod)
	require.Equal(t, "Never", spec["restartPolicy"])
	require.Equal(t, []any{
		"smoke", "-broker=hopper-broker:9090", "-queue=sleep", `-payload={"ms":10}`, "-timeout=60s",
	}, container(t, spec, "smoke")["args"])
}
