{{/* Objects are named <release>-<component>. Truncated so the longest suffix,
     "-postgres-15" (the last shard's Service, and its StatefulSet pod), stays
     inside a 63-character label. */}}
{{- define "hopper.fullname" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end }}

{{- define "hopper.workerQueues" -}}
sleep fail_n_times cpu_burn
{{- end }}

{{- define "hopper.depthQuery" -}}
{{- $brokers := printf "namespace=%q, job=%q" .Release.Namespace (printf "%s-broker" (include "hopper.fullname" .)) -}}
{{- printf "sum(max by (queue, shard) (hopper_queue_depth{%s, state=\"available\", queue=~%q} and on (job, instance, shard) (time() - hopper_queue_depth_last_sample_timestamp_seconds{%s} < 15)))" $brokers (include "hopper.workerQueues" . | splitList " " | join "|") $brokers -}}
{{- end }}

{{- define "hopper.checkAutoscaling" -}}
{{- $a := .Values.worker.autoscaling -}}
{{- if not .Values.broker.serviceMonitor.enabled }}{{ fail "worker.autoscaling needs broker.serviceMonitor.enabled" }}{{- end -}}
{{- range include "hopper.workerQueues" . | splitList " " }}{{- if not (has . $.Values.broker.metricsQueues) }}{{ fail (printf "worker.autoscaling needs %q in broker.metricsQueues" .) }}{{- end }}{{- end -}}
{{- $min := $a.minReplicas | int -}}{{- $max := $a.maxReplicas | int -}}{{- $floor := $a.fallbackReplicas | int -}}
{{- if lt $min 1 }}{{ fail (printf "worker.autoscaling.minReplicas (%d) must be at least 1" $min) }}{{- end -}}
{{- if lt $max $min }}{{ fail (printf "worker.autoscaling.maxReplicas (%d) must be at least minReplicas (%d)" $max $min) }}{{- end -}}
{{- if or (lt $floor $min) (gt $floor $max) }}{{ fail (printf "worker.autoscaling.fallbackReplicas (%d) must be between minReplicas (%d) and maxReplicas (%d)" $floor $min $max) }}{{- end -}}
{{- if le ($a.availablePerWorker | float64) 0.0 }}{{ fail (printf "worker.autoscaling.availablePerWorker (%v) must be greater than 0" $a.availablePerWorker) }}{{- end -}}
{{- $window := $a.scaleDownStabilizationSeconds | int -}}{{- if or (lt $window 0) (gt $window 3600) }}{{ fail (printf "worker.autoscaling.scaleDownStabilizationSeconds (%d) must be between 0 and 3600" $window) }}{{- end -}}
{{- if not (regexMatch "^https?://[^/:\\s]+:[0-9]+$" (toString $a.prometheusAddress)) }}{{ fail (printf "worker.autoscaling.prometheusAddress (%q) must be http(s)://<host>:<port>" (toString $a.prometheusAddress)) }}{{- end -}}
{{- end }}

{{/* Selector labels never include a version. Deployment and StatefulSet
     selectors are immutable, so a label that changed per release would make
     every upgrade fail. Takes (dict "root" . "component" "broker"). */}}
{{- define "hopper.selectorLabels" -}}
app.kubernetes.io/name: hopper
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "hopper.labels" -}}
{{ include "hopper.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{/* Takes (dict "root" . "repository" .Values.broker.image). */}}
{{- define "hopper.image" -}}
{{ .repository }}:{{ .root.Values.image.tag | default .root.Chart.AppVersion }}
{{- end }}

{{/* The restricted Pod Security Standard, for pods whose images run as a
     numeric non-root user (Decision 8). */}}
{{- define "hopper.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "hopper.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end }}

{{/* Fails the render when a shutdown cannot finish inside its grace period.
     The countdown starts at deletion and includes preStop, so the preStop
     sleep, the process's shutdown budget and 5s for exit must all fit.
     Takes (dict "name" "worker" "v" .Values.worker). */}}
{{- define "hopper.checkGrace" -}}
{{- $need := add .v.preStopSleepSeconds .v.shutdownTimeoutSeconds 5 | int -}}
{{- $grace := .v.terminationGracePeriodSeconds | int -}}
{{- if lt $grace $need -}}
{{- fail (printf "%s: terminationGracePeriodSeconds (%d) must be at least preStopSleepSeconds + shutdownTimeoutSeconds + 5 = %d" .name $grace $need) -}}
{{- end -}}
{{- end }}

{{/* The host brokers connect to: the bundled StatefulSet's Service, or the
     CloudNativePG Cluster's read-write Service, which follows its primary. */}}
{{- define "hopper.databaseHost" -}}
{{- if eq .Values.postgres.mode "cloudnativepg" -}}
{{ include "hopper.fullname" . }}-db-rw
{{- else -}}
{{ include "hopper.fullname" . }}-postgres
{{- end -}}
{{- end }}

{{/* Fails the render on a database mode or replication that cannot keep
     its promise (docs/semantics.md §1), or a shard count out of range. */}}
{{- define "hopper.checkPostgres" -}}
{{- $mode := .Values.postgres.mode -}}
{{- if not (has $mode (list "bundled" "cloudnativepg")) }}{{ fail (printf "postgres.mode (%q) must be bundled or cloudnativepg" $mode) }}{{- end -}}
{{- $shards := .Values.postgres.shards | int -}}
{{- if or (lt $shards 1) (gt $shards 16) }}{{ fail (printf "postgres.shards (%v) must be between 1 and 16: every broker opens up to 16 connections to each shard, and more shards than that outgrow well-sized pools" .Values.postgres.shards) }}{{- end -}}
{{- if eq $mode "cloudnativepg" -}}
{{- $c := .Values.postgres.cloudnativepg -}}
{{- $n := $c.instances | int -}}{{- $s := $c.synchronousStandbys | int -}}
{{- if lt $n 2 }}{{ fail (printf "postgres.cloudnativepg.instances (%d) must be at least 2: one instance has nothing to fail over to" $n) }}{{- end -}}
{{- if lt $s 0 }}{{ fail (printf "postgres.cloudnativepg.synchronousStandbys (%d) must be 0 or more" $s) }}{{- end -}}
{{- if and (gt $s 0) (lt $n (add $s 2 | int)) }}{{ fail (printf "postgres.cloudnativepg.instances (%d) must be at least synchronousStandbys + 2 = %d, or after a failover every commit waits for the lost instance to return" $n (add $s 2 | int)) }}{{- end -}}
{{- end -}}
{{- end }}

{{/* The number of databases jobs are split across (postgres.shards). */}}
{{- define "hopper.shards" -}}
{{- .Values.postgres.shards | int -}}
{{- end }}

{{/* The host brokers reach shard i on when there are several: StatefulSet
     pod i's own Service, or Cluster i's read-write Service, which follows its
     primary. Takes (dict "root" . "i" 0). */}}
{{- define "hopper.shardHost" -}}
{{- if eq .root.Values.postgres.mode "cloudnativepg" -}}
{{ include "hopper.fullname" .root }}-db-{{ .i }}-rw
{{- else -}}
{{ include "hopper.fullname" .root }}-postgres-{{ .i }}
{{- end -}}
{{- end }}

{{/* A passwordless DSN for host: the broker reads the password from the
     Secret as PGPASSWORD. Takes (dict "root" . "host" "<host>"). */}}
{{- define "hopper.dsn" -}}
{{- printf "postgres://%s@%s:5432/%s?sslmode=disable" .root.Values.postgres.user .host .root.Values.postgres.database -}}
{{- end }}

{{/* HOPPER_SHARD_DSNS: every shard's DSN, space-separated, in shard order.
     The order is the shard index, which every job ID carries, so it never
     changes for a release. */}}
{{- define "hopper.shardDSNs" -}}
{{- $dsns := list -}}
{{- range $i := until (include "hopper.shards" . | int) -}}
{{- $dsns = append $dsns (include "hopper.dsn" (dict "root" $ "host" (include "hopper.shardHost" (dict "root" $ "i" $i)))) -}}
{{- end -}}
{{- join " " $dsns -}}
{{- end }}
