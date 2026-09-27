{{/* Objects are named <release>-<component>. Truncated so the longest suffix,
     "-postgres" plus a StatefulSet's "-0", stays inside a 63-character label. */}}
{{- define "hopper.fullname" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end }}

{{- define "hopper.workerQueues" -}}
sleep fail_n_times cpu_burn
{{- end }}

{{- define "hopper.depthQuery" -}}
{{- $brokers := printf "namespace=%q, job=%q" .Release.Namespace (printf "%s-broker" (include "hopper.fullname" .)) -}}
{{- printf "sum(max by (queue) (hopper_queue_depth{%s, state=\"available\", queue=~%q} and on (job, instance) (time() - hopper_queue_depth_last_sample_timestamp_seconds{%s} < 15)))" $brokers (include "hopper.workerQueues" . | splitList " " | join "|") $brokers -}}
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
