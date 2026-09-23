{{/* Objects are named <release>-<component>. Truncated so the longest suffix,
     "-postgres" plus a StatefulSet's "-0", stays inside a 63-character label. */}}
{{- define "hopper.fullname" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
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
