{{- define "cefas.name" -}}
{{ .Chart.Name }}
{{- end -}}

{{- define "cefas.fullname" -}}
{{ .Release.Name }}-{{ include "cefas.name" . }}
{{- end -}}

{{- define "cefas.headless" -}}
{{ include "cefas.fullname" . }}-headless
{{- end -}}

{{- define "cefas.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ default (include "cefas.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{- define "cefas.labels" -}}
app.kubernetes.io/name: {{ include "cefas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "cefas.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cefas.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cefas.resolvedReplicaCount" -}}
{{- $replicas := int .Values.replicaCount -}}
{{- if .Values.resilience.enabled -}}
{{- $resilienceReplicas := int .Values.resilience.replicas -}}
{{- if gt $resilienceReplicas $replicas -}}
{{- $resilienceReplicas -}}
{{- else -}}
{{- $replicas -}}
{{- end -}}
{{- else -}}
{{- $replicas -}}
{{- end -}}
{{- end -}}

{{- define "cefas.resolvedReplicationFactor" -}}
{{- if gt (int .Values.cluster.replicationFactor) 0 -}}
{{- int .Values.cluster.replicationFactor -}}
{{- else if .Values.resilience.enabled -}}
{{- int .Values.resilience.replicationFactor -}}
{{- else -}}
0
{{- end -}}
{{- end -}}

{{- define "cefas.podDNS" -}}
{{- $root := index . 0 -}}
{{- $ordinal := index . 1 -}}
{{ include "cefas.fullname" $root }}-{{ $ordinal }}.{{ include "cefas.headless" $root }}.{{ $root.Release.Namespace }}.svc.cluster.local
{{- end -}}

{{- define "cefas.startupProbe" -}}
httpGet:
{{- toYaml .Values.startupProbe.httpGet | nindent 2 }}
{{- if .Values.resilience.enabled }}
failureThreshold: {{ max (int .Values.startupProbe.failureThreshold) (int .Values.resilience.startupProbe.failureThreshold) }}
periodSeconds: {{ max (int .Values.startupProbe.periodSeconds) (int .Values.resilience.startupProbe.periodSeconds) }}
{{- else }}
{{- omit .Values.startupProbe "httpGet" | toYaml | nindent 0 }}
{{- end }}
{{- end -}}

{{- define "cefas.dbResources" -}}
{{- $requests := .Values.resources.requests | default dict -}}
{{- $limits := .Values.resources.limits | default dict -}}
{{- if $requests }}
requests:
{{- toYaml $requests | nindent 2 }}
{{- end }}
{{- if or $limits.cpu $limits.memory }}
limits:
{{- if and $limits.cpu (not .Values.resourcePolicy.disableCPULimits) }}
  cpu: {{ $limits.cpu | quote }}
{{- end }}
{{- if $limits.memory }}
  memory: {{ $limits.memory | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "cefas.validate" -}}
{{- if .Values.resilience.enabled -}}
{{- $replicas := include "cefas.resolvedReplicaCount" . | int -}}
{{- $rf := include "cefas.resolvedReplicationFactor" . | int -}}
{{- if lt (int .Values.resilience.replicas) 3 -}}
{{- fail "resilience.replicas must be >= 3 for the RF=3 resilience profile" -}}
{{- end -}}
{{- if lt $replicas 3 -}}
{{- fail "resilience.enabled=true requires at least 3 rendered database replicas" -}}
{{- end -}}
{{- if lt $rf 3 -}}
{{- fail "resilience.enabled=true requires cluster.replicationFactor or resilience.replicationFactor >= 3" -}}
{{- end -}}
{{- if gt $rf $replicas -}}
{{- fail "replication factor cannot exceed rendered database replicas" -}}
{{- end -}}
{{- if and .Values.resilience.requirePersistentStorage (not .Values.persistence.enabled) (not .Values.resilience.allowEphemeralStorage) -}}
{{- fail "resilience.enabled=true requires persistence.enabled=true unless resilience.allowEphemeralStorage=true is explicitly set" -}}
{{- end -}}
{{- if and .Values.resourcePolicy.requireMemoryLimit (not .Values.resources.limits.memory) -}}
{{- fail "resilience.enabled=true requires resources.limits.memory to preserve memory safeguards" -}}
{{- end -}}
{{- if lt (int .Values.startupProbe.failureThreshold) 1 -}}
{{- fail "startupProbe.failureThreshold must be >= 1" -}}
{{- end -}}
{{- end -}}
{{- end -}}
