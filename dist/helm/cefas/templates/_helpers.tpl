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
