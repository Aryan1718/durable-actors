{{- define "actors.name" -}}
{{- printf "%s-durable-actors" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "actors.sandboxNamespace" -}}
{{- default (printf "%s-%s" .Release.Namespace .Release.Name | trunc 63 | trimSuffix "-") .Values.sandboxNamespace -}}
{{- end -}}
{{- define "actors.image" -}}
{{- printf "%s@sha256:%s" .repository (required "image digest is required for source builds; published charts include all image digests" .digest) -}}
{{- end -}}
{{- define "actors.validate" -}}
{{- if eq (include "actors.sandboxNamespace" .) .Release.Namespace }}{{ fail "sandbox namespace must differ from the control-plane namespace" }}{{ end -}}
{{- if and (gt (int .Values.replicaCount) 1) (not .Values.redis.urlSecretKey) }}{{ fail "replicaCount above 1 requires redis.urlSecretKey" }}{{ end -}}
{{- if .Values.ingress.enabled -}}
{{- if or (empty .Values.ingress.className) (empty .Values.ingress.tlsSecret) }}{{ fail "ingress requires className and tlsSecret" }}{{ end -}}
{{- end -}}
{{- end -}}
