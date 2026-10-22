# Durable Actors on GCP

Run Durable Actors on GKE Sandbox with one Standard GCS bucket and PostgreSQL. Standard GCS is the default persistence mode. Enable GCS Rapid when you need faster state writes.

Follow the [self-hosting guide](../../docs/self-hosting.md) for cluster prerequisites, IAM, credentials, installation, and Rapid configuration.

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west4-a
storage:
  bucket: my-actor-state
serviceAccount:
  googleServiceAccount: actors@my-project.iam.gserviceaccount.com
ingress:
  enabled: true
  className: my-ingress
  tlsSecret: actors-tls
```

Use your installed ingress controller's class. The controller must support WebSockets; configure its connection timeouts through `ingress.annotations`. With ingress disabled, route your HTTPS endpoint to the chart's ClusterIP Service on port 7100.

Published chart packages pin three matching image digests: `images.controlPlane.digest`, `images.typescript.digest`, and `images.python.digest`. For source builds, supply all three from the same source revision, without the `sha256:` prefix. Each image also has its own `repository` setting.

The Dockerfile has `control-plane`, `typescript`, and `python` targets. The default target contains the Rust control plane. The TypeScript image adds Bun and the execution SDK; the Python image adds Python and its SDK. Releases publish `<version>-control-plane`, `<version>-typescript`, and `<version>-python` tags, each supporting amd64 and arm64. The controller selects the actor image from the compiled artifact and keeps warm pools separate by image.

When upgrading from the shared image configuration, replace `image.repository` and `image.digest` with the three `images` entries. Direct control-plane deployments must replace `DURABLE_ACTORS_RUNTIME_IMAGE` with `DURABLE_ACTORS_TYPESCRIPT_IMAGE` and `DURABLE_ACTORS_PYTHON_IMAGE`. Redeploy existing actor bundles to replace their stored image references.

## Defaults

- One controller handles API requests and WebSockets.
- Actor pods start on demand: `actors.warm: 0`.
- The controller checks for due alarms every 100 milliseconds; configure `alarms.pollIntervalMs` to change the cadence.
- Each actor requests and is limited to 500 millicores and 256 MiB unless its deployment overrides resources.
- The controller requests 250 millicores and 512 MiB, with a 1 GiB memory limit. Measure and adjust `resources` for your workload.
- The sandbox namespace defaults to `<release-namespace>-<release-name>`; use one unique sandbox namespace per installation.
- gVisor, separate sandbox credentials, RBAC, and sandbox NetworkPolicy are included.

A single controller can interrupt service during replacement; clients must reconnect WebSockets. Increase `replicaCount` for controller redundancy; more than one controller requires Redis (`redis.urlSecretKey`). Increase `actors.warm` to reduce cold starts. The warm count is per image/region and supports 0–256. It does not cap active actors. Node autoscaling and quotas must accommodate your active workload.

## Rapid writes

Add one or two precreated Rapid buckets in distinct supported zones. The Standard bucket continues to store ownership, code, and permanent archives.

```yaml
storage:
  bucket: my-actor-state
  mode: rapid
  rapid:
    buckets:
      - {bucket: my-actor-rapid-a, zone: us-west4-a}
      - {bucket: my-actor-rapid-b, zone: us-west4-b}
```

Every configured copy must durably flush before a Rapid write is acknowledged. With one zone, there is no second Rapid copy if that zone is lost before archival. Mode and bucket identities are fixed for existing actor ownership records; choose Rapid before creating actors in that installation. An existing installation needs a planned data migration, not a values-only mode change.

## Advanced settings

`credentialsSecret` defaults to `actors-credentials`; it must contain `postgres-url`, `api-key`, and `jwt-signing-key`. For Redis, add the keys named by `redis.urlSecretKey` and, optionally, `redis.caSecretKey`. `nodeSelector` places controllers on ordinary nodes. `networkPolicy.dnsCidrs` permits specific host-networked or link-local DNS endpoints, for example a NodeLocal DNS `/32`. Add only the address your cluster uses.

See [values.yaml](values.yaml) for the full values reference and [values.schema.json](values.schema.json) for validation. No database, bucket, ingress controller, DNS record, or certificate is provisioned by this chart.
