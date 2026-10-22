# Environment variables

Configuration uses environment variables. Precedence is to use exported variables followed by reading a .env in the folder durable-actors executes in. There is also support for specifying an env file using the `--env-file` switch.

## Connecting to the control plane

| Variable                           | Default                                 | Description                                                                                                                 |
| ---------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT_ID`        | `local` on localhost; required remotely | Actor project ID; use the same value in the server registration and backend.                                                |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | `http://127.0.0.1:7100`                 | HTTP(S) server origin. Paths, query strings, fragments, and embedded credentials are not allowed.                           |
| `DURABLE_ACTORS_SECRET`            | Unset                                   | Optional shared secret. Set the same value on the server and backend to enable authentication. Keep it out of browser code. |

## Diagnostics and runtime overrides

| Variable                   | Default                   | Description                                                                                      |
| -------------------------- | ------------------------- | ------------------------------------------------------------------------------------------------ |
| `RUST_LOG`                 | `info`                    | Runtime log filter, such as `warn` or `debug`; the packaged container supplies its own filter.   |
| `DURABLE_ACTORS_TELEMETRY` | Disabled                  | Set to `1` to enable SDK invocation telemetry on standard error; unset or `0` keeps it disabled. |
| `DURABLE_ACTORS_BINARY`    | Downloaded runtime        | Use an existing native executable. Relative paths resolve from the working directory.            |
| `DURABLE_ACTORS_CACHE_DIR` | `~/.cache/durable-actors` | Runtime download cache; ignored when `DURABLE_ACTORS_BINARY` is set.                             |

## Local development

| Variable                    | Default                            | Description                                                                                                           |
| --------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_PROJECT`    | `.`                                | Actor project directory.                                                                                              |
| `DURABLE_ACTORS_ENTRYPOINT` | Auto-detected | Actor source file, relative to the project. Discovers `actors.ts` or `actors.py` in `src/` or the project root; multiple matches require an explicit setting. |
| `DURABLE_ACTORS_PORT`       | `7100`                             | Listening port; `0` selects a free port. `--port` overrides it for one run.                                           |
| `DURABLE_ACTORS_DATA_DIR`   | `<project>/.durable-actors`        | Persistent local state directory.                                                                                     |
| `DURABLE_ACTORS_STORAGE`    | `local`                            | `local` for file storage or `gcs` for a GCS bucket. GCS also requires `DURABLE_ACTORS_BUCKET` and Google credentials. |

## WebSockets

| Variable                                | Default | Description                                                                                                                          |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS` | `32768` | Per-actor connection limit on each process; 1–32,768, including pending connect handlers. Actual capacity depends on workload and gateway resources. |

## Capacity and placement

| Variable                                | Default | Description                                                                                                                                                                                                                                                                                 |
| --------------------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS`   | `10000` | Actor idle time before eviction; 1–86400000 ms. Applies locally too. Method calls and WebSocket handlers reset the timer; running handlers defer eviction. Open WebSockets remain at the gateway and do not keep the sandbox alive. Automatic gateway replies do not reset actor idle time. |
| `DURABLE_ACTORS_HOST_STARTUP_MS`        | `30000` | Executor startup timeout in milliseconds. Actor hosts set it to three times the Rapid operation timeout.                                                                                                                                                                                    |
| `DURABLE_ACTORS_SPARE_IDLE`             | `64`    | Ready actor sandboxes per image and configured compute region; must not exceed the fleet budget. Zero creates hosts on demand. Control-plane replicas must share pool settings. Customer secrets are installed at assignment.                                                               |
| `DURABLE_ACTORS_SPARE_FLEET_MAX`        | `256`   | Maximum unassigned spares across pools. Active actors do not count against this budget.                                                                                                                                                                                                     |
| `DURABLE_ACTORS_SPARE_MAX_STARTING`     | `32`    | Maximum simultaneous spare starts across control-plane replicas.                                                                                                                                                                                                                            |
| `DURABLE_ACTORS_SPARE_TTL_SECONDS`      | `600`   | Unassigned host lifetime; 30–3600 seconds.                                                                                                                                                                                                                                                  |
| `DURABLE_ACTORS_HOST_CPU_MILLIS`        | `250`   | Default actor CPU request in millicores; spares use it.                                                                                                                                                                                                                                     |
| `DURABLE_ACTORS_HOST_CPU_LIMIT_MILLIS`  | Unset   | Default actor CPU limit; at least the request. Unset leaves CPU uncapped.                                                                                                                                                                                                                   |
| `DURABLE_ACTORS_HOST_MEMORY_MIB`        | `256`   | Default actor memory request in MiB; spares use it.                                                                                                                                                                                                                                         |
| `DURABLE_ACTORS_HOST_MEMORY_LIMIT_MIB`  | Request | Default actor memory limit; at least the request.                                                                                                                                                                                                                                           |
| `DURABLE_ACTORS_SANDBOX_MIN_CPU_MILLIS` | `125`   | Smallest `@Compute` CPU request or limit a deployment may set, in millicores.                                                                                                                                                                                                               |
| `DURABLE_ACTORS_SANDBOX_MAX_CPU_MILLIS` | `4000`  | Largest `@Compute` CPU request or limit a deployment may set, in millicores.                                                                                                                                                                                                                |
| `DURABLE_ACTORS_SANDBOX_MIN_MEMORY_MIB` | `256`   | Smallest `@Compute` memory request or limit a deployment may set, in MiB.                                                                                                                                                                                                                   |
| `DURABLE_ACTORS_SANDBOX_MAX_MEMORY_MIB` | `4096`  | Largest `@Compute` memory request or limit a deployment may set, in MiB.                                                                                                                                                                                                                    |
| `DURABLE_ACTORS_REGION`                 | Unset   | Default region for new actors. Without a decorator region override, explicit assignments must match it; existing actors keep their saved home.                                                                                                                                              |
| `DURABLE_ACTORS_HOME_REGION`            | Unset   | Region requested by a trusted backend. Omit to use the actor's saved home or the server default.                                                                                                                                                                                            |

## Authentication and callbacks

| Variable                                | Default                        | Description                                                                                                                                    |
| --------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_ACTORS_JWT_KEY_ID`             | `primary`                      | Signing key identifier.                                                                                                                        |
| `DURABLE_ACTORS_JWT_ISSUER`             | `durable-actors-control-plane` | Token issuer.                                                                                                                                  |
| `DURABLE_ACTORS_AUTHORITY_JWT_AUDIENCE` | `durable-actors-authority`     | Server authentication audience.                                                                                                                |
| `DURABLE_ACTORS_INVOKE_JWT_AUDIENCE`    | `durable-actors-invoke`        | Actor-call audience.                                                                                                                           |
| `DURABLE_ACTORS_JWT_MAX_TTL_SECONDS`    | `86400`                        | Positive maximum credential lifetime in seconds.                                                                                               |
| `DURABLE_ACTORS_SOCKET_EVENT_URL`       | Disabled                       | Incoming-message callback URL. Callbacks send the shared secret when configured and omit authorization otherwise; see [OpenAPI](openapi.yaml). |

## Kubernetes hosting

Use the [self-hosting guide](../self-hosting.md) and [Helm chart](../../charts/durable-actors/README.md) to run on GKE Sandbox with PostgreSQL and one Standard GCS bucket. One controller handles HTTP and WebSockets, and actor pods start on demand. The chart sets `DURABLE_ACTORS_SPARE_IDLE=0`; the runtime's standalone default above remains 64.

| Variable | Description |
| --- | --- |
| `DURABLE_ACTORS_PROCESS_ROLE` | `control_plane` for the server; the provider assigns actor/spare roles. |
| `DURABLE_ACTORS_CONTROL_PLANE_BIND` | Listen address, `0.0.0.0:7100` in the chart. |
| `DURABLE_ACTORS_ALARM_POLL_INTERVAL_MS` | Interval for checking due alarms, default `100` milliseconds. The chart sets it from `alarms.pollIntervalMs`. Shorter intervals reduce polling delay and increase database load. |
| `DURABLE_ACTORS_CONTROL_PLANE_URL` | Private Kubernetes Service origin reachable from sandboxes. |
| `DURABLE_ACTORS_GATEWAY_ACCEPT_CONNECTIONS` | Whether this process accepts browser WebSockets; defaults to `true`. |
| `DURABLE_ACTORS_PUBLIC_URL` | Public HTTPS origin for client invocation and socket routing. |
| `DURABLE_ACTORS_POSTGRES_URL` | Registry, trace, and spare bookkeeping database; the user needs migration privileges. |
| `DURABLE_ACTORS_ANALYTICS_BACKEND` | Request telemetry backend: `postgres` (default) or `clickhouse`. PostgreSQL remains required for control-plane bookkeeping. |
| `DURABLE_ACTORS_CLICKHOUSE_URL` | ClickHouse HTTP(S) endpoint, required for the ClickHouse backend. |
| `DURABLE_ACTORS_CLICKHOUSE_DATABASE` | Existing ClickHouse database. The application user needs SELECT, INSERT, CREATE TABLE, and ALTER TABLE privileges in it. |
| `DURABLE_ACTORS_CLICKHOUSE_USER` | ClickHouse application user. |
| `DURABLE_ACTORS_CLICKHOUSE_PASSWORD` | ClickHouse application password. Supply through a secret. |
| `DURABLE_ACTORS_POSTGRES_MAX_CONNECTIONS` | PostgreSQL pool size per process, default `8`, range 1–1,000. The chart sets it from `postgres.maxConnections`. Invocation routing, trace writes, and alarm polling share the pool. Budget the database's connection limit for this pool size times the controller replica count, plus other database clients. |
| `DURABLE_ACTORS_REDIS_URL` | Redis that carries socket messages between processes; required with more than one. `redis://host:6379`, or `rediss://:password@host:6378` for TLS. If the connection drops, the process closes its sockets with `1012` and browsers reconnect. Unset, a single process keeps everything in memory. The chart reads it from the key named by `redis.urlSecretKey`. |
| `DURABLE_ACTORS_REDIS_CA_CERT` | PEM CA for a `rediss://` server with a private CA, such as Memorystore. The chart reads it from the key named by `redis.caSecretKey`. |
| `DURABLE_ACTORS_PERSISTENCE` | `standard` by default. Select `rapid` for Rapid append logs with Standard archives. |
| `DURABLE_ACTORS_BUCKET` | Standard GCS bucket for ownership and, in Standard mode, immutable state snapshots. |
| `DURABLE_ACTORS_ARTIFACT_BUCKET` | Immutable compiled actor code. Defaults to `DURABLE_ACTORS_BUCKET` in Standard mode; the chart uses that bucket in both modes. |
| `DURABLE_ACTORS_ARCHIVE_BUCKET` | Required only in Rapid mode: permanent Standard GCS manifests, checkpoints, and archived log segments. The chart uses `storage.bucket`. |
| `DURABLE_ACTORS_RAPID_BUCKETS` | Required only in Rapid mode: JSON array of one or two `{ "bucket": "name", "zone": "us-west4-a" }` placements in distinct zones. Every configured zone must durably flush before acknowledgment. One zone has no second Rapid copy if that zone is lost before archival. |
| `DURABLE_ACTORS_ARCHIVE_BATCH_BYTES` | Rapid archive batch target, default `16777216` bytes. |
| `DURABLE_ACTORS_ARCHIVE_BATCH_INTERVAL_MS` | Rapid archive batch interval, default `10000` milliseconds. |
| `DURABLE_ACTORS_GKE_NAMESPACE` | Dedicated sandbox namespace; the chart defaults to `<release-namespace>-<release-name>`. |
| `DURABLE_ACTORS_GKE_ZONE` | Actor placement zone, for example `us-west4-a`; the runtime infers the canonical compute region. The chart sets this from `placement.zone`. |
| `DURABLE_ACTORS_GKE_ZONES` | Advanced alternative to `GKE_ZONE`: JSON map from canonical compute region to eligible Google zones, such as `{"north-america-west":["us-west4-a","us-west4-b"]}`. A single zone string per region is also accepted. Configure only one of the two placement variables. |
| `DURABLE_ACTORS_TYPESCRIPT_IMAGE` | TypeScript actor OCI image pinned by SHA-256 digest; selected for compiled `.mjs` artifacts. |
| `DURABLE_ACTORS_PYTHON_IMAGE` | Python actor OCI image pinned by SHA-256 digest; selected for compiled `.pyz` artifacts. |
| `DURABLE_ACTORS_EXECUTOR_RUNTIME` | Set by actor images to `typescript` or `python`; selects the executor before a warm spare is assigned. Native hosts infer it from the compiled entrypoint. |
| `DURABLE_ACTORS_JWT_SIGNING_KEY` | Shared base64 Ed25519 PKCS#8 key; stable across restarts. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Optional ADC file; use Workload Identity on GKE. |

Standard mode rejects Rapid bucket and archive settings. Storage mode and bucket identities are fixed for existing actor ownership records; changing them requires a planned data migration. See [Rapid configuration](../self-hosting.md#speed-up-writes-with-gcs-rapid) for bucket creation, IAM, and chart values.

## Modal data plane

Set `DURABLE_ACTORS_SANDBOX_PROVIDER=modal` to run hosts on Modal. The default is `gke`.
Both providers use the same PostgreSQL spare pool, deployment API, actor runtime, and GCS storage.

| Variable | Default | Description |
| --- | --- | --- |
| `DURABLE_ACTORS_SANDBOX_PROVIDER` | `gke` | `gke` or `modal`. |
| `DURABLE_ACTORS_MODAL_APP` | Required for Modal | Dedicated Modal App for this control-plane installation. |
| `DURABLE_ACTORS_MODAL_REGIONS` | Required for Modal | JSON mapping of canonical actor regions to Modal regions, e.g. `{"north-america-west":"us-west"}`. |
| `DURABLE_ACTORS_MODAL_TIMEOUT_SECONDS` | `3600` | Maximum sandbox lifetime, 120–86400 seconds, including time spent as a spare. |

Modal reads the active `~/.modal.toml` profile or `MODAL_TOKEN_ID` / `MODAL_TOKEN_SECRET`.
Its control-plane URL must be an HTTPS origin that carries gRPC. Proxies must preserve HTTP/2
and gRPC response trailers on `/durable_actors.v1.ActorControlPlaneService/*`. GKE zone settings
are unnecessary in Modal mode. CPU requests and limits are passed as Modal physical cores
(`cpu_millis / 1000`); memory requests and limits retain their MiB values.

Sandboxes run on GCP. Use a dedicated Modal App and PostgreSQL spare registry for each
installation; sharing a registry between providers is unsupported. Runtime images must be
accessible without private registry credentials.

Deployment `secretRefs` use the same Kubernetes secrets as GKE. The control plane reads them
from `DURABLE_ACTORS_GKE_NAMESPACE` (default `terse-sandboxes`) at assignment and requires
Kubernetes credentials with permission to get those secrets. Secrets must carry the
`terse.ai/customer-secret=true` label; duplicate environment keys and non-UTF-8 values are rejected.
Deployments without secret references do not require Kubernetes access in Modal mode.
