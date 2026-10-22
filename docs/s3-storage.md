# S3 state storage

S3 support is under development. Standard S3 state storage is wired for local validation, but it is not ready for production deployment. S3 Express remains disabled until its fencing protocol passes the real-AWS test gate.

## Standard mode

Configure the control plane with:

```text
DURABLE_ACTORS_STORAGE_PROVIDER=s3
DURABLE_ACTORS_PERSISTENCE=standard
DURABLE_ACTORS_BUCKET=actor-state-bucket
DURABLE_ACTORS_AWS_REGION=us-west-2
```

The state bucket stores actor ownership records and immutable state snapshots. The adapter uses conditional S3 writes, CRC32C validation, paginated listings, exact range reads, and conditional multipart completion. Existing actors record their snapshot-store configuration. Persisting AWS provider and bucket identity still needs implementation; changing provider or mode requires an explicit state migration.

Compiled actor artifacts still use the existing GCS artifact bucket and scoped GCS token path. Selecting S3 changes state storage only.

The current implementation loads AWS credentials through the SDK default credential chain. Before production enablement, replace ambient host access with issued, refreshed credentials restricted to one actor's ownership object and snapshot prefix, then prove cross-actor denial in live AWS tests. Credentials and state payloads must not be logged.

## Express mode and fallback

Express mode is not configurable yet. Its proposed design writes every acknowledged change to two directory buckets in distinct Availability Zones and archives the history to Standard S3. If both Express replicas cannot be opened before the first write, that activation may use synchronous Standard writes. An uncertain active append must poison the activation and be recovered before another activation starts; it must never be retried directly against Standard.

Express must remain disabled until AWS testing proves that conditional atomic rename fences concurrent append requests.

## Release checks

- Restore acknowledged Standard state from a fresh runtime after host termination.
- Verify conditional-write reconciliation, multipart publication, ranges, pagination, and checksums against AWS.
- Issue and refresh actor-scoped credentials and deny access to another actor's keys.
- Run the two-zone Express race and failure matrix before exposing Express configuration.
- Run the full Rust suite and existing GCS integration checks after shared storage changes.
