CREATE TABLE IF NOT EXISTS actor_traces (
    project_id String,
    event_id String,
    started_at_ms UInt64,
    actor_name LowCardinality(String),
    actor_id String,
    request_id String,
    connection_id Nullable(String),
    kind LowCardinality(String),
    operation LowCardinality(String),
    outcome LowCardinality(String),
    duration_ms Float64,
    queue_wait_ms Nullable(Float64),
    host_id String,
    event_json String,
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, started_at_ms, event_id)
PRIMARY KEY (project_id, started_at_ms)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE TABLE IF NOT EXISTS actor_trace_status (
    project_id String,
    event_id String,
    dropped UInt64,
    failed UInt8,
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, event_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

ALTER TABLE actor_traces MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE actor_trace_status MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
