use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

use super::*;
use crate::request_traces::{InvalidTraceQuery, TraceRecord, metrics::ClassMetrics};

impl ClickHouseTracePersistence {
    pub(super) async fn read_history(
        &self,
        project: &str,
        query: &HistoryQuery,
    ) -> Result<TracePage> {
        let after = query
            .cursor
            .as_deref()
            .map(|value| Cursor::decode(value, project, query))
            .transpose()?;
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("history.sql"), project)
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .param("actor_id", query.actor_id.as_deref().unwrap_or(""))
                .param("request_id", query.request_id.as_deref().unwrap_or(""))
                .param(
                    "connection_id",
                    query.connection_id.as_deref().unwrap_or(""),
                )
                .param(
                    "outcome",
                    query.outcome.map(|value| value.as_str()).unwrap_or(""),
                )
                .param("from", query.from_ms.unwrap_or(0))
                .param("to", query.to_ms.unwrap_or(u64::MAX))
                .param("has_cursor", after.is_some())
                .param("after_time", after.as_ref().map_or(0, |value| value.time))
                .param(
                    "after_id",
                    after.as_ref().map_or("", |value| value.event_id.as_str()),
                )
                .param("limit", query.limit + 1)
                .fetch_all::<JsonEvent>(),
        )
        .await??;
        let more = rows.len() > query.limit;
        let records = rows
            .into_iter()
            .take(query.limit)
            .map(|row| {
                let event: TraceEvent = serde_json::from_str(&row.event_json)?;
                Ok(TraceRecord {
                    sequence: event.trace.started_at_ms,
                    event,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let filters = query.filter_key()?;
        let next_cursor = if more {
            records
                .last()
                .map(|last| {
                    Cursor {
                        project: project.into(),
                        filters,
                        time: last.event.trace.started_at_ms,
                        event_id: last.event.event_id.clone(),
                    }
                    .encode()
                })
                .transpose()?
        } else {
            None
        };
        Ok(TracePage {
            epoch: "clickhouse-v1".into(),
            cursor: records.first().map_or(0, |record| record.sequence),
            capacity: query.limit,
            evicted: 0,
            dropped: 0,
            persistence_failed: false,
            records,
            next_cursor,
            resume_cursor: "snapshot".into(),
            reset: false,
            snapshot: false,
        })
    }

    pub(super) async fn read_metrics(
        &self,
        project: &str,
        range: &TimeRange,
    ) -> Result<OverviewMetrics> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("overview.sql"), project)
                .param("from", range.from_ms.unwrap_or(0))
                .param("to", range.to_ms.unwrap_or(u64::MAX))
                .fetch_all::<OverviewRow>(),
        )
        .await??;
        let mut result = OverviewMetrics::default();
        for row in rows {
            let metric = ClassMetrics {
                actor_name: row.actor_name,
                count: row.count,
                success: (row.attempts > 0)
                    .then(|| 100.0 * row.completed as f64 / row.attempts as f64),
                p95: row.p95,
                queue_p95: row.queue_p95,
            };
            if row.is_total != 0 {
                result.total = metric;
            } else {
                result.classes.push(metric);
            }
        }
        Ok(result)
    }

    pub(super) async fn read_queue_waits(
        &self,
        project: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("queue_waits.sql"), project)
                .param("from", query.from_ms.unwrap_or(0))
                .param("to", query.to_ms.unwrap_or(u64::MAX))
                .param("actor_name", query.actor_name.as_deref().unwrap_or(""))
                .fetch_all::<QueueRow>(),
        )
        .await??;
        Ok(rows
            .into_iter()
            .map(|row| QueueWaitRow {
                actor_name: row.actor_name,
                actor_id: row.actor_id,
                admitted: row.admitted,
                average_ms: row.average_ms,
                max_ms: row.max_ms,
            })
            .collect())
    }

    pub(super) async fn read_websockets(
        &self,
        project: &str,
        range: &TimeRange,
    ) -> Result<Vec<SocketSession>> {
        let rows = tokio::time::timeout(
            Duration::from_secs(15),
            self.query(include_str!("websockets.sql"), project)
                .param("from", range.from_ms.unwrap_or(0))
                .param("to", range.to_ms.unwrap_or(u64::MAX))
                .fetch_all::<SocketRow>(),
        )
        .await??;
        rows.into_iter()
            .map(|row| {
                let metadata = if row.connect_event.is_empty() {
                    None
                } else {
                    serde_json::from_str::<TraceEvent>(&row.connect_event)?
                        .trace
                        .metadata
                };
                Ok(SocketSession {
                    connection_id: row.connection_id.unwrap_or_default(),
                    actor_name: row.actor_name,
                    actor_id: row.actor_id,
                    host_id: Some(row.host_id),
                    opened_at_ms: row.opened_at_ms,
                    closed_at_ms: row.closed_at_ms,
                    last_seen_ms: Some(row.last_seen_ms),
                    messages: row.messages,
                    failures: row.failures,
                    metadata,
                })
            })
            .collect()
    }

    fn query(&self, sql: &str, project: &str) -> clickhouse::query::Query {
        self.client
            .query(sql)
            .param("project", project)
            .param("retention", self.retention_seconds)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    project: String,
    filters: String,
    time: u64,
    event_id: String,
}

impl Cursor {
    fn decode(value: &str, project: &str, query: &HistoryQuery) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| InvalidTraceQuery)?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| InvalidTraceQuery)?;
        ensure!(
            cursor.project == project
                && cursor.filters == query.filter_key()?
                && !cursor.event_id.is_empty()
                && cursor.event_id.len() <= 256,
            InvalidTraceQuery
        );
        Ok(cursor)
    }

    fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }
}

#[derive(Row, Deserialize)]
struct JsonEvent {
    event_json: String,
}

#[derive(Row, Deserialize)]
struct OverviewRow {
    actor_name: String,
    count: u64,
    attempts: u64,
    completed: u64,
    p95: Option<f64>,
    queue_p95: Option<f64>,
    is_total: u8,
}

#[derive(Row, Deserialize)]
struct QueueRow {
    actor_name: String,
    actor_id: String,
    admitted: u64,
    average_ms: f64,
    max_ms: f64,
}

#[derive(Row, Deserialize)]
struct SocketRow {
    connection_id: Option<String>,
    actor_name: String,
    actor_id: String,
    host_id: String,
    opened_at_ms: Option<u64>,
    closed_at_ms: Option<u64>,
    last_seen_ms: u64,
    messages: u64,
    failures: u64,
    connect_event: String,
}
