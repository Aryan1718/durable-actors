use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use moka::future::Cache;
use tokio::sync::Mutex;

use super::{LifecycleEvent, LifecycleKind, STALE_AFTER_MS};
use crate::redis_client::Redis;

const RETENTION_MS: u64 = 86_400_000;
const BATCH_SIZE: usize = 128;

#[async_trait]
pub(crate) trait UsageSessions: Send + Sync {
    /// Returns true when this report corrects an estimated stop.
    async fn observe(&self, event: &LifecycleEvent) -> Result<bool>;
    async fn expire(&self, now: u64) -> Result<Vec<LifecycleEvent>>;
}

pub(crate) struct MemoryUsageSessions(Mutex<Cache<String, Session>>);

impl Default for MemoryUsageSessions {
    fn default() -> Self {
        Self(Mutex::new(
            Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_millis(RETENTION_MS))
                .build(),
        ))
    }
}

#[async_trait]
impl UsageSessions for MemoryUsageSessions {
    async fn observe(&self, event: &LifecycleEvent) -> Result<bool> {
        let sessions = self.0.lock().await;
        let key = session_key(event);
        let previous = sessions.get(&key).await;
        if previous.as_ref().is_some_and(|previous| {
            event.report.sequence <= previous.event.report.sequence
                || previous.event.report.kind == LifecycleKind::Stopped
        }) {
            return Ok(false);
        }
        let corrected = previous.is_some_and(|previous| previous.estimated);
        sessions
            .insert(
                key,
                Session {
                    event: event.clone(),
                    estimated: false,
                },
            )
            .await;
        Ok(corrected)
    }

    async fn expire(&self, now: u64) -> Result<Vec<LifecycleEvent>> {
        let sessions = self.0.lock().await;
        let mut expired = Vec::new();
        for (key, mut session) in sessions.iter() {
            if !session.estimated
                && session.event.report.kind != LifecycleKind::Stopped
                && now.saturating_sub(session.event.received_at_ms) >= STALE_AFTER_MS
            {
                session.estimated = true;
                expired.push(session.event.clone());
                sessions.insert((*key).clone(), session).await;
                if expired.len() == BATCH_SIZE {
                    break;
                }
            }
        }
        Ok(expired)
    }
}

#[derive(Clone)]
struct Session {
    event: LifecycleEvent,
    estimated: bool,
}

pub(crate) struct RedisUsageSessions {
    redis: Redis,
    prefix: String,
}

impl RedisUsageSessions {
    pub(crate) fn new(redis: Redis, namespace: &str) -> Self {
        Self {
            redis,
            prefix: format!("{namespace}:"),
        }
    }
}

#[async_trait]
impl UsageSessions for RedisUsageSessions {
    async fn observe(&self, event: &LifecycleEvent) -> Result<bool> {
        let key = session_key(event);
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("observe_usage.lua"))
            .arg(2)
            .arg(format!("{}{key}", self.prefix))
            .arg(format!("{}due", self.prefix))
            .arg(serde_json::to_string(event)?)
            .arg(&key)
            .arg(STALE_AFTER_MS)
            .arg(RETENTION_MS);
        self.redis.run(command).await
    }

    async fn expire(&self, now: u64) -> Result<Vec<LifecycleEvent>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("expire_usage.lua"))
            .arg(1)
            .arg(format!("{}due", self.prefix))
            .arg(&self.prefix)
            .arg(now)
            .arg(RETENTION_MS)
            .arg(BATCH_SIZE);
        let events: Vec<String> = self.redis.run(command).await?;
        events
            .into_iter()
            .map(|event| Ok(serde_json::from_str(&event)?))
            .collect()
    }
}

fn session_key(event: &LifecycleEvent) -> String {
    super::routing::session_key(
        &event.actor.storage_key(),
        &event.host_id,
        &event.session_id,
        event.report.owner_epoch,
    )
}
