use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use moka::{future::Cache, ops::compute::Op};

use super::LifecycleKind;
use crate::{actor_state::ActorStorageKey, redis_client::Redis};

#[async_trait]
pub(crate) trait RoutingHints: Send + Sync {
    async fn record(&self, key: &str, kind: LifecycleKind) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<LifecycleKind>>;
}

pub(crate) struct MemoryRoutingHints(Cache<String, LifecycleKind>);

impl MemoryRoutingHints {
    pub(crate) fn new(retention: Duration) -> Self {
        Self(
            Cache::builder()
                .max_capacity(100_000)
                .time_to_live(retention)
                .build(),
        )
    }
}

#[async_trait]
impl RoutingHints for MemoryRoutingHints {
    async fn record(&self, key: &str, kind: LifecycleKind) -> Result<()> {
        self.0
            .entry(key.to_owned())
            .and_compute_with(|previous| async move {
                if previous.is_some_and(|entry| *entry.value() == LifecycleKind::Stopped) {
                    Op::Nop
                } else {
                    Op::Put(kind)
                }
            })
            .await;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<LifecycleKind>> {
        Ok(self.0.get(key).await)
    }
}

pub(crate) struct RedisRoutingHints {
    redis: Redis,
    namespace: String,
    retention: Duration,
}

impl RedisRoutingHints {
    pub(crate) fn new(redis: Redis, namespace: &str, retention: Duration) -> Self {
        Self {
            redis,
            namespace: namespace.into(),
            retention,
        }
    }
}

#[async_trait]
impl RoutingHints for RedisRoutingHints {
    async fn record(&self, key: &str, kind: LifecycleKind) -> Result<()> {
        let mut command = redis::cmd("SET");
        command
            .arg(format!("{}:{key}", self.namespace))
            .arg(serde_json::to_string(&kind)?)
            .arg("EX")
            .arg(self.retention.as_secs().max(1));
        if kind == LifecycleKind::Draining {
            command.arg("NX");
        }
        let _: Option<String> = self.redis.run(command).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<LifecycleKind>> {
        let mut command = redis::cmd("GET");
        command.arg(format!("{}:{key}", self.namespace));
        let value: Option<String> = self.redis.run(command).await?;
        value
            .map(|value| Ok(serde_json::from_str(&value)?))
            .transpose()
    }
}

pub(super) fn session_key(
    actor: &ActorStorageKey,
    host: &str,
    session: &str,
    epoch: u64,
) -> String {
    serde_json::to_string(&(actor, host, session, epoch)).expect("serializable actor session")
}
