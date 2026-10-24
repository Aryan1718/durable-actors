use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;

use super::{Activation, AdmissionRejected, ProjectLimits, ReservationStore};
use crate::{actor_lifecycle::LifecycleEvent, redis_client::Redis};

pub(in crate::control_plane) struct RedisReservations {
    redis: Redis,
    namespace: String,
    retention: Duration,
}

impl RedisReservations {
    pub fn new(redis: Redis, namespace: &str, retention: Duration) -> Self {
        Self {
            redis,
            namespace: namespace.into(),
            retention,
        }
    }

    fn prefix(&self, project: &str) -> String {
        format!("{}:{{{project}}}", self.namespace)
    }
}

#[async_trait]
impl ReservationStore for RedisReservations {
    async fn reserve(&self, activation: &Activation, limits: ProjectLimits) -> Result<()> {
        let prefix = self.prefix(&activation.actor.project_id);
        let member = activation.member()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("reserve.lua"))
            .arg(3)
            .arg(format!("{prefix}:active"))
            .arg(format!("{prefix}:bucket"))
            .arg(format!("{prefix}:session:{member}"))
            .arg(member)
            .arg(limits.max_active_actors)
            .arg(limits.starts_per_second)
            .arg(limits.start_burst);
        let (code, retry): (i32, u64) = self.redis.run(command).await?;
        match code {
            0 => Ok(()),
            1 => Err(AdmissionRejected::Quota {
                limit: limits.max_active_actors,
            }
            .into()),
            2 => Err(AdmissionRejected::Rate {
                retry_after_seconds: retry,
            }
            .into()),
            _ => bail!("actor activation has already stopped"),
        }
    }

    async fn observe(&self, event: &LifecycleEvent) -> Result<()> {
        let prefix = self.prefix(&event.actor.project_id);
        let member = Activation::new(&event.actor, &event.host_id, &event.session_id).member()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("observe.lua"))
            .arg(2)
            .arg(format!("{prefix}:active"))
            .arg(format!("{prefix}:session:{member}"))
            .arg(member)
            .arg(event.report.owner_epoch)
            .arg(event.report.sequence)
            .arg(event.report.kind == crate::actor_lifecycle::LifecycleKind::Stopped)
            .arg(self.retention.as_millis().max(1) as u64);
        let _: i32 = self.redis.run(command).await?;
        Ok(())
    }
}
