use std::time::Duration;

use anyhow::{Result, bail};
use async_trait::async_trait;

use super::{
    Activation, AdmissionRejected, LAUNCH_LEASE_MS, OrganizationLimits, REPORT_LEASE_MS,
    ReservationStore,
};
use crate::{
    actor_lifecycle::{LifecycleKind, LifecycleReport},
    redis_client::Redis,
};

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

    fn prefix(&self, organization: &str) -> String {
        format!("{}:{{{organization}}}", self.namespace)
    }

    fn retention_ms(&self) -> u64 {
        self.retention.as_millis().max(1) as u64
    }
}

#[async_trait]
impl ReservationStore for RedisReservations {
    async fn reserve(&self, activation: &Activation, limits: OrganizationLimits) -> Result<()> {
        let prefix = self.prefix(&activation.organization);
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
            .arg(limits.start_burst)
            .arg(LAUNCH_LEASE_MS)
            .arg(self.retention_ms());
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

    async fn release(&self, activation: &Activation) -> Result<()> {
        let prefix = self.prefix(&activation.organization);
        let member = activation.member()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("release.lua"))
            .arg(2)
            .arg(format!("{prefix}:active"))
            .arg(format!("{prefix}:session:{member}"))
            .arg(member);
        let _: i32 = self.redis.run(command).await?;
        Ok(())
    }

    async fn observe(&self, activation: &Activation, report: &LifecycleReport) -> Result<()> {
        let prefix = self.prefix(&activation.organization);
        let member = activation.member()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(include_str!("observe.lua"))
            .arg(2)
            .arg(format!("{prefix}:active"))
            .arg(format!("{prefix}:session:{member}"))
            .arg(member)
            .arg(report.owner_epoch)
            .arg(report.sequence)
            .arg(report.kind == LifecycleKind::Stopped)
            .arg(self.retention_ms())
            .arg(REPORT_LEASE_MS);
        let _: i32 = self.redis.run(command).await?;
        Ok(())
    }
}
