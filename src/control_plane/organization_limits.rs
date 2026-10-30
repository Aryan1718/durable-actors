use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{
    actor::ActorKey,
    actor_lifecycle::{LifecycleEvent, LifecycleObserver, LifecycleReport},
    host::HostId,
    postgres::PostgresDatabase,
};

mod memory;
mod redis;
pub(super) use memory::MemoryReservations;
pub(super) use redis::RedisReservations;

// Slots are leases so lost stop reports, crashed hosts, and cancelled launches cannot leak them.
// A launch outlasts the slowest sandbox start; reports must arrive within the heartbeat staleness window.
const LAUNCH_LEASE_MS: u64 = 180_000;
const REPORT_LEASE_MS: u64 = crate::actor_lifecycle::STALE_AFTER_MS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrganizationLimits {
    pub max_active_actors: u32,
    pub starts_per_second: u32,
    pub start_burst: u32,
}

impl Default for OrganizationLimits {
    fn default() -> Self {
        Self {
            max_active_actors: 500,
            starts_per_second: 50,
            start_burst: 100,
        }
    }
}

impl OrganizationLimits {
    pub(super) fn from_lookup(
        get: &mut impl FnMut(&str) -> Option<String>,
        enabled: bool,
    ) -> Result<Option<Self>> {
        let enabled = get("DURABLE_ACTORS_ORGANIZATION_LIMITS_ENABLED")
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("DURABLE_ACTORS_ORGANIZATION_LIMITS_ENABLED must be boolean")?
            .unwrap_or(enabled);
        if !enabled {
            return Ok(None);
        }
        let defaults = Self::default();
        Ok(Some(Self {
            max_active_actors: positive(
                get,
                "DURABLE_ACTORS_ORGANIZATION_MAX_ACTIVE_ACTORS",
                defaults.max_active_actors,
            )?,
            starts_per_second: positive(
                get,
                "DURABLE_ACTORS_ORGANIZATION_ACTOR_STARTS_PER_SECOND",
                defaults.starts_per_second,
            )?,
            start_burst: positive(
                get,
                "DURABLE_ACTORS_ORGANIZATION_ACTOR_START_BURST",
                defaults.start_burst,
            )?,
        }))
    }
}

pub(super) fn local_admission(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<Arc<ActorAdmission>>> {
    Ok(OrganizationLimits::from_lookup(get, false)?.map(|limits| {
        Arc::new(ActorAdmission::new(
            limits,
            Arc::new(MemoryReservations::new(
                Arc::new(crate::clock::SystemClock),
                std::time::Duration::from_secs(86_460),
            )),
        ))
    }))
}

pub(super) struct ActorAdmission {
    defaults: OrganizationLimits,
    database: Option<PostgresDatabase>,
    reservations: Arc<dyn ReservationStore>,
}

impl ActorAdmission {
    pub fn new(defaults: OrganizationLimits, reservations: Arc<dyn ReservationStore>) -> Self {
        Self {
            defaults,
            database: None,
            reservations,
        }
    }

    pub fn with_database(mut self, database: PostgresDatabase) -> Self {
        self.database = Some(database);
        self
    }

    pub async fn reserve(
        &self,
        organization: &str,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
        let reserve = async {
            actor.validate()?;
            let limits = self.limits(organization).await?;
            let activation = Activation::new(organization, actor, host.as_str(), session);
            self.reservations.reserve(&activation, limits).await
        };
        reserve.await.context(AdmissionFailure)
    }

    pub async fn release(
        &self,
        organization: &str,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) {
        let activation = Activation::new(organization, actor, host.as_str(), session);
        if let Err(error) = self.reservations.release(&activation).await {
            tracing::warn!(organization_id = organization, error = %format!("{error:#}"), "failed launch kept its activation slot");
        }
    }

    async fn limits(&self, organization: &str) -> Result<OrganizationLimits> {
        let Some(database) = &self.database else {
            return Ok(self.defaults);
        };
        let row = database.query_opt(
            "SELECT max_active_actors, actor_starts_per_second, actor_start_burst FROM durable_actors_organization_limits WHERE organization_id = $1", &[&organization],
        ).await?;
        let Some(row) = row else {
            return Ok(self.defaults);
        };
        Ok(OrganizationLimits {
            max_active_actors: row
                .get::<_, Option<i64>>(0)
                .map(u32::try_from)
                .transpose()?
                .unwrap_or(self.defaults.max_active_actors),
            starts_per_second: row
                .get::<_, Option<i64>>(1)
                .map(u32::try_from)
                .transpose()?
                .unwrap_or(self.defaults.starts_per_second),
            start_burst: row
                .get::<_, Option<i64>>(2)
                .map(u32::try_from)
                .transpose()?
                .unwrap_or(self.defaults.start_burst),
        })
    }
}

#[async_trait]
impl LifecycleObserver for ActorAdmission {
    async fn observe(&self, event: &LifecycleEvent) -> Result<()> {
        let organization = event
            .organization_id
            .as_deref()
            .context("lifecycle report has no organization")?;
        let activation = Activation::new(
            organization,
            &event.actor,
            &event.host_id,
            &event.session_id,
        );
        self.reservations.observe(&activation, &event.report).await
    }
}

#[async_trait]
pub(super) trait ReservationStore: Send + Sync {
    async fn reserve(&self, activation: &Activation, limits: OrganizationLimits) -> Result<()>;
    async fn release(&self, activation: &Activation) -> Result<()>;
    async fn observe(&self, activation: &Activation, report: &LifecycleReport) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Activation {
    organization: String,
    actor: ActorKey,
    host: String,
    session: String,
}

impl Activation {
    fn new(organization: &str, actor: &ActorKey, host: &str, session: &str) -> Self {
        Self {
            organization: organization.into(),
            actor: actor.clone(),
            host: host.into(),
            session: session.into(),
        }
    }

    fn member(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            &self.actor.project_id,
            &self.actor.actor_name,
            &self.actor.actor_id,
            &self.host,
            &self.session,
        ))?)
    }
}

#[derive(Debug)]
pub(super) enum AdmissionRejected {
    Quota { limit: u32 },
    Rate { retry_after_seconds: u64 },
}

impl std::fmt::Display for AdmissionRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quota { limit } => write!(
                f,
                "organization limit of {limit} starting or running actors has been reached"
            ),
            Self::Rate { .. } => {
                f.write_str("organization actor start rate limit has been reached")
            }
        }
    }
}

impl std::error::Error for AdmissionRejected {}

#[derive(Debug)]
pub(super) struct AdmissionFailure;

impl std::fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("organization activation admission failed")
    }
}

impl std::error::Error for AdmissionFailure {}

fn positive(get: &mut impl FnMut(&str) -> Option<String>, name: &str, default: u32) -> Result<u32> {
    let value = get(name)
        .map(|value| value.parse::<u32>())
        .transpose()
        .with_context(|| format!("{name} must be a positive integer"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be a positive integer");
    Ok(value)
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/organization_limits.rs"]
mod tests;
