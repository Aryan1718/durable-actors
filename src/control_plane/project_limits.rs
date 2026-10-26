use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{
    actor::ActorKey,
    actor_lifecycle::{LifecycleEvent, LifecycleObserver},
    host::HostId,
    postgres::PostgresDatabase,
};

mod memory;
mod redis;
pub(super) use memory::MemoryReservations;
pub(super) use redis::RedisReservations;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectLimits {
    pub max_active_actors: u32,
    pub starts_per_second: u32,
    pub start_burst: u32,
}

impl Default for ProjectLimits {
    fn default() -> Self {
        Self {
            max_active_actors: 500,
            starts_per_second: 50,
            start_burst: 100,
        }
    }
}

impl ProjectLimits {
    pub(super) fn from_lookup(
        get: &mut impl FnMut(&str) -> Option<String>,
        enabled: bool,
    ) -> Result<Option<Self>> {
        let enabled = get("DURABLE_ACTORS_PROJECT_LIMITS_ENABLED")
            .map(|value| value.parse::<bool>())
            .transpose()
            .context("DURABLE_ACTORS_PROJECT_LIMITS_ENABLED must be boolean")?
            .unwrap_or(enabled);
        if !enabled {
            return Ok(None);
        }
        let defaults = Self::default();
        Ok(Some(Self {
            max_active_actors: positive(
                get,
                "DURABLE_ACTORS_PROJECT_MAX_ACTIVE_ACTORS",
                defaults.max_active_actors,
            )?,
            starts_per_second: positive(
                get,
                "DURABLE_ACTORS_PROJECT_ACTOR_STARTS_PER_SECOND",
                defaults.starts_per_second,
            )?,
            start_burst: positive(
                get,
                "DURABLE_ACTORS_PROJECT_ACTOR_START_BURST",
                defaults.start_burst,
            )?,
        }))
    }
}

pub(super) fn local_admission(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<Arc<ActorAdmission>>> {
    Ok(ProjectLimits::from_lookup(get, false)?.map(|limits| {
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
    defaults: ProjectLimits,
    database: Option<PostgresDatabase>,
    reservations: Arc<dyn ReservationStore>,
}

impl ActorAdmission {
    pub fn new(defaults: ProjectLimits, reservations: Arc<dyn ReservationStore>) -> Self {
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

    pub async fn reserve(&self, actor: &ActorKey, host: &HostId, session: &str) -> Result<()> {
        let reserve = async {
            actor.validate()?;
            let limits = self.limits(&actor.project_id).await?;
            self.reservations
                .reserve(&Activation::new(actor, host.as_str(), session), limits)
                .await
        };
        reserve.await.context(AdmissionFailure)
    }

    pub async fn release(&self, actor: &ActorKey, host: &HostId, session: &str) {
        let activation = Activation::new(actor, host.as_str(), session);
        if let Err(error) = self.reservations.release(&activation).await {
            tracing::warn!(project_id = %actor.project_id, error = %format!("{error:#}"), "failed launch kept its activation slot");
        }
    }

    async fn limits(&self, project: &str) -> Result<ProjectLimits> {
        let Some(database) = &self.database else {
            return Ok(self.defaults);
        };
        let row = database.query_opt(
            "SELECT max_active_actors, actor_starts_per_second, actor_start_burst FROM durable_actors_project_limits WHERE project_id = $1", &[&project],
        ).await?;
        let Some(row) = row else {
            return Ok(self.defaults);
        };
        Ok(ProjectLimits {
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
        self.reservations.observe(event).await
    }
}

#[async_trait]
pub(super) trait ReservationStore: Send + Sync {
    async fn reserve(&self, activation: &Activation, limits: ProjectLimits) -> Result<()>;
    async fn release(&self, activation: &Activation) -> Result<()>;
    async fn observe(&self, event: &LifecycleEvent) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Activation {
    actor: ActorKey,
    host: String,
    session: String,
}

impl Activation {
    fn new(actor: &ActorKey, host: &str, session: &str) -> Self {
        Self {
            actor: actor.clone(),
            host: host.into(),
            session: session.into(),
        }
    }

    fn member(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
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
                "project limit of {limit} starting or running actors has been reached"
            ),
            Self::Rate { .. } => f.write_str("project actor start rate limit has been reached"),
        }
    }
}

impl std::error::Error for AdmissionRejected {}

#[derive(Debug)]
pub(super) struct AdmissionFailure;

impl std::fmt::Display for AdmissionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("project activation admission failed")
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
#[path = "../../tests/unit/control_plane/project_limits.rs"]
mod tests;
