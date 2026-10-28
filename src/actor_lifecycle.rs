use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use moka::{future::Cache, ops::compute::Op};
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    clock::{Clock, SystemClock},
    host_leases::WaitingOperation,
    placement::{
        ActorInstanceOverview, ActorInventory, ActorInventoryReader, ActorInventorySnapshot,
        ActorResidency,
    },
    sandbox::ResourceLimits,
};

pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const STALE_AFTER_MS: u64 = 35_000;

mod pubsub;
pub(crate) mod reconciliation;
mod residency;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LifecycleKind {
    Started,
    Heartbeat,
    Draining,
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleReport {
    pub kind: LifecycleKind,
    pub owner_epoch: u64,
    pub sequence: u64,
    pub started_at_ms: u64,
    pub occurred_at_ms: u64,
    pub resources: ResourceLimits,
    pub resident: Option<bool>,
    #[serde(default)]
    pub last_active_at_ms: Option<u64>,
    pub waiting: Vec<WaitingOperation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LifecycleEvent {
    pub organization_id: Option<String>,
    pub event_id: String,
    pub actor: ActorKey,
    pub host_id: String,
    pub session_id: String,
    pub region: String,
    pub received_at_ms: u64,
    #[serde(flatten)]
    pub report: LifecycleReport,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UsageUpdate {
    pub usage_id: String,
    pub revision: u64,
    pub ended_at_ms: Option<u64>,
    pub estimated: bool,
    #[serde(flatten)]
    pub event: LifecycleEvent,
}

impl From<LifecycleEvent> for UsageUpdate {
    fn from(event: LifecycleEvent) -> Self {
        Self {
            usage_id: format!("{}:{}", event.host_id, event.session_id),
            revision: event.report.sequence * 2,
            ended_at_ms: (event.report.kind == LifecycleKind::Stopped)
                .then_some(event.report.occurred_at_ms),
            estimated: false,
            event,
        }
    }
}

impl UsageUpdate {
    fn estimated_stop(mut event: LifecycleEvent, now: u64) -> Self {
        event.event_id.push_str(":estimated");
        event.report.kind = LifecycleKind::Stopped;
        event.report.resident = None;
        event.report.waiting.clear();
        event.received_at_ms = now;
        let mut update = Self::from(event);
        update.revision += 1;
        update.estimated = true;
        update
    }
}

#[async_trait]
pub(crate) trait LifecyclePersistence: Send + Sync {
    async fn record(&self, event: &LifecycleEvent) -> Result<()>;
    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>>;
    async fn history(
        &self,
        project: &str,
        actor_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<LifecycleEvent>> {
        let _ = from;
        Ok(self
            .latest(project)
            .await?
            .into_iter()
            .filter(|event| {
                event.actor.actor_name == actor_name && event.report.occurred_at_ms <= to
            })
            .collect())
    }
}

#[async_trait]
pub(crate) trait UsagePublisher: Send + Sync {
    async fn publish(&self, event: &UsageUpdate) -> Result<()>;
}

#[async_trait]
pub(crate) trait LifecycleObserver: Send + Sync {
    async fn observe(&self, event: &LifecycleEvent) -> Result<()>;
}

#[derive(Clone)]
pub(crate) struct LifecycleStore {
    persistence: Arc<dyn LifecyclePersistence>,
    usage: Option<Arc<dyn UsagePublisher>>,
    usage_sessions: Arc<dyn reconciliation::UsageSessions>,
    observer: Option<Arc<dyn LifecycleObserver>>,
}

impl Default for LifecycleStore {
    fn default() -> Self {
        Self::new(Arc::new(MemoryLifecyclePersistence::default()))
    }
}

impl LifecycleStore {
    pub(crate) fn new(persistence: Arc<dyn LifecyclePersistence>) -> Self {
        Self {
            persistence,
            usage: None,
            observer: None,
            usage_sessions: Arc::new(reconciliation::MemoryUsageSessions::default()),
        }
    }

    pub(crate) fn with_observer(mut self, observer: Arc<dyn LifecycleObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub(crate) fn with_publisher(mut self, publisher: Arc<dyn UsagePublisher>) -> Self {
        self.usage = Some(publisher);
        self
    }

    pub(crate) fn with_usage_sessions(
        mut self,
        sessions: Arc<dyn reconciliation::UsageSessions>,
    ) -> Self {
        self.usage_sessions = sessions;
        self
    }

    pub(crate) async fn with_usage_topic(mut self, topic: Option<&str>) -> Result<Self> {
        if let Some(topic) = topic {
            self = self.with_publisher(Arc::new(pubsub::PubSubPublisher::connect(topic).await?));
        }
        Ok(self)
    }

    pub(crate) async fn record(&self, event: LifecycleEvent) -> Result<()> {
        ensure!(
            event.report.sequence > 0 && event.report.sequence < (1 << 52),
            "invalid lifecycle sequence"
        );
        ensure!(event.report.owner_epoch > 0, "invalid ownership epoch");
        ensure!(
            event.report.occurred_at_ms >= event.report.started_at_ms,
            "invalid lifecycle time range"
        );
        let (persisted, (), observed) = tokio::join!(
            self.persistence.record(&event),
            self.record_usage(&event),
            async {
                match &self.observer {
                    Some(observer) => observer.observe(&event).await,
                    None => Ok(()),
                }
            }
        );
        persisted.and(observed)
    }

    pub(crate) fn start_reconciliation(&self, stop: tokio_util::sync::CancellationToken) {
        if self.usage.is_none() {
            return;
        }
        let store = self.clone();
        tokio::spawn(async move {
            let work = async {
                let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let result = async { store.reconcile(SystemClock.now_ms()?).await }.await;
                    if let Err(error) = result {
                        tracing::warn!(%error, "usage reconciliation failed");
                    }
                }
            };
            tokio::select! { _ = stop.cancelled() => {}, _ = work => {} }
        });
    }

    async fn record_usage(&self, event: &LifecycleEvent) {
        if self.usage.is_none() {
            return;
        }
        let boundary = matches!(
            event.report.kind,
            LifecycleKind::Started | LifecycleKind::Stopped
        );
        if boundary {
            self.publish_usage(UsageUpdate::from(event.clone()));
        }
        match self.usage_sessions.observe(event).await {
            Ok(true) if !boundary => self.publish_usage(UsageUpdate::from(event.clone())),
            Err(error) => tracing::warn!(%error, "usage session could not be recorded"),
            _ => {}
        }
    }

    async fn reconcile(&self, now: u64) -> Result<()> {
        for event in self.usage_sessions.expire(now).await? {
            self.publish_usage(UsageUpdate::estimated_stop(event, now));
        }
        Ok(())
    }

    fn publish_usage(&self, update: UsageUpdate) {
        let Some(publisher) = self.usage.clone() else {
            return;
        };
        tokio::spawn(async move {
            if let Err(error) =
                tokio::time::timeout(Duration::from_secs(5), publisher.publish(&update))
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|result| result)
            {
                tracing::warn!(event_id = update.event.event_id, %error, "usage publication failed");
            }
        });
    }
}

#[async_trait]
impl ActorInventoryReader for LifecycleStore {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let now = SystemClock.now_ms()?;
        let mut actors = BTreeMap::<String, ActorInventory>::new();
        for event in self.persistence.latest(project).await? {
            let status = residency(&event, now);
            let actor = actors
                .entry(event.actor.actor_name.clone())
                .or_insert_with(|| ActorInventory {
                    actor_name: event.actor.actor_name.clone(),
                    ..Default::default()
                });
            match status {
                ActorResidency::Live => actor.live += 1,
                ActorResidency::Dormant => actor.dormant += 1,
                ActorResidency::Unknown => actor.unknown += 1,
            }
            actor.instances.push(ActorInstanceOverview {
                actor_id: event.actor.actor_id,
                status,
                last_active_at_ms: event.report.last_active_at_ms,
                connections: Vec::new(),
                waiting: match status {
                    ActorResidency::Unknown => None,
                    ActorResidency::Dormant if event.report.kind == LifecycleKind::Stopped => {
                        Some(Vec::new())
                    }
                    _ => Some(event.report.waiting),
                },
            });
        }
        Ok(ActorInventorySnapshot {
            actors: actors.into_values().collect(),
            connections_complete: true,
        })
    }

    async fn residency_history(
        &self,
        project: &str,
        actor_name: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<crate::placement::ResidencyHistory> {
        let events = self
            .persistence
            .history(project, actor_name, from_ms, to_ms)
            .await?;
        Ok(residency::history(events, from_ms, to_ms))
    }
}

fn residency(event: &LifecycleEvent, now: u64) -> ActorResidency {
    if event.report.kind == LifecycleKind::Stopped {
        return ActorResidency::Dormant;
    }
    if now.saturating_sub(event.received_at_ms) > STALE_AFTER_MS {
        return ActorResidency::Unknown;
    }
    match event.report.resident {
        Some(true) => ActorResidency::Live,
        Some(false) => ActorResidency::Dormant,
        None => ActorResidency::Unknown,
    }
}

struct MemoryLifecyclePersistence(
    Cache<ActorKey, LifecycleEvent>,
    Cache<(ActorKey, String), LifecycleEvent>,
);

impl Default for MemoryLifecyclePersistence {
    fn default() -> Self {
        Self(
            Cache::builder().max_capacity(10_000).build(),
            Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(86_400))
                .build(),
        )
    }
}

#[async_trait]
impl LifecyclePersistence for MemoryLifecyclePersistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
        self.1
            .insert((event.actor.clone(), event.event_id.clone()), event.clone())
            .await;
        self.0
            .entry(event.actor.clone())
            .and_compute_with(|previous| async move {
                if previous.is_some_and(|previous| {
                    let old = &previous.value().report;
                    (old.owner_epoch, old.sequence)
                        >= (event.report.owner_epoch, event.report.sequence)
                }) {
                    Op::Nop
                } else {
                    Op::Put(event.clone())
                }
            })
            .await;
        Ok(())
    }

    async fn latest(&self, project: &str) -> Result<Vec<LifecycleEvent>> {
        let mut events: Vec<_> = self
            .0
            .iter()
            .filter(|(actor, _)| actor.project_id == project)
            .map(|(_, event)| event)
            .collect();
        events.sort_by(|a, b| {
            (&a.actor.actor_name, &a.actor.actor_id).cmp(&(&b.actor.actor_name, &b.actor.actor_id))
        });
        Ok(events)
    }

    async fn history(
        &self,
        project: &str,
        actor_name: &str,
        from: u64,
        to: u64,
    ) -> Result<Vec<LifecycleEvent>> {
        let mut before = BTreeMap::<String, LifecycleEvent>::new();
        let mut events = Vec::new();
        for event in self
            .1
            .iter()
            .map(|(_, event)| event)
            .chain(self.0.iter().map(|(_, event)| event))
        {
            if event.actor.project_id != project
                || event.actor.actor_name != actor_name
                || event.report.occurred_at_ms > to
            {
                continue;
            }
            if event.report.occurred_at_ms >= from {
                events.push(event);
            } else {
                let previous = before
                    .entry(event.actor.actor_id.clone())
                    .or_insert_with(|| event.clone());
                if (event.report.owner_epoch, event.report.sequence)
                    > (previous.report.owner_epoch, previous.report.sequence)
                {
                    *previous = event;
                }
            }
        }
        events.extend(before.into_values());
        events.sort_by(|a, b| {
            (&b.report.occurred_at_ms, &b.actor.actor_id, &b.event_id).cmp(&(
                &a.report.occurred_at_ms,
                &a.actor.actor_id,
                &a.event_id,
            ))
        });
        events.dedup_by(|a, b| a.actor == b.actor && a.event_id == b.event_id);
        events.truncate(residency::EVENT_LIMIT + 1);
        Ok(events)
    }
}

#[cfg(test)]
#[path = "../tests/unit/actor_lifecycle.rs"]
mod tests;
