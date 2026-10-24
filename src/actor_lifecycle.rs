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
pub(crate) mod routing;

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
    routing: Arc<dyn routing::RoutingHints>,
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
            routing: Arc::new(routing::MemoryRoutingHints::new(Duration::from_secs(
                86_460,
            ))),
        }
    }

    pub(crate) fn with_routing(mut self, routing: Arc<dyn routing::RoutingHints>) -> Self {
        self.routing = routing;
        self
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
        let (persisted, (), (), observed) = tokio::join!(
            self.persistence.record(&event),
            self.record_routing(&event),
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

    pub(crate) async fn routing(
        &self,
        actor: &crate::actor_state::ActorStorageKey,
        host: &str,
        session: &str,
        epoch: u64,
    ) -> Option<LifecycleKind> {
        match self
            .routing
            .get(&routing::session_key(actor, host, session, epoch))
            .await
        {
            Ok(hint) => hint,
            Err(error) => {
                tracing::warn!(%error, "shutdown hint unavailable; routing with ownership lease");
                None
            }
        }
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

    async fn record_routing(&self, event: &LifecycleEvent) {
        if !matches!(
            event.report.kind,
            LifecycleKind::Draining | LifecycleKind::Stopped
        ) {
            return;
        }
        let key = routing::session_key(
            &event.actor.storage_key(),
            &event.host_id,
            &event.session_id,
            event.report.owner_epoch,
        );
        if let Err(error) = self.routing.record(&key, event.report.kind).await {
            tracing::warn!(%error, "host shutdown hint could not be recorded");
        }
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

struct MemoryLifecyclePersistence(Cache<ActorKey, LifecycleEvent>);

impl Default for MemoryLifecyclePersistence {
    fn default() -> Self {
        Self(
            Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(Duration::from_secs(86_400))
                .build(),
        )
    }
}

#[async_trait]
impl LifecyclePersistence for MemoryLifecyclePersistence {
    async fn record(&self, event: &LifecycleEvent) -> Result<()> {
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
}

#[cfg(test)]
#[path = "../tests/unit/actor_lifecycle.rs"]
mod tests;
