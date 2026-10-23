use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    actor::ActorExecutor,
    actor_lifecycle::{HEARTBEAT_INTERVAL, LifecycleKind, LifecycleReport},
    clock::{Clock, SystemClock},
    control_plane::ControlPlaneClient,
    sandbox::ResourceLimits,
};

use super::actor_host::ActorStatus;

const REPORT_TIMEOUT: Duration = Duration::from_secs(1);

#[async_trait]
pub(super) trait LifecycleTransport: Send + Sync {
    async fn report(&self, report: LifecycleReport) -> Result<()>;
}

#[async_trait]
impl LifecycleTransport for ControlPlaneClient {
    async fn report(&self, report: LifecycleReport) -> Result<()> {
        self.report_lifecycle(report).await
    }
}

pub(super) struct HostLifecycle {
    commands: mpsc::Sender<LifecycleKind>,
    task: JoinHandle<()>,
}

impl HostLifecycle {
    pub(super) fn start(
        transport: Arc<dyn LifecycleTransport>,
        owner_epoch: u64,
        resources: ResourceLimits,
        executor: Arc<dyn ActorExecutor>,
        status: watch::Receiver<ActorStatus>,
    ) -> Result<Self> {
        let (commands, receiver) = mpsc::channel(2);
        let reporter = Reporter {
            transport,
            executor,
            status,
            report: LifecycleReport {
                kind: LifecycleKind::Started,
                owner_epoch,
                sequence: 0,
                started_at_ms: SystemClock.now_ms()?,
                occurred_at_ms: 0,
                resources,
                resident: None,
                waiting: Vec::new(),
            },
        };
        let task = tokio::spawn(reporter.run(receiver));
        Ok(Self { commands, task })
    }

    pub(super) fn draining(&self) {
        let _ = self.commands.try_send(LifecycleKind::Draining);
    }

    pub(super) async fn stop(mut self) {
        let _ = self.commands.try_send(LifecycleKind::Stopped);
        let _ = tokio::time::timeout(Duration::from_secs(3), &mut self.task).await;
    }
}

impl Drop for HostLifecycle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Reporter {
    transport: Arc<dyn LifecycleTransport>,
    executor: Arc<dyn ActorExecutor>,
    status: watch::Receiver<ActorStatus>,
    report: LifecycleReport,
}

impl Reporter {
    async fn run(mut self, mut commands: mpsc::Receiver<LifecycleKind>) {
        self.send(LifecycleKind::Started).await;
        let mut interval = tokio::time::interval_at(
            tokio::time::Instant::now() + HEARTBEAT_INTERVAL,
            HEARTBEAT_INTERVAL,
        );
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut draining = false;
        loop {
            let kind = tokio::select! {
                biased;
                command = commands.recv() => match command { Some(kind) => kind, None => break },
                _ = interval.tick(), if !draining => LifecycleKind::Heartbeat,
            };
            self.send(kind).await;
            if kind == LifecycleKind::Stopped {
                break;
            }
            draining |= kind == LifecycleKind::Draining;
        }
    }

    async fn send(&mut self, kind: LifecycleKind) {
        let Ok(now) = SystemClock.now_ms() else {
            return;
        };
        self.report.kind = kind;
        self.report.sequence += 1;
        self.report.occurred_at_ms = now;
        self.report.resident = self.executor.resident();
        self.report.waiting = self.status.borrow().waiting.clone();
        if kind == LifecycleKind::Stopped {
            self.report.resident = Some(false);
            self.report.waiting.clear();
        }
        if let Err(error) =
            tokio::time::timeout(REPORT_TIMEOUT, self.transport.report(self.report.clone()))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
        {
            tracing::warn!(?kind, %error, "host lifecycle report failed");
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/lifecycle.rs"]
mod tests;
