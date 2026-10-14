use super::{actor_database::ActorStorage, storage::HostStorage};
use crate::{
    state_transport::{SnapshotWriter, StateWrite},
    storage::WritePlan,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub(super) struct ActorPersistence {
    saves: mpsc::Sender<PendingSave>,
    stop: CancellationToken,
}

impl ActorPersistence {
    pub fn new(storage: Arc<HostStorage>) -> Arc<Self> {
        Self::with_writer(
            Arc::new(HostWriter {
                storage: storage.clone(),
            }),
            storage.stop.clone(),
        )
    }

    fn with_writer<W: SnapshotWriter + 'static>(
        writer: Arc<W>,
        stop: CancellationToken,
    ) -> Arc<Self> {
        let (saves, pending) = mpsc::channel(1);
        tokio::spawn(save_in_order(writer, pending, stop.clone()));
        Arc::new(Self { saves, stop })
    }
}

#[async_trait]
impl SnapshotWriter for ActorPersistence {
    async fn write_snapshot(&self, plan: &WritePlan, bytes: bytes::Bytes) -> Result<StateWrite> {
        let attempt = self.stop.clone().drop_guard();
        let (reply, result) = oneshot::channel();
        self.saves
            .send(PendingSave {
                plan: plan.clone(),
                bytes,
                reply,
            })
            .await
            .map_err(|_| anyhow::anyhow!("actor persistence queue stopped"))?;
        let result = result
            .await
            .context("actor persistence stopped before acknowledgment")??;
        attempt.disarm();
        Ok(result)
    }
}

struct PendingSave {
    plan: WritePlan,
    bytes: bytes::Bytes,
    reply: oneshot::Sender<Result<StateWrite>>,
}

impl PendingSave {
    async fn persist(self, writer: &impl SnapshotWriter) -> bool {
        let result = writer.write_snapshot(&self.plan, self.bytes).await;
        let success = result.is_ok();
        let _ = self.reply.send(result);
        success
    }
}

async fn save_in_order<W: SnapshotWriter>(
    writer: Arc<W>,
    mut pending: mpsc::Receiver<PendingSave>,
    stop: CancellationToken,
) {
    loop {
        let save = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            save = pending.recv() => match save { Some(save) => save, None => return },
        };
        let success = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            success = save.persist(&*writer) => success,
        };
        if !success {
            return;
        }
    }
}

struct HostWriter {
    storage: Arc<HostStorage>,
}

#[async_trait]
impl SnapshotWriter for HostWriter {
    async fn write_snapshot(&self, plan: &WritePlan, bytes: bytes::Bytes) -> Result<StateWrite> {
        self.storage.ensure_authority()?;
        let attempt = self.storage.stop.clone().drop_guard();
        let result = self.storage.runtime.write_snapshot(plan, bytes).await?;
        self.storage.ensure_authority()?;
        attempt.disarm();
        Ok(result)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/persistence.rs"]
mod tests;
