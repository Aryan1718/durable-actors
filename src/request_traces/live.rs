use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use super::{TRACE_BATCH_SIZE, TraceEvent};
use crate::redis_client::Redis;

const CHANNEL: &str = "durable-actors:v1:traces";

pub(crate) trait CommittedTracePublisher: Send + Sync {
    fn publish(&self, events: &[TraceEvent]);
}

#[derive(Clone)]
pub(crate) struct LiveTraceFeed {
    updates: broadcast::Sender<Arc<LiveTraceUpdate>>,
    outgoing: Option<mpsc::Sender<Arc<LiveTraceUpdate>>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum LiveTraceUpdate {
    Batch(Vec<TraceEvent>),
    Resync,
}

impl Default for LiveTraceFeed {
    fn default() -> Self {
        Self {
            updates: broadcast::channel(128).0,
            outgoing: None,
        }
    }
}

impl LiveTraceFeed {
    pub(crate) fn start(redis: Option<Redis>, stop: CancellationToken) -> Self {
        let mut feed = Self::default();
        if let Some(redis) = redis {
            let origin = uuid::Uuid::new_v4().to_string();
            let (sender, receiver) = mpsc::channel(128);
            feed.outgoing = Some(sender);
            tokio::spawn(publish_batches(
                redis.clone(),
                origin.clone(),
                receiver,
                stop.clone(),
            ));
            tokio::spawn(subscribe(redis, origin, feed.updates.clone(), stop));
        }
        feed
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Arc<LiveTraceUpdate>> {
        self.updates.subscribe()
    }
}

impl CommittedTracePublisher for LiveTraceFeed {
    fn publish(&self, events: &[TraceEvent]) {
        for events in events.chunks(TRACE_BATCH_SIZE) {
            let update = Arc::new(LiveTraceUpdate::Batch(events.to_vec()));
            let _ = self.updates.send(update.clone());
            if let Some(outgoing) = &self.outgoing {
                if outgoing.try_send(update).is_err() {
                    tracing::warn!("Live trace broadcast queue full; dashboards will reconcile");
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    origin: String,
    update: T,
}

async fn publish_batches(
    redis: Redis,
    origin: String,
    mut receiver: mpsc::Receiver<Arc<LiveTraceUpdate>>,
    stop: CancellationToken,
) {
    let work = async {
        while let Some(update) = receiver.recv().await {
            let result = async {
                let payload = serde_json::to_string(&Envelope {
                    origin: origin.clone(),
                    update: update.as_ref(),
                })?;
                let mut command = redis::cmd("PUBLISH");
                command.arg(CHANNEL).arg(payload);
                redis.run::<usize>(command).await?;
                anyhow::Ok(())
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "Live trace broadcast failed; dashboards will reconcile");
            }
        }
    };
    tokio::select! { _ = stop.cancelled() => {}, _ = work => {} }
}

async fn subscribe(
    redis: Redis,
    origin: String,
    updates: broadcast::Sender<Arc<LiveTraceUpdate>>,
    stop: CancellationToken,
) {
    loop {
        let result = tokio::select! {
            _ = stop.cancelled() => return,
            result = listen(&redis, &origin, &updates) => result,
        };
        if let Err(error) = result {
            tracing::warn!(%error, "Live trace subscription disconnected; dashboards will reconcile");
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
        }
    }
}

async fn listen(
    redis: &Redis,
    origin: &str,
    updates: &broadcast::Sender<Arc<LiveTraceUpdate>>,
) -> Result<()> {
    let mut subscriber = redis.pubsub().await?;
    tokio::time::timeout(Duration::from_secs(5), subscriber.subscribe(CHANNEL)).await??;
    let _ = updates.send(Arc::new(LiveTraceUpdate::Resync));
    let mut messages = subscriber.on_message();
    while let Some(message) = messages.next().await {
        let envelope: Envelope<LiveTraceUpdate> =
            serde_json::from_slice(message.get_payload_bytes())?;
        if envelope.origin != origin {
            let _ = updates.send(Arc::new(envelope.update));
        }
    }
    anyhow::bail!("Redis trace subscription closed")
}
