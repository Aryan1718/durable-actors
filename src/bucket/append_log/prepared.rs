use super::*;
use crate::bucket::RapidPlacement;
use futures_util::{StreamExt, stream::FuturesUnordered};

type Opened = Vec<(Replica, Box<dyn LogWriter>)>;

pub(super) struct Prepared {
    pub id: String,
    pub prefix: String,
    opened: AbortOnDropHandle<Result<Option<Opened>>>,
}

impl Prepared {
    pub fn new(prefix: String, zones: Vec<Arc<dyn LogZone>>, placement: RapidPlacement) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let object = format!("{prefix}{id}.segment");
        let opened = AbortOnDropHandle::new(tokio::spawn(async move {
            match placement {
                RapidPlacement::All => open_all(&zones, &object).await,
                RapidPlacement::Nearest => open_nearest(zones, object).await,
            }
        }));
        Self { id, prefix, opened }
    }

    pub async fn take(self) -> Result<Option<Opened>> {
        self.opened
            .await
            .context("Rapid connection preparation failed")?
    }
}

async fn open_all(zones: &[Arc<dyn LogZone>], object: &str) -> Result<Option<Opened>> {
    let results =
        futures_util::future::join_all(zones.iter().map(|zone| bounded(zone.open(object)))).await;
    let mut opened = Vec::new();
    for (zone, result) in zones.iter().zip(results) {
        match result {
            Ok((replica, writer)) => {
                if let Err(error) = validate_replica(zone.as_ref(), &replica, object) {
                    tracing::warn!(%error, "Rapid log replica invalid; activation will use Standard");
                    drop(writer);
                    let _ = bounded(zone.delete(&replica)).await;
                } else {
                    opened.push((replica, writer));
                }
            }
            Err(error) => {
                tracing::warn!(%error, "Rapid log unavailable; activation will use Standard")
            }
        }
    }
    if opened.len() == zones.len() {
        return Ok(Some(opened));
    }
    for (replica, writer) in opened {
        drop(writer);
        if let Some(zone) = zones.iter().find(|zone| zone.bucket() == replica.bucket) {
            let _ = bounded(zone.delete(&replica)).await;
        }
    }
    Ok(None)
}

/// The first zone to open is the closest one from this host; later opens are discarded.
async fn open_nearest(zones: Vec<Arc<dyn LogZone>>, object: String) -> Result<Option<Opened>> {
    let mut opens: FuturesUnordered<_> = zones
        .into_iter()
        .map(|zone| {
            let object = object.clone();
            async move {
                let result = bounded(zone.open(&object)).await;
                (zone, result)
            }
        })
        .collect();
    while let Some((_, result)) = opens.next().await {
        match result {
            Ok(copy) => {
                tokio::spawn(discard(opens));
                return Ok(Some(vec![copy]));
            }
            Err(error) => tracing::warn!(%error, "Rapid zone unavailable during placement"),
        }
    }
    tracing::warn!("Rapid log unavailable; activation will use Standard");
    Ok(None)
}

async fn discard<F>(mut opens: FuturesUnordered<F>)
where
    F: std::future::Future<Output = (Arc<dyn LogZone>, Result<(Replica, Box<dyn LogWriter>)>)>,
{
    while let Some((zone, result)) = opens.next().await {
        if let Ok((replica, writer)) = result {
            drop(writer);
            if let Err(error) = bounded(zone.delete(&replica)).await {
                tracing::warn!(%error, "unused Rapid placement was not deleted");
            }
        }
    }
}
