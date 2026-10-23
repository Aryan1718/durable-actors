use std::collections::{HashMap, HashSet};

use tokio::time::Instant;

use super::*;

impl SparePool {
    pub(super) async fn prewarm(self: Arc<Self>, stop: CancellationToken) {
        let mut warmed = HashMap::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = interval.tick() => {},
            }
            tokio::select! {
                () = stop.cancelled() => return,
                result = self.warm_ready(&mut warmed) => report(result),
            }
        }
    }

    async fn warm_ready(&self, warmed: &mut HashMap<String, Instant>) -> Result<()> {
        let ready = self.ready_spares().await?;
        let names: HashSet<_> = ready.iter().map(|spare| spare.name.as_str()).collect();
        warmed.retain(|name, _| names.contains(name.as_str()));
        let due: Vec<_> = ready
            .into_iter()
            .filter(|spare| {
                warmed
                    .get(&spare.name)
                    .is_none_or(|last| last.elapsed() >= Duration::from_secs(20))
            })
            .collect();
        self.provider.prewarm_spares(&due).await;
        for spare in due {
            warmed.insert(spare.name, Instant::now());
        }
        Ok(())
    }

    async fn ready_spares(&self) -> Result<Vec<SpareHandle>> {
        self.store.0.connection().await?.query(
            "SELECT name, handle FROM durable_actors_spares WHERE kind = $1 AND status = 'ready' AND expires_at > clock_timestamp()",
            &[&self.config.kind.as_str()],
        ).await?.iter().map(decode_handle).collect()
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/sandbox/pool_prewarming.rs"]
mod tests;
