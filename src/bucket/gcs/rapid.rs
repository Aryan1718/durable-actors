use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::{
    appendable_object_writer::AppendableObjectWriter, model_ext::ReadRange,
};
use tokio_util::sync::CancellationToken;

use super::{GcsBucket, GcsClients};
use crate::bucket::{
    RapidBucket, SnapshotStoreConfig,
    append_log::{AppendLogSnapshots, LogWriter, LogZone, Replica, ReplicaIdentity},
};

pub(crate) async fn validate(config: &SnapshotStoreConfig, clients: GcsClients) -> Result<()> {
    let Some((buckets, archive_bucket)) = config.rapid_settings() else {
        anyhow::bail!("Rapid persistence configuration required");
    };
    GcsBucket::with_clients(archive_bucket, clients.clone())?
        .require_standard()
        .await?;
    for placement in buckets {
        let bucket = clients
            .control
            .get_bucket()
            .set_name(format!("projects/_/buckets/{}", placement.bucket))
            .send()
            .await?;
        ensure!(
            bucket.storage_class == "RAPID",
            "Rapid bucket has the wrong storage class"
        );
        validate_retention(&bucket)?;
        let locations = bucket
            .custom_placement_config
            .context("Rapid bucket zone missing")?
            .data_locations;
        ensure!(
            locations.len() == 1 && locations[0].eq_ignore_ascii_case(&placement.zone),
            "Rapid bucket does not occupy its configured zone"
        );
    }
    Ok(())
}

pub(crate) fn snapshots(
    config: &SnapshotStoreConfig,
    clients: GcsClients,
    stop: CancellationToken,
    actor: Option<&crate::actor::ActorKey>,
) -> Result<AppendLogSnapshots> {
    validate_zones(config)?;
    let (_, archive_bucket) = config
        .rapid_settings()
        .context("append-log persistence configuration required")?;
    AppendLogSnapshots::with_zones(
        config,
        Arc::new(GcsBucket::with_clients(archive_bucket, clients.clone())?),
        |placement| {
            Arc::new(GcsZone {
                bucket: format!("projects/_/buckets/{}", placement.bucket),
                clients: clients.clone(),
            })
        },
        stop,
        actor,
    )
}

pub(crate) fn validate_zones(config: &SnapshotStoreConfig) -> Result<()> {
    config
        .rapid_settings()
        .map_or(&[][..], |(buckets, _)| buckets)
        .iter()
        .try_for_each(validate_zone)
}

fn validate_zone(placement: &RapidBucket) -> Result<()> {
    let (region, suffix) = placement
        .zone
        .rsplit_once('-')
        .context("Rapid placement must name a Google Cloud zone")?;
    ensure!(
        suffix.len() == 1
            && suffix.as_bytes()[0].is_ascii_lowercase()
            && region.chars().last().is_some_and(|c| c.is_ascii_digit()),
        "invalid Rapid zone"
    );
    Ok(())
}

struct GcsZone {
    bucket: String,
    clients: GcsClients,
}
struct GcsWriter(AppendableObjectWriter);

#[async_trait]
impl LogZone for GcsZone {
    fn bucket(&self) -> &str {
        &self.bucket
    }

    fn validate_identity(&self, replica: &Replica) -> Result<()> {
        ensure!(generation(replica)? > 0, "invalid GCS generation");
        Ok(())
    }

    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)> {
        let writer = self
            .clients
            .storage
            .open_appendable_object(&self.bucket, object)
            .set_if_generation_match(0)
            .send()
            .await?;
        let replica = Replica {
            bucket: self.bucket.clone(),
            object: object.into(),
            identity: ReplicaIdentity::Gcs {
                generation: writer.generation(),
            },
        };
        Ok((replica, Box::new(GcsWriter(writer))))
    }

    async fn read(&self, replica: &Replica, fence: bool) -> Result<Bytes> {
        let _transfer = self.clients.transfers.acquire().await?;
        let generation = generation(replica)?;
        let mut writer = if fence {
            Some(
                self.clients
                    .storage
                    .reopen_appendable_object(&self.bucket, &replica.object, generation)
                    .send()
                    .await?,
            )
        } else {
            None
        };
        let persisted = if let Some(writer) = &mut writer {
            Some(writer.flush().await?)
        } else {
            None
        };
        if persisted == Some(0) {
            return Ok(Bytes::new());
        }
        let descriptor = self
            .clients
            .storage
            .open_object(&self.bucket, &replica.object)
            .set_generation(generation)
            .send()
            .await?;
        let size = descriptor.object().size;
        ensure!(size >= 0, "invalid log segment size");
        if size == 0 {
            return Ok(Bytes::new());
        }
        let mut reader = descriptor
            .read_range(ReadRange::segment(0, size as u64))
            .await;
        let mut download = crate::payload::Download::new();
        let mut length = 0usize;
        while let Some(chunk) = reader.next().await {
            let chunk = chunk?;
            length += chunk.len();
            ensure!(length <= size as usize, "log read exceeded persisted size");
            download = download.append(chunk).await?;
        }
        ensure!(
            length == size as usize && persisted.is_none_or(|size| size as usize == length),
            "incomplete fenced log read"
        );
        drop(writer);
        download.finish().await
    }

    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Bytes> {
        let _transfer = self.clients.transfers.acquire().await?;
        let descriptor = self
            .clients
            .storage
            .open_object(&self.bucket, &replica.object)
            .set_generation(generation(replica)?)
            .send()
            .await?;
        let mut reader = descriptor
            .read_range(ReadRange::segment(start, length))
            .await;
        let mut download = crate::payload::Download::new();
        let mut received = 0u64;
        while let Some(chunk) = reader.next().await {
            let chunk = chunk?;
            received += chunk.len() as u64;
            ensure!(received <= length, "Rapid range exceeded requested length");
            download = download.append(chunk).await?;
        }
        ensure!(received == length, "incomplete Rapid range");
        download.finish().await
    }

    async fn delete(&self, replica: &Replica) -> Result<()> {
        match self
            .clients
            .control
            .delete_object()
            .set_bucket(&self.bucket)
            .set_object(&replica.object)
            .set_if_generation_match(generation(replica)?)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error)
                if error.http_status_code() == Some(404)
                    || error.status().is_some_and(|s| s.code.name() == "NOT_FOUND") =>
            {
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn generation(replica: &Replica) -> Result<i64> {
    match replica.identity {
        ReplicaIdentity::Gcs { generation } => Ok(generation),
        ReplicaIdentity::Express {} => anyhow::bail!("GCS log replica required"),
    }
}

#[async_trait]
impl LogWriter for GcsWriter {
    async fn append_and_flush(&mut self, bytes: Bytes) -> Result<u64> {
        self.0.append(bytes).await?;
        Ok(self.0.flush().await?.try_into()?)
    }
}

pub(crate) fn validate_retention(bucket: &google_cloud_storage::model::Bucket) -> Result<()> {
    const PREFIX: &str = "durable-actors-v3-logs-";
    if let Some(lifecycle) = &bucket.lifecycle {
        for rule in &lifecycle.rule {
            if rule
                .action
                .as_ref()
                .is_none_or(|action| action.r#type != "Delete")
            {
                continue;
            }
            let prefixes = rule
                .condition
                .as_ref()
                .map(|condition| condition.matches_prefix.as_slice())
                .unwrap_or_default();
            ensure!(
                !prefixes.is_empty()
                    && prefixes
                        .iter()
                        .all(|prefix| !PREFIX.starts_with(prefix) && !prefix.starts_with(PREFIX)),
                "Rapid log objects must be excluded from automatic lifecycle deletion"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/gcs/rapid.rs"]
mod tests;
