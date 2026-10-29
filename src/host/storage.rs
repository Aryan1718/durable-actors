use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_database::{ActorActivation, ActorStorage},
};
use crate::{
    actor::ActorKey,
    bucket::{
        RuntimeStorage, WarmStorage,
        access::{BucketLocation, HostBuckets, HostContext, HostStorageConfig, SharedAccess},
    },
    bundle::ArtifactInstaller,
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
    storage::WritePlan,
};

pub(crate) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    artifacts: Option<Box<dyn ArtifactInstaller>>,
    pub(super) stop: CancellationToken,
    observer: Arc<ControlPlaneClient>,
    host: HostId,
    session: String,
    region: String,
    actor: ActorKey,
    new_actor: bool,
    owner_hint: Option<crate::bucket::OwnershipHint>,
    activation: Mutex<Option<ActorActivation>>,
    checkpoint: Mutex<Option<crate::bucket::recovery::PrefetchedCheckpoint>>,
    fence: Mutex<LeaseFence>,
    lease: Mutex<Option<HostLease>>,
    renewal: tokio::sync::Mutex<()>,
}

impl HostStorage {
    pub async fn new(
        config: HostStorageConfig,
        host: HostId,
        session: String,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
        warm: Option<WarmStorage>,
        actor: ActorKey,
    ) -> Result<Self> {
        let access: SharedAccess = Arc::new(std::sync::RwLock::new(config.access));
        start_access_refresh(access.clone(), client.clone(), stop.clone());
        let buckets = open_buckets(
            config.bucket,
            warm,
            client.clone(),
            HostContext {
                persistence: &config.persistence,
                access,
                stop: &stop,
                actor: &actor,
            },
        )
        .await?;
        let runtime = RuntimeStorage::new(buckets.authority, Arc::new(SystemClock))?
            .with_persistence(config.persistence, buckets.snapshots)?;
        Ok(Self {
            artifacts: buckets.artifacts,
            observer: client,
            stop,
            runtime: Arc::new(runtime),
            host,
            session,
            region: config.region,
            actor,
            new_actor: false,
            owner_hint: None,
            activation: Mutex::new(None),
            checkpoint: Mutex::new(None),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
            renewal: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) async fn install_code(
        &self,
        artifact: &crate::bundle::ArtifactBundle,
    ) -> Result<()> {
        self.artifacts
            .as_ref()
            .context("artifact provider does not match host storage")?
            .install(artifact, std::path::Path::new("/customer"))
            .await
    }

    pub(crate) fn with_activation(
        mut self,
        new_actor: bool,
        owner_hint: Option<crate::bucket::OwnershipHint>,
    ) -> Self {
        self.new_actor = new_actor;
        self.owner_hint = owner_hint;
        self
    }

    pub(super) fn current_lease(&self) -> Result<HostLease> {
        self.ensure_authority()?;
        self.lease
            .lock()
            .unwrap()
            .clone()
            .context("activation lease missing")
    }

    pub(super) async fn get_alarm(&self) -> Result<Option<i64>> {
        self.ensure_authority()?;
        let timestamp_ms = self.observer.get_alarm().await?;
        self.ensure_authority()?;
        Ok(timestamp_ms)
    }

    pub(super) async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()> {
        self.ensure_authority()?;
        self.observer.update_alarm(timestamp_ms).await?;
        self.ensure_authority()
    }

    pub(super) async fn finish(&self, deadline: tokio::time::Instant) -> Result<()> {
        let started = Instant::now();
        let actor = &self.actor;
        let drain_deadline = deadline - Duration::from_secs(3);
        let completed = tokio::time::timeout_at(
            drain_deadline,
            self.runtime
                .drain_activation(actor, &self.host, &self.session, drain_deadline),
        )
        .await
        .context("snapshot drain timed out")
        .and_then(|result| result);
        let checkpoint = match completed {
            Ok(checkpoint) => Some(checkpoint),
            Err(error) => {
                tracing::warn!(%error, "clean session shutdown deferred to recovery");
                None
            }
        };
        let sealed = checkpoint.is_some();
        let storage_ms = started.elapsed().as_secs_f64() * 1000.0;
        let release_started = Instant::now();
        self.fence.lock().unwrap().fenced = true;
        // Ownership CAS prevents an in-flight renewal from reviving the released lease.
        let released = tokio::time::timeout_at(
            deadline,
            self.runtime
                .release_with_checkpoint(actor, &self.host, &self.session, checkpoint),
        )
        .await
        .context("ownership release timed out")
        .and_then(|result| result);
        tracing::info!(
            event = "host_storage_shutdown",
            sealed = sealed && released.is_ok(),
            released = released.is_ok(),
            error = released.as_ref().err().map(|error| format!("{error:#}")),
            storage_ms,
            release_ms = release_started.elapsed().as_secs_f64() * 1000.0,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        released
    }
}

#[async_trait]
impl ActorStorage for HostStorage {
    async fn get_alarm(&self) -> Result<Option<i64>> {
        HostStorage::get_alarm(self).await
    }

    async fn update_alarm(&self, timestamp_ms: Option<i64>) -> Result<()> {
        HostStorage::update_alarm(self, timestamp_ms).await
    }

    fn ensure_authority(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled(),
            "host session is permanently fenced"
        );
        self.fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .check(Instant::now())
    }

    async fn acquire_actor(&self) -> Result<ActorActivation> {
        self.ensure_authority()?;
        if let Some(activation) = self.activation.lock().unwrap().take() {
            return Ok(activation);
        }
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .activate_actor(&self.actor, &lease, &self.region)
            .await?;
        self.ensure_authority()?;
        Ok(ActorActivation {
            owner_epoch: loaded.placement.owner_epoch,
            state_version: loaded.placement.state_version,
            state: loaded.state,
        })
    }

    async fn load_actor_state(&self, epoch: u64) -> Result<(u64, bytes::Bytes)> {
        self.ensure_authority()?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .load_owned_actor(&self.actor, &lease, epoch)
            .await?;
        self.ensure_authority()?;
        Ok((
            loaded.placement.state_version,
            loaded.state.unwrap_or_default(),
        ))
    }

    async fn read_sqlite(&self, bytes: &[u8]) -> Result<crate::state_log::SqliteSnapshot> {
        self.ensure_authority()?;
        let snapshot = crate::state_log::StateSnapshot::decode(bytes)?;
        let object = crate::storage::snapshot_object_name(
            &self.actor,
            snapshot.state_version,
            &format!("{:032x}", snapshot.owner_epoch),
        )?;
        let checkpoint = self.checkpoint.lock().unwrap().take();
        self.runtime
            .read_sqlite_prefetched(&object, bytes::Bytes::copy_from_slice(bytes), checkpoint)
            .await
    }

    async fn prepare_state_write(&self, epoch: u64, version: u64) -> Result<WritePlan> {
        self.ensure_authority()?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let ticket = self
            .runtime
            .prepare_actor_write(
                &self.actor,
                &lease,
                epoch,
                version.checked_add(1).context("state version overflow")?,
            )
            .await?;
        self.ensure_authority()?;
        Ok(ticket)
    }
}

#[async_trait]
impl HostLeaseRegistry for HostStorage {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        ensure!(
            request.id == self.host && request.session_id == self.session,
            "host lease scope mismatch"
        );
        let _renewal = self.renewal.lock().await;
        let started = Instant::now();
        self.fence.lock().unwrap().begin(started)?;
        let actor = &self.actor;
        let first = self.lease.lock().unwrap().is_none();
        let lease = if first {
            let loaded = self
                .runtime
                .register_activation(
                    actor,
                    request,
                    &self.region,
                    self.new_actor,
                    self.owner_hint.as_ref(),
                )
                .await?;
            let lease = loaded.placement.lease;
            *self.checkpoint.lock().unwrap() = loaded.checkpoint;
            *self.activation.lock().unwrap() = Some(ActorActivation {
                owner_epoch: loaded.placement.owner_epoch,
                state_version: loaded.placement.state_version,
                state: loaded.state,
            });
            lease
        } else {
            self.runtime.renew_activation(actor, request).await?
        };
        self.fence.lock().unwrap().confirm(
            started,
            Duration::from_millis(request.duration_ms),
            Instant::now(),
        )?;
        self.lease.lock().unwrap().replace(lease.clone());
        Ok(lease)
    }
    async fn unregister(&self, host: &HostId, session: &str) -> Result<()> {
        ensure!(
            *host == self.host && session == self.session,
            "host lease scope mismatch"
        );
        self.finish(tokio::time::Instant::now() + Duration::from_secs(25))
            .await
    }
}

async fn open_buckets(
    location: BucketLocation,
    warm: Option<WarmStorage>,
    client: Arc<ControlPlaneClient>,
    host: HostContext<'_>,
) -> Result<HostBuckets> {
    match location {
        BucketLocation::Gcs { bucket, .. } => {
            crate::bucket::gcs::host::open(&bucket, warm, host).await
        }
        BucketLocation::S3 {
            bucket,
            region,
            artifact_bucket,
        } => {
            ensure!(warm.is_none(), "S3 hosts cannot reuse GCS storage clients");
            crate::bucket::s3::host::open(&bucket, &region, artifact_bucket, client, host).await
        }
        BucketLocation::File { directory } => {
            crate::bucket::file::open_host(directory, host.persistence)
        }
    }
}

fn start_access_refresh(
    access: SharedAccess,
    client: Arc<ControlPlaneClient>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let now = SystemClock.now_ms().unwrap_or(u64::MAX);
            let host_expiry = client.token_expires_at_ms().unwrap_or(now);
            let expires = access
                .read()
                .unwrap()
                .as_ref()
                .map_or(host_expiry, |access| access.expires_at_ms())
                .min(host_expiry);
            let delay = Duration::from_millis(
                expires
                    .saturating_sub(now.saturating_add(30_000))
                    .clamp(1_000, 600_000),
            );
            tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(delay) => {} }
            match client.refresh_storage_access().await {
                Ok(refreshed) => *access.write().unwrap() = refreshed,
                Err(error) => tracing::warn!(%error, "could not refresh host storage credentials"),
            }
        }
    });
}

#[cfg(test)]
#[path = "../../tests/unit/host/storage.rs"]
mod tests;
