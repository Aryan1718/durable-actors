use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_database::{ActorActivation, ActorStorage},
};
use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, BucketSnapshots, GcsBucket, RapidSnapshots, RuntimeStorage, SnapshotStore,
        WarmStorage,
        access::{HostStorageConfig, StorageAccess, StorageToken, StorageTokens},
    },
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
    storage::WritePlan,
};

pub(crate) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    artifacts: Option<ArtifactStorage>,
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
        let access = Arc::new(std::sync::RwLock::new(StorageAccess {
            tokens: config.tokens,
            aws: config.aws,
        }));
        start_access_refresh(access.clone(), client.clone(), stop.clone());
        let credentials = HostCredentials::new(access.clone());
        let (runtime, artifacts) = match config.bucket {
            crate::bucket::access::BucketLocation::Gcs { bucket, .. } => {
                let (runtime, objects) = Self::open_gcs(
                    &bucket,
                    config.persistence,
                    credentials,
                    warm,
                    &stop,
                    &actor,
                )
                .await?;
                (runtime, objects.map(ArtifactStorage::Gcs))
            }
            crate::bucket::access::BucketLocation::S3 {
                bucket,
                region,
                artifact_bucket,
            } => {
                ensure!(
                    config.persistence.rapid_settings().is_none(),
                    "Rapid persistence requires GCS"
                );
                ensure!(warm.is_none(), "S3 hosts cannot reuse GCS storage clients");
                let ownership = s3_bucket(
                    &bucket,
                    &region,
                    AwsHostCredentials::new(access.clone(), AwsScope::Ownership),
                )
                .await?;
                let state = s3_bucket(
                    &bucket,
                    &region,
                    AwsHostCredentials::new(access.clone(), AwsScope::State),
                )
                .await?;
                let snapshots: Arc<dyn SnapshotStore> = Arc::new(BucketSnapshots(Arc::new(state)));
                (
                    RuntimeStorage::new(Arc::new(ownership), Arc::new(SystemClock))?
                        .with_persistence(config.persistence, snapshots)?,
                    Some(ArtifactStorage::S3(S3ArtifactReader {
                        bucket: artifact_bucket,
                        region,
                        access,
                    })),
                )
            }
            crate::bucket::access::BucketLocation::File { directory } => {
                ensure!(
                    config.persistence.rapid_settings().is_none(),
                    "Rapid persistence requires GCS"
                );
                let authority = Arc::new(crate::bucket::FileBucket::new(directory)?);
                (RuntimeStorage::new(authority, Arc::new(SystemClock))?, None)
            }
        };
        Ok(Self {
            artifacts,
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

    async fn open_gcs(
        bucket: &str,
        persistence: crate::bucket::SnapshotStoreConfig,
        credentials: HostCredentials,
        warm: Option<WarmStorage>,
        stop: &CancellationToken,
        actor: &ActorKey,
    ) -> Result<(
        RuntimeStorage,
        Option<google_cloud_storage::client::Storage>,
    )> {
        let ownership = credentials.scoped(|tokens| &tokens.ownership);
        let state = credentials.scoped(|tokens| &tokens.state);
        let (authority, state) = match warm {
            Some(warm) => (
                warm.ownership.bind(bucket, ownership)?,
                warm.state.bind(bucket, state)?,
            ),
            None => tokio::try_join!(
                GcsBucket::with_credentials(bucket, ownership),
                GcsBucket::with_credentials(bucket, state),
            )?,
        };
        let snapshots: Arc<dyn SnapshotStore> = match persistence.rapid_settings() {
            Some(_) => Arc::new(RapidSnapshots::gcs(
                &persistence,
                state.clients(),
                stop.clone(),
                Some(actor),
            )?),
            None => Arc::new(BucketSnapshots(Arc::new(state))),
        };
        let objects = authority.clients().storage;
        let runtime = RuntimeStorage::new(Arc::new(authority), Arc::new(SystemClock))?
            .with_persistence(persistence, snapshots)?;
        Ok((runtime, Some(objects)))
    }

    pub(crate) async fn install_code(
        &self,
        artifact: &crate::bundle::ArtifactBundle,
    ) -> Result<()> {
        match (artifact, self.artifacts.as_ref()) {
            (crate::bundle::ArtifactBundle::Gcs(bundle), Some(ArtifactStorage::Gcs(storage))) => {
                bundle
                    .install(std::path::Path::new("/customer"), storage)
                    .await
            }
            (crate::bundle::ArtifactBundle::S3(bundle), Some(ArtifactStorage::S3(reader))) => {
                bundle
                    .install(std::path::Path::new("/customer"), reader)
                    .await
            }
            _ => anyhow::bail!("artifact provider does not match host storage"),
        }
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

enum ArtifactStorage {
    Gcs(google_cloud_storage::client::Storage),
    S3(S3ArtifactReader),
}

struct S3ArtifactReader {
    bucket: String,
    region: String,
    access: Arc<std::sync::RwLock<StorageAccess>>,
}

#[async_trait]
impl crate::bundle::s3::ArtifactReader for S3ArtifactReader {
    async fn download(
        &self,
        bucket: &str,
        region: &str,
        object: &str,
    ) -> Result<Option<crate::bucket::BucketObject>> {
        ensure!(
            self.bucket == bucket && self.region == region,
            "artifact storage location mismatch"
        );
        s3_bucket(
            bucket,
            region,
            AwsHostCredentials::new(self.access.clone(), AwsScope::Artifact(object.into())),
        )
        .await?
        .get(object)
        .await
    }
}

async fn s3_bucket(
    bucket: &str,
    region: &str,
    credentials: AwsHostCredentials,
) -> Result<crate::bucket::S3Bucket> {
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_owned()))
        .credentials_provider(credentials)
        .load()
        .await;
    crate::bucket::S3Bucket::new(bucket, aws_sdk_s3::Client::new(&sdk))
}

#[derive(Clone, Debug)]
enum AwsScope {
    Ownership,
    State,
    Artifact(String),
}

#[derive(Clone, Debug)]
struct AwsHostCredentials {
    access: Arc<std::sync::RwLock<StorageAccess>>,
    scope: AwsScope,
}

impl AwsHostCredentials {
    fn new(access: Arc<std::sync::RwLock<StorageAccess>>, scope: AwsScope) -> Self {
        Self { access, scope }
    }

    async fn load(&self) -> aws_credential_types::provider::Result {
        let access = self.access.read().unwrap();
        let aws = access.aws.as_ref().ok_or_else(credentials_missing)?;
        let credential = match &self.scope {
            AwsScope::Ownership => &aws.ownership,
            AwsScope::State => &aws.state,
            AwsScope::Artifact(key) => {
                &aws.artifacts
                    .iter()
                    .find(|batch| batch.keys.contains(key))
                    .ok_or_else(credentials_missing)?
                    .credentials
            }
        };
        Ok(aws_credential_types::Credentials::new(
            credential.access_key_id.clone(),
            credential.secret_access_key.clone(),
            Some(credential.session_token.clone()),
            Some(std::time::UNIX_EPOCH + Duration::from_millis(credential.expires_at_ms)),
            "actor-scoped-sts",
        ))
    }
}

impl aws_credential_types::provider::ProvideCredentials for AwsHostCredentials {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(self.load())
    }
}

fn credentials_missing() -> aws_credential_types::provider::error::CredentialsError {
    aws_credential_types::provider::error::CredentialsError::not_loaded(std::io::Error::other(
        "actor-scoped AWS credentials missing",
    ))
}

fn start_access_refresh(
    access: Arc<std::sync::RwLock<StorageAccess>>,
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
                .expires_at_ms()
                .unwrap_or(host_expiry)
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

impl StorageAccess {
    fn expires_at_ms(&self) -> Option<u64> {
        let gcs = self.tokens.as_ref().map(|tokens| {
            tokens
                .ownership
                .expires_at_ms
                .min(tokens.state.expires_at_ms)
        });
        let aws = self.aws.as_ref().map(|credentials| {
            credentials.artifacts.iter().fold(
                credentials
                    .ownership
                    .expires_at_ms
                    .min(credentials.state.expires_at_ms),
                |expires, artifact| expires.min(artifact.credentials.expires_at_ms),
            )
        });
        gcs.into_iter().chain(aws).min()
    }
}

#[derive(Clone)]
struct HostCredentials {
    access: Arc<std::sync::RwLock<StorageAccess>>,
    scope: fn(&StorageTokens) -> &StorageToken,
}
impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}
impl HostCredentials {
    fn new(access: Arc<std::sync::RwLock<StorageAccess>>) -> Self {
        Self {
            access,
            scope: |tokens| &tokens.ownership,
        }
    }

    fn scoped(&self, scope: fn(&StorageTokens) -> &StorageToken) -> Credentials {
        Self {
            access: self.access.clone(),
            scope,
        }
        .into()
    }
}
impl CredentialsProvider for HostCredentials {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let stored = self.access.read().unwrap();
        let token = stored.tokens.as_ref().map(self.scope).ok_or_else(|| {
            google_cloud_auth::errors::CredentialsError::from_msg(false, "GCS credentials missing")
        })?;
        let mut headers = axum::http::HeaderMap::new();
        let mut value: axum::http::HeaderValue = format!("Bearer {}", token.access_token)
            .parse()
            .map_err(|_| {
            google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "invalid storage credential",
            )
        })?;
        value.set_sensitive(true);
        headers.insert(axum::http::header::AUTHORIZATION, value);
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }
    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/storage.rs"]
mod tests;
