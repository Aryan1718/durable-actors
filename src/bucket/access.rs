use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{
    Bucket, SnapshotStore, SnapshotStoreConfig,
    gcs::StorageTokens,
    s3::{AwsStorageCredentials, ExpressLogReply, ExpressLogRequest},
};
use crate::{
    actor::ActorKey,
    bundle::{ArtifactBundle, ArtifactInstaller},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HostStorageConfig {
    pub bucket: BucketLocation,
    pub persistence: SnapshotStoreConfig,
    pub region: String,
    #[serde(flatten)]
    pub access: Option<StorageAccess>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BucketLocation {
    Gcs {
        bucket: String,
        artifact_bucket: String,
    },
    S3 {
        bucket: String,
        region: String,
        artifact_bucket: String,
    },
    File {
        directory: PathBuf,
    },
}

/// The storage credentials a host holds for one actor. Variant names keep the wire format of the
/// `tokens` and `aws` fields they replaced.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum StorageAccess {
    #[serde(rename = "tokens")]
    Gcs(StorageTokens),
    #[serde(rename = "aws")]
    S3(AwsStorageCredentials),
}

impl StorageAccess {
    pub(crate) fn expires_at_ms(&self) -> u64 {
        match self {
            Self::Gcs(tokens) => tokens.expires_at_ms(),
            Self::S3(credentials) => credentials.expires_at_ms(),
        }
    }
}

/// A host's current credentials, replaced as the control plane refreshes them.
pub(crate) type SharedAccess = Arc<RwLock<Option<StorageAccess>>>;

/// What every provider needs to open a host's storage for its actor.
pub(crate) struct HostContext<'a> {
    pub persistence: &'a SnapshotStoreConfig,
    pub access: SharedAccess,
    pub stop: &'a CancellationToken,
    pub actor: &'a ActorKey,
}

/// What a host opens for its actor, whichever provider stores it.
pub(crate) struct HostBuckets {
    pub authority: Arc<dyn Bucket>,
    pub snapshots: Arc<dyn SnapshotStore>,
    pub artifacts: Option<Box<dyn ArtifactInstaller>>,
}

/// A storage provider's side of the control plane: it scopes credentials to one actor.
#[async_trait]
pub(crate) trait AccessIssuer: Send + Sync {
    fn location(&self) -> BucketLocation;
    fn validate_code(&self, code: &ArtifactBundle) -> Result<()>;
    async fn issue(
        &self,
        actor: &ActorKey,
        persistence: &SnapshotStoreConfig,
        code: Option<ArtifactBundle>,
    ) -> Result<Option<StorageAccess>>;
    async fn express_log(
        &self,
        _actor: &ActorKey,
        _persistence: &SnapshotStoreConfig,
        _request: ExpressLogRequest,
    ) -> Result<ExpressLogReply> {
        anyhow::bail!("Express logs are not configured")
    }
}

pub(crate) struct RuntimeAccess {
    issuer: Box<dyn AccessIssuer>,
    persistence: SnapshotStoreConfig,
}

impl RuntimeAccess {
    pub fn new(issuer: Box<dyn AccessIssuer>, persistence: SnapshotStoreConfig) -> Self {
        Self {
            issuer,
            persistence,
        }
    }

    pub(crate) async fn express_log(
        &self,
        actor: &ActorKey,
        request: ExpressLogRequest,
    ) -> Result<ExpressLogReply> {
        self.issuer
            .express_log(actor, &self.persistence, request)
            .await
    }

    pub(crate) fn validate_code(&self, code: &ArtifactBundle) -> Result<()> {
        self.issuer.validate_code(code)
    }

    pub async fn bootstrap(
        &self,
        region: &str,
        actor: &ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<String> {
        Ok(serde_json::to_string(&HostStorageConfig {
            bucket: self.issuer.location(),
            persistence: self.persistence.clone(),
            region: region.into(),
            access: self.issue(actor, code_snapshot).await?,
        })?)
    }

    pub async fn issue(
        &self,
        actor: &ActorKey,
        code_snapshot: Option<&str>,
    ) -> Result<Option<StorageAccess>> {
        let code = code_snapshot.map(ArtifactBundle::decode).transpose()?;
        self.issuer.issue(actor, &self.persistence, code).await
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/access.rs"]
mod tests;
