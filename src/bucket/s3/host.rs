use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use async_trait::async_trait;

use super::{ExpressLogReply, ExpressLogRequest, ExpressLogs, S3Bucket, bundle::ArtifactReader};
use crate::{
    bucket::{
        Bucket, BucketObject, BucketSnapshots, SnapshotStore,
        access::{HostBuckets, HostContext, SharedAccess, StorageAccess},
    },
    bundle::{ArtifactBundle, ArtifactInstaller},
    control_plane::ControlPlaneClient,
};

pub(crate) async fn open(
    bucket: &str,
    region: &str,
    artifact_bucket: String,
    control_plane: Arc<ControlPlaneClient>,
    host: HostContext<'_>,
) -> Result<HostBuckets> {
    let ownership = s3_bucket(
        bucket,
        region,
        AwsHostCredentials::new(host.access.clone(), AwsScope::Ownership),
    )
    .await?;
    let rapid = host.persistence.rapid_settings();
    let state = Arc::new(
        s3_bucket(
            rapid.map_or(bucket, |(_, archive)| archive),
            region,
            AwsHostCredentials::new(host.access.clone(), AwsScope::State),
        )
        .await?,
    );
    let snapshots: Arc<dyn SnapshotStore> = match rapid {
        Some(_) => Arc::new(super::express::snapshots(
            host.persistence,
            state,
            Arc::new(ControlPlaneExpressLogs(control_plane)),
            host.stop.clone(),
            Some(host.actor),
        )?),
        None => Arc::new(BucketSnapshots(state)),
    };
    Ok(HostBuckets {
        authority: Arc::new(ownership),
        snapshots,
        artifacts: Some(Box::new(S3ArtifactReader {
            bucket: artifact_bucket,
            region: region.into(),
            access: host.access,
        })),
    })
}

struct ControlPlaneExpressLogs(Arc<ControlPlaneClient>);

#[async_trait]
impl ExpressLogs for ControlPlaneExpressLogs {
    async fn handle(&self, request: ExpressLogRequest) -> Result<ExpressLogReply> {
        self.0.express_log(request).await
    }
}

struct S3ArtifactReader {
    bucket: String,
    region: String,
    access: SharedAccess,
}

#[async_trait]
impl ArtifactInstaller for S3ArtifactReader {
    async fn install(&self, bundle: &ArtifactBundle, root: &Path) -> Result<()> {
        let ArtifactBundle::S3(bundle) = bundle else {
            anyhow::bail!("artifact provider does not match host storage");
        };
        bundle.install(root, self).await
    }
}

#[async_trait]
impl ArtifactReader for S3ArtifactReader {
    async fn download(
        &self,
        bucket: &str,
        region: &str,
        object: &str,
    ) -> Result<Option<BucketObject>> {
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
) -> Result<S3Bucket> {
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_owned()))
        .credentials_provider(credentials)
        .load()
        .await;
    S3Bucket::new(bucket, aws_sdk_s3::Client::new(&sdk))
}

#[derive(Clone, Debug)]
enum AwsScope {
    Ownership,
    State,
    Artifact(String),
}

#[derive(Clone, Debug)]
struct AwsHostCredentials {
    access: SharedAccess,
    scope: AwsScope,
}

impl AwsHostCredentials {
    fn new(access: SharedAccess, scope: AwsScope) -> Self {
        Self { access, scope }
    }

    async fn load(&self) -> aws_credential_types::provider::Result {
        let access = self.access.read().unwrap();
        let Some(StorageAccess::S3(aws)) = access.as_ref() else {
            return Err(credentials_missing());
        };
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

#[cfg(test)]
#[path = "../../../tests/unit/bucket/s3/host.rs"]
mod tests;
