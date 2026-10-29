use std::{path::Path, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};

use super::{GcsBucket, StorageTokens, WarmStorage, access::StorageToken};
use crate::{
    bucket::{
        BucketSnapshots, SnapshotStore,
        access::{HostBuckets, HostContext, SharedAccess, StorageAccess},
    },
    bundle::{ArtifactBundle, ArtifactInstaller},
};

pub(crate) async fn open(
    bucket: &str,
    warm: Option<WarmStorage>,
    host: HostContext<'_>,
) -> Result<HostBuckets> {
    let ownership = HostCredentials::scoped(host.access.clone(), |tokens| &tokens.ownership);
    let state = HostCredentials::scoped(host.access, |tokens| &tokens.state);
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
    let snapshots: Arc<dyn SnapshotStore> = match host.persistence.rapid_settings() {
        Some(_) => Arc::new(super::rapid::snapshots(
            host.persistence,
            state.clients(),
            host.stop.clone(),
            Some(host.actor),
        )?),
        None => Arc::new(BucketSnapshots(Arc::new(state))),
    };
    Ok(HostBuckets {
        artifacts: Some(Box::new(GcsArtifacts(authority.clients().storage))),
        authority: Arc::new(authority),
        snapshots,
    })
}

struct GcsArtifacts(google_cloud_storage::client::Storage);

#[async_trait]
impl ArtifactInstaller for GcsArtifacts {
    async fn install(&self, bundle: &ArtifactBundle, root: &Path) -> Result<()> {
        let ArtifactBundle::Gcs(bundle) = bundle else {
            anyhow::bail!("artifact provider does not match host storage");
        };
        bundle.install(root, &self.0).await
    }
}

#[derive(Clone)]
struct HostCredentials {
    access: SharedAccess,
    scope: fn(&StorageTokens) -> &StorageToken,
}

impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}

impl HostCredentials {
    fn scoped(access: SharedAccess, scope: fn(&StorageTokens) -> &StorageToken) -> Credentials {
        Self { access, scope }.into()
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
        let Some(StorageAccess::Gcs(tokens)) = stored.as_ref() else {
            return Err(google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "GCS credentials missing",
            ));
        };
        let token = (self.scope)(tokens);
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
