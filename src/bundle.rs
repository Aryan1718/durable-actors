use std::path::{Component, Path};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_lc_rs::digest::{Context as Digest, SHA256};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::bucket::{gcs::GcsBundle, s3::S3Bundle};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorRuntime {
    Typescript,
    Python,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ArtifactBundle {
    Gcs(GcsBundle),
    S3(S3Bundle),
}

impl ArtifactBundle {
    pub fn encode(&self) -> Result<String> {
        match self {
            Self::Gcs(bundle) => bundle.encode(),
            Self::S3(bundle) => bundle.encode(),
        }
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        match encoded.split_once(':').map(|(provider, _)| provider) {
            Some("gcs") => Ok(Self::Gcs(GcsBundle::decode(encoded)?)),
            Some("s3") => Ok(Self::S3(S3Bundle::decode(encoded)?)),
            _ => anyhow::bail!("unknown code artifact provider"),
        }
    }

    pub fn entrypoint(&self) -> Result<&str> {
        match self {
            Self::Gcs(bundle) => bundle.entrypoint(),
            Self::S3(bundle) => bundle.entrypoint(),
        }
    }
}

impl ActorRuntime {
    pub(crate) fn from_entrypoint(entrypoint: &str) -> Result<Self> {
        if entrypoint.ends_with(".mjs") {
            Ok(Self::Typescript)
        } else if entrypoint.ends_with(".pyz") {
            Ok(Self::Python)
        } else {
            anyhow::bail!("actor entrypoint must be a compiled .mjs or .pyz artifact")
        }
    }
}

/// Installs a deployment's code from the storage it was published to.
#[async_trait]
pub(crate) trait ArtifactInstaller: Send + Sync {
    async fn install(&self, bundle: &ArtifactBundle, root: &Path) -> Result<()>;
}

pub(crate) async fn install_verified_file(
    root: &Path,
    path: &str,
    sha256: &str,
    chunks: impl Stream<Item = Result<Bytes>>,
) -> Result<()> {
    validate_path(path)?;
    let destination = root.join(path);
    let parent = destination.parent().context("artifact parent missing")?;
    tokio::fs::create_dir_all(parent).await?;
    let temporary = tempfile::NamedTempFile::new_in(parent)?;
    let mut file = tokio::fs::File::from_std(temporary.reopen()?);
    let mut hash = Digest::new(&SHA256);
    futures_util::pin_mut!(chunks);
    while let Some(chunk) = chunks.try_next().await? {
        hash.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    ensure!(
        URL_SAFE_NO_PAD.encode(hash.finish().as_ref()) == sha256,
        "artifact digest mismatch"
    );
    drop(file);
    temporary.persist(destination)?;
    Ok(())
}

pub(crate) fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && Path::new(path)
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "artifact path must stay inside the customer directory"
    );
    Ok(())
}
