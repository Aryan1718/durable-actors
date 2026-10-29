use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::stream;
use serde::{Deserialize, Serialize};

use crate::bucket::{BucketObject, StorageRevision};

const INSTALL_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct S3Bundle {
    pub bucket: String,
    pub region: String,
    pub files: Vec<S3BundleFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct S3BundleFile {
    pub path: String,
    pub object: String,
    pub etag: String,
    pub sha256: String,
}

impl S3Bundle {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "s3:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let encoded = encoded
            .strip_prefix("s3:")
            .context("S3 artifact reference required")?;
        let bundle: Self = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?;
        bundle.validate()?;
        Ok(bundle)
    }

    pub fn entrypoint(&self) -> Result<&str> {
        let mut entries = self
            .files
            .iter()
            .filter(|file| matches!(file.path.as_str(), "actors.mjs" | "actors.pyz"));
        let first = entries
            .next()
            .context("actor artifact requires exactly one compiled entrypoint")?;
        ensure!(
            entries.next().is_none(),
            "actor artifact requires exactly one compiled entrypoint"
        );
        Ok(&first.path)
    }

    pub async fn install(&self, root: &Path, reader: &dyn ArtifactReader) -> Result<()> {
        self.validate()?;
        tokio::time::timeout(INSTALL_TIMEOUT, self.install_staged(root, reader))
            .await
            .context("artifact installation timed out")?
    }

    async fn install_staged(&self, root: &Path, reader: &dyn ArtifactReader) -> Result<()> {
        tokio::fs::create_dir_all(root).await?;
        self.check_destinations(root).await?;
        let staged = tempfile::Builder::new()
            .prefix(".s3-artifacts-")
            .tempdir_in(root)?;
        for file in &self.files {
            self.download_file(staged.path(), file, reader).await?;
        }
        self.publish(root, staged.path())
    }

    async fn check_destinations(&self, root: &Path) -> Result<()> {
        let metadata = tokio::fs::symlink_metadata(root).await?;
        ensure!(
            metadata.is_dir() && !metadata.is_symlink(),
            "artifact root must be a directory"
        );
        for file in &self.files {
            check_parents(root, &file.path)?;
            ensure!(
                tokio::fs::symlink_metadata(root.join(&file.path))
                    .await
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
                "artifact destination must not already exist"
            );
        }
        Ok(())
    }

    async fn download_file(
        &self,
        root: &Path,
        file: &S3BundleFile,
        reader: &dyn ArtifactReader,
    ) -> Result<()> {
        let object = reader
            .download(&self.bucket, &self.region, &file.object)
            .await
            .map_err(|_| anyhow::anyhow!("artifact download failed"))?
            .context("artifact object missing")?;
        ensure!(
            object.revision == StorageRevision::ETag(file.etag.clone()),
            "artifact revision mismatch"
        );
        let chunks = stream::iter(
            object
                .bytes
                .chunks(crate::payload::IO_BUFFER_BYTES)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk))),
        );
        crate::bundle::install_verified_file(root, &file.path, &file.sha256, chunks).await
    }

    fn publish(&self, root: &Path, staged: &Path) -> Result<()> {
        // The host owns this directory exclusively; commit has no cancellation points.
        let mut published = PublishedFiles(Vec::new());
        for file in &self.files {
            let destination = root.join(&file.path);
            create_parents(root, &file.path)?;
            std::fs::hard_link(staged.join(&file.path), &destination)?;
            published.0.push(destination);
        }
        published.0.clear();
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        validate_location(&self.bucket, &self.region)?;
        self.entrypoint()?;
        let prefix = deployment_prefix(&self.files[0].object)?;
        let mut paths = std::collections::HashSet::new();
        for file in &self.files {
            file.validate(&prefix)?;
            ensure!(paths.insert(&file.path), "duplicate artifact path");
        }
        Ok(())
    }
}

impl S3BundleFile {
    fn validate(&self, prefix: &str) -> Result<()> {
        crate::bundle::validate_path(&self.path)?;
        ensure!(
            !self.path.contains(['\\', ':'])
                && self
                    .path
                    .split('/')
                    .all(|part| !part.is_empty() && !matches!(part, "." | "..")),
            "invalid S3 artifact path"
        );
        ensure!(
            self.object == format!("{prefix}{}", self.path)
                && self.object.len() <= 1024
                && !self.object.contains(['*', '?'])
                && !self.object.contains("${")
                && !self.object.chars().any(char::is_control),
            "invalid S3 artifact location"
        );
        ensure!(
            !self.etag.is_empty()
                && !self.etag.chars().any(char::is_control)
                && URL_SAFE_NO_PAD.decode(&self.sha256)?.len() == 32,
            "invalid S3 artifact identity"
        );
        Ok(())
    }
}

#[async_trait]
pub(crate) trait ArtifactReader: Send + Sync {
    async fn download(
        &self,
        bucket: &str,
        region: &str,
        object: &str,
    ) -> Result<Option<BucketObject>>;
}

fn validate_location(bucket: &str, region: &str) -> Result<()> {
    ensure!(
        (3..=63).contains(&bucket.len())
            && bucket.bytes().all(|byte| byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'.'))
            && bucket.as_bytes()[0].is_ascii_alphanumeric()
            && bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric(),
        "invalid S3 artifact bucket"
    );
    ensure!(
        !region.is_empty()
            && region
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && !region.starts_with('-')
            && !region.ends_with('-'),
        "invalid S3 artifact region"
    );
    Ok(())
}

fn deployment_prefix(object: &str) -> Result<String> {
    let root = format!("{}artifacts/", crate::storage_paths::ROOT);
    let relative = object
        .strip_prefix(&root)
        .context("invalid artifact root")?;
    let (deployment, _) = relative
        .split_once('/')
        .context("artifact deployment missing")?;
    uuid::Uuid::parse_str(deployment).context("invalid artifact deployment")?;
    Ok(format!("{root}{deployment}/"))
}

fn create_parents(root: &Path, path: &str) -> Result<()> {
    let mut directory = root.to_owned();
    for component in Path::new(path)
        .parent()
        .context("artifact parent missing")?
        .components()
    {
        directory.push(component);
        match std::fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = std::fs::symlink_metadata(&directory)?;
                ensure!(
                    metadata.is_dir() && !metadata.is_symlink(),
                    "artifact parent must be a directory"
                );
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn check_parents(root: &Path, path: &str) -> Result<()> {
    let mut directory = root.to_owned();
    for component in Path::new(path)
        .parent()
        .context("artifact parent missing")?
        .components()
    {
        directory.push(component);
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !metadata.is_symlink(),
                "artifact parent must be a directory"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

struct PublishedFiles(Vec<PathBuf>);

impl Drop for PublishedFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/s3/bundle.rs"]
mod tests;
