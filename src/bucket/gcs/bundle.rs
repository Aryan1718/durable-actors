use std::path::Path;

use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use google_cloud_storage::client::Storage;
use serde::{Deserialize, Serialize};

use crate::bundle::{install_verified_file, validate_path};

const MAX_CONCURRENT_DOWNLOADS: usize = 4;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GcsBundle {
    pub bucket: String,
    pub files: Vec<GcsBundleFile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GcsBundleFile {
    pub path: String,
    pub object: String,
    pub generation: i64,
    pub sha256: String,
}

impl GcsBundle {
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "gcs:{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let value: Self = serde_json::from_slice(
            &URL_SAFE_NO_PAD.decode(
                encoded
                    .strip_prefix("gcs:")
                    .context("GCS artifact reference required")?,
            )?,
        )?;
        value.validate()?;
        Ok(value)
    }

    pub async fn install(&self, root: &Path, storage: &Storage) -> Result<()> {
        self.validate()?;
        let bucket = format!("projects/_/buckets/{}", self.bucket);
        futures_util::stream::iter(self.files.iter().map(Ok))
            .try_for_each_concurrent(MAX_CONCURRENT_DOWNLOADS, |file| async {
                let response = storage
                    .read_object(&bucket, &file.object)
                    .set_generation(file.generation)
                    .send()
                    .await?;
                let chunks = futures_util::stream::try_unfold(response, |mut response| async {
                    match response.next().await {
                        Some(chunk) => Ok(Some((chunk?, response))),
                        None => anyhow::Ok(None),
                    }
                });
                install_file(root, file, chunks).await
            })
            .await
    }

    pub fn entrypoint(&self) -> Result<&str> {
        let names: Vec<_> = self
            .files
            .iter()
            .filter(|file| matches!(file.path.as_str(), "actors.mjs" | "actors.pyz"))
            .collect();
        ensure!(
            names.len() == 1,
            "actor artifact requires exactly one compiled entrypoint"
        );
        Ok(&names[0].path)
    }

    fn validate(&self) -> Result<()> {
        crate::storage::validate_bucket(&self.bucket)?;
        let mut paths = std::collections::HashSet::new();
        for file in &self.files {
            validate_path(&file.path)?;
            ensure!(paths.insert(&file.path), "duplicate artifact path");
            ensure!(
                file.generation > 0
                    && !file.object.is_empty()
                    && URL_SAFE_NO_PAD.decode(&file.sha256)?.len() == 32,
                "invalid artifact identity"
            );
        }
        self.entrypoint()?;
        Ok(())
    }
}

async fn install_file(
    root: &Path,
    artifact: &GcsBundleFile,
    chunks: impl Stream<Item = Result<Bytes>>,
) -> Result<()> {
    install_verified_file(root, &artifact.path, &artifact.sha256, chunks).await
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/gcs/bundle.rs"]
mod tests;
