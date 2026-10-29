pub(crate) mod access;
mod append_log;
pub(crate) mod file;
pub(crate) mod gcs;
pub(crate) mod s3;
pub(crate) use append_log::LOG_OPERATION_TIMEOUT;
pub(crate) mod recovery;
mod snapshots;
pub use snapshots::{ArchiveBatchConfig, RapidBucket, RapidPlacement, SnapshotStoreConfig};
pub(crate) use snapshots::{BucketSnapshots, SnapshotStore};
mod runtime;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use file::FileBucket;
pub use gcs::GcsBucket;
pub(crate) use gcs::WarmStorage;
pub(crate) use runtime::ActorStateReader;
pub use runtime::{LoadedActor, OwnershipHint, RuntimeStorage, RuntimeStorageReader};
pub use s3::S3Bucket;

#[derive(Clone, Debug)]
pub struct BucketObject {
    pub revision: StorageRevision,
    pub bytes: bytes::Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StorageRevision {
    Generation(i64),
    ETag(String),
}

impl StorageRevision {
    pub(crate) fn is_valid(&self) -> bool {
        match self {
            Self::Generation(generation) => *generation > 0,
            Self::ETag(etag) => !etag.is_empty(),
        }
    }

    pub fn require_generation(&self) -> Result<i64> {
        match self {
            Self::Generation(generation) => Ok(*generation),
            Self::ETag(_) => anyhow::bail!("numeric storage generation required"),
        }
    }
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        revision: Option<StorageRevision>,
        bytes: bytes::Bytes,
    ) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

pub(crate) async fn replace(
    bucket: &dyn Bucket,
    key: &str,
    revision: Option<StorageRevision>,
    bytes: bytes::Bytes,
) -> Result<bool> {
    match bucket.compare_and_swap(key, revision, bytes.clone()).await {
        Ok(true) => Ok(true),
        result => {
            // A retry can report a failed precondition after the first request succeeded.
            if bucket
                .get(key)
                .await?
                .is_some_and(|object| object.bytes == bytes)
            {
                return Ok(true);
            }
            result
        }
    }
}

/// Opens the storage connections a spare keeps ready for its future actor. Only GCS keeps any;
/// other providers connect at assignment.
pub(crate) async fn warm_storage(provider: &str) -> Result<Option<WarmStorage>> {
    match provider {
        "gcs" => {
            let storage = WarmStorage::new().await?;
            storage.preconnect().await;
            Ok(Some(storage))
        }
        "s3" => Ok(None),
        _ => anyhow::bail!("unsupported storage provider {provider:?}"),
    }
}

/// Whether a cloud client reads host credentials from this environment variable; actor code must
/// not inherit it.
pub(crate) fn is_credential_variable(name: &str) -> bool {
    gcs::is_credential_variable(name) || s3::is_credential_variable(name)
}

/// Variables set for actor code so its cloud clients cannot fall back to the node's credentials.
pub(crate) fn executor_environment() -> impl Iterator<Item = (&'static str, &'static str)> {
    s3::EXECUTOR_ENVIRONMENT.iter().copied()
}

#[cfg(test)]
#[path = "../tests/support/bucket.rs"]
pub(crate) mod testing;
