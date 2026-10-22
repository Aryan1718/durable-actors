pub(crate) mod access;
mod file;
mod gcs;
mod rapid;
mod s3;
pub(crate) use rapid::{RAPID_OPERATION_TIMEOUT, RapidSnapshots};
pub(crate) mod recovery;
mod snapshots;
pub use snapshots::{ArchiveBatchConfig, RapidBucket, SnapshotStoreConfig};
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

#[cfg(test)]
#[path = "../tests/support/bucket.rs"]
pub(crate) mod testing;
