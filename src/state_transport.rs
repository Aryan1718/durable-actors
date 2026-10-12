use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    AlreadyExists,
}

#[async_trait]
pub trait SnapshotWriter: Send + Sync {
    async fn write_snapshot(
        &self,
        plan: &crate::storage::WritePlan,
        bytes: bytes::Bytes,
    ) -> Result<StateWrite>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SqlitePersistenceMode {
    AtomicSnapshot,
    DurableWrite,
}

#[derive(Clone, Debug)]
pub(crate) struct SqliteWrite {
    pub request_id: String,
    pub actor: crate::actor::ActorKey,
    pub path: std::path::PathBuf,
    pub mode: SqlitePersistenceMode,
    pub txid: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqlitePersisted {
    pub txid: u64,
    pub state_version: Option<u64>,
}

#[async_trait]
pub(crate) trait SqliteWriter: Send + Sync {
    async fn persist_sqlite(&self, write: &SqliteWrite) -> Result<SqlitePersisted>;
}
