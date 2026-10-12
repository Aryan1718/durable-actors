use crate::state_transport::{SqlitePersisted, SqliteWrite, SqliteWriter};
use anyhow::Result;
use async_trait::async_trait;

pub struct RejectSqliteWrites;

#[async_trait]
impl SqliteWriter for RejectSqliteWrites {
    async fn persist_sqlite(&self, _: &SqliteWrite) -> Result<SqlitePersisted> {
        anyhow::bail!("unexpected immediate SQLite write in test")
    }
}
