use crate::{actor::ActorKey, host::HostId};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub const MAX_HOST_LEASE_DURATION_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLease {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostLeaseRequest {
    pub id: HostId,
    pub session_id: String,
    pub route: String,
    pub duration_ms: u64,
}

impl HostLeaseRequest {
    pub fn validate_duration(&self) -> Result<()> {
        ensure!(self.duration_ms > 0, "host lease duration must be positive");
        ensure!(
            self.duration_ms <= MAX_HOST_LEASE_DURATION_MS,
            "host lease duration must not exceed {MAX_HOST_LEASE_DURATION_MS}ms"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ActivationInventory {
    pub resident: Option<bool>,
    pub waiting: Option<Vec<WaitingOperation>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActorSocketInventory {
    pub actor: ActorKey,
    pub connections: Vec<crate::actor::ActorSocketConnection>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingOperation {
    pub id: String,
    pub operation: String,
}

#[async_trait]
pub trait HostLeaseRegistry: Send + Sync {
    async fn register(
        &self,
        request: &HostLeaseRequest,
        inventory: ActivationInventory,
    ) -> Result<HostLease>;
    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()>;
}
