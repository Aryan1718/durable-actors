use super::actor_database::ActorStorage;
use crate::{
    actor::{
        ActorKey, ActorSocketEffect, ActorSocketPublisher, ActorSocketSource, SocketLookup,
        SocketQuery, validate_socket_effects,
    },
    sockets::operations::{SocketOperation, SocketOperationReply, SocketOperations},
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub(crate) struct HostSockets {
    actor: ActorKey,
    storage: Arc<dyn ActorStorage>,
    gateway: Arc<dyn SocketOperations>,
}

impl HostSockets {
    pub(crate) fn new(
        actor: ActorKey,
        storage: Arc<dyn ActorStorage>,
        gateway: Arc<dyn SocketOperations>,
    ) -> Self {
        Self {
            actor,
            storage,
            gateway,
        }
    }
}

#[async_trait]
impl ActorSocketPublisher for HostSockets {
    async fn publish(&self, effects: Vec<ActorSocketEffect>) -> Result<()> {
        self.storage.ensure_authority()?;
        validate_socket_effects(&effects)?;
        if !effects.is_empty() {
            self.gateway
                .execute(&self.actor, SocketOperation::Publish { effects })
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ActorSocketSource for HostSockets {
    async fn query(&self, query: SocketQuery) -> Result<SocketLookup> {
        self.storage.ensure_authority()?;
        let operation = if query.count_only {
            SocketOperation::Count
        } else {
            SocketOperation::Connections { tag: query.tag }
        };
        match self.gateway.execute(&self.actor, operation).await? {
            SocketOperationReply::Connections { connections } => {
                Ok(SocketLookup::Connections(connections))
            }
            SocketOperationReply::Count { count } => Ok(SocketLookup::Count(count)),
            _ => anyhow::bail!("unexpected connection lookup reply"),
        }
    }
}
