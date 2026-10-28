use super::socket_gateway::SocketGateway;
use crate::{
    host_leases::ActorSocketInventory,
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorInventorySnapshot, ActorResidency,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub(super) struct GatewayInventoryReader {
    actors: Arc<dyn ActorInventoryReader>,
    gateway: Arc<SocketGateway>,
}

impl GatewayInventoryReader {
    pub(super) fn new(actors: Arc<dyn ActorInventoryReader>, gateway: Arc<SocketGateway>) -> Self {
        Self { actors, gateway }
    }
}

#[async_trait]
impl ActorInventoryReader for GatewayInventoryReader {
    async fn actor_inventory(&self, project: &str) -> Result<ActorInventorySnapshot> {
        let (actors, sockets) = tokio::try_join!(
            self.actors.actor_inventory(project),
            self.gateway.inventory(project)
        )?;
        Ok(ActorInventorySnapshot {
            actors: merge_rooms(actors.actors, sockets.rooms),
            connections_complete: actors.connections_complete && sockets.complete,
        })
    }

    async fn residency_history(
        &self,
        project: &str,
        actor_name: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<crate::placement::ResidencyHistory> {
        self.actors
            .residency_history(project, actor_name, from_ms, to_ms)
            .await
    }
}

fn merge_rooms(
    actors: Vec<ActorInventory>,
    sockets: Vec<ActorSocketInventory>,
) -> Vec<ActorInventory> {
    let mut actors: std::collections::BTreeMap<_, _> = actors
        .into_iter()
        .map(|actor| (actor.actor_name.clone(), actor))
        .collect();
    let mut instances: std::collections::HashMap<_, _> = actors
        .values()
        .flat_map(|actor| {
            actor.instances.iter().enumerate().map(|(index, instance)| {
                ((actor.actor_name.clone(), instance.actor_id.clone()), index)
            })
        })
        .collect();
    for room in sockets {
        let key = (room.actor.actor_name.clone(), room.actor.actor_id.clone());
        let actor = actors
            .entry(room.actor.actor_name.clone())
            .or_insert_with(|| ActorInventory {
                actor_name: room.actor.actor_name,
                ..Default::default()
            });
        let index = *instances.entry(key).or_insert_with(|| {
            actor.dormant += 1;
            actor.instances.push(ActorInstanceOverview {
                actor_id: room.actor.actor_id,
                status: ActorResidency::Dormant,
                last_active_at_ms: None,
                connections: vec![],
                waiting: None,
            });
            actor.instances.len() - 1
        });
        actor.instances[index].connections = room
            .connections
            .into_iter()
            .map(|connection| ActorConnectionInventory {
                id: connection.id,
                metadata: connection.metadata,
            })
            .collect();
    }
    actors.into_values().collect()
}

pub(super) struct SocketInventory {
    pub rooms: Vec<ActorSocketInventory>,
    pub complete: bool,
}
