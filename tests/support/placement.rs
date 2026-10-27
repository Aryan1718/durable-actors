use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::Result;
use async_trait::async_trait;

use super::{ObjectPlacement, ObjectPlacementStore, validate_region};
use crate::{actor_state::ActorStorageKey, host_leases::HostLease};

#[derive(Default)]
pub(crate) struct LocalObjectPlacementStore {
    reads: AtomicUsize,
    placements: Mutex<HashMap<ActorStorageKey, ObjectPlacement>>,
}

#[async_trait]
impl ObjectPlacementStore for LocalObjectPlacementStore {
    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(
        Option<ObjectPlacement>,
        Option<crate::bucket::OwnershipHint>,
    )> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok((self.get(object).await?, None))
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?
            .get(object)
            .cloned())
    }
}

impl LocalObjectPlacementStore {
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    pub fn set_owner(
        &self,
        object: &ActorStorageKey,
        lease: HostLease,
        home_region: &str,
    ) -> Result<u64> {
        validate_region(home_region)?;
        let mut placements = self.placements.lock().unwrap();
        let owner_epoch = match placements.get(object) {
            Some(current) if current.lease.session_id == lease.session_id => current.owner_epoch,
            Some(current) => current.owner_epoch + 1,
            None => 1,
        };
        placements.insert(
            object.clone(),
            ObjectPlacement {
                owner: lease.id.clone(),
                lease,
                object: object.clone(),
                owner_epoch,
                home_region: home_region.into(),
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
        );
        Ok(owner_epoch)
    }
}
