use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;

use super::{
    Activation, AdmissionRejected, LAUNCH_LEASE_MS, OrganizationLimits, REPORT_LEASE_MS,
    ReservationStore,
};
use crate::{
    actor_lifecycle::{LifecycleKind, LifecycleReport},
    clock::Clock,
};

pub(in crate::control_plane) struct MemoryReservations {
    organizations: Mutex<HashMap<String, Usage>>,
    clock: Arc<dyn Clock>,
    retention: Duration,
}

impl MemoryReservations {
    pub fn new(clock: Arc<dyn Clock>, retention: Duration) -> Self {
        Self {
            organizations: Mutex::new(HashMap::new()),
            clock,
            retention,
        }
    }
}

#[async_trait]
impl ReservationStore for MemoryReservations {
    async fn reserve(&self, activation: &Activation, limits: OrganizationLimits) -> Result<()> {
        let now = self.clock.now_ms()?;
        let mut organizations = self.organizations.lock().unwrap();
        let usage = organizations
            .entry(activation.organization.clone())
            .or_default();
        usage.prune(now);
        if let Some(state) = usage.sessions.get(activation) {
            ensure!(!state.stopped, "actor activation has already stopped");
            return Ok(());
        }
        ensure!(
            usage.active.len() < limits.max_active_actors as usize,
            AdmissionRejected::Quota {
                limit: limits.max_active_actors
            }
        );
        usage.consume(limits, now)?;
        usage
            .active
            .insert(activation.clone(), now.saturating_add(LAUNCH_LEASE_MS));
        usage.sessions.insert(
            activation.clone(),
            Session {
                forget_at: self.forget_at(now),
                ..Session::default()
            },
        );
        Ok(())
    }

    async fn release(&self, activation: &Activation) -> Result<()> {
        let mut organizations = self.organizations.lock().unwrap();
        if let Some(usage) = organizations.get_mut(&activation.organization) {
            usage.active.remove(activation);
            usage.sessions.remove(activation);
        }
        Ok(())
    }

    async fn observe(&self, activation: &Activation, report: &LifecycleReport) -> Result<()> {
        let now = self.clock.now_ms()?;
        let mut organizations = self.organizations.lock().unwrap();
        let usage = organizations
            .entry(activation.organization.clone())
            .or_default();
        usage.prune(now);
        let state = usage.sessions.entry(activation.clone()).or_default();
        if state.stopped
            || (state.epoch > 0 && state.epoch != report.owner_epoch)
            || report.sequence <= state.sequence
        {
            return Ok(());
        }
        state.epoch = report.owner_epoch;
        state.sequence = report.sequence;
        state.stopped = report.kind == LifecycleKind::Stopped;
        state.forget_at = self.forget_at(now);
        if state.stopped {
            usage.active.remove(activation);
        } else {
            usage
                .active
                .insert(activation.clone(), now.saturating_add(REPORT_LEASE_MS));
        }
        Ok(())
    }
}

impl MemoryReservations {
    fn forget_at(&self, now: u64) -> u64 {
        now.saturating_add(self.retention.as_millis() as u64)
    }
}

#[derive(Default)]
struct Usage {
    active: HashMap<Activation, u64>,
    sessions: HashMap<Activation, Session>,
    tokens: Option<f64>,
    refilled_at: u64,
}

impl Usage {
    fn prune(&mut self, now: u64) {
        self.active.retain(|_, expires_at| *expires_at > now);
        self.sessions.retain(|_, state| state.forget_at > now);
    }

    fn consume(&mut self, limits: OrganizationLimits, now: u64) -> Result<()> {
        let rate = f64::from(limits.starts_per_second);
        let burst = f64::from(limits.start_burst);
        let tokens = (self.tokens.unwrap_or(burst)
            + now.saturating_sub(self.refilled_at) as f64 * rate / 1000.0)
            .min(burst);
        ensure!(
            tokens >= 1.0,
            AdmissionRejected::Rate {
                retry_after_seconds: ((1.0 - tokens) / rate).ceil().max(1.0) as u64
            }
        );
        self.tokens = Some(tokens - 1.0);
        self.refilled_at = self.refilled_at.max(now);
        Ok(())
    }
}

#[derive(Default)]
struct Session {
    epoch: u64,
    sequence: u64,
    stopped: bool,
    forget_at: u64,
}
