use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, ensure};
use async_trait::async_trait;

use super::{
    Activation, AdmissionRejected, LAUNCH_LEASE_MS, ProjectLimits, REPORT_LEASE_MS,
    ReservationStore,
};
use crate::{
    actor_lifecycle::{LifecycleEvent, LifecycleKind},
    clock::Clock,
};

pub(in crate::control_plane) struct MemoryReservations {
    projects: Mutex<HashMap<String, Project>>,
    clock: Arc<dyn Clock>,
    retention: Duration,
}

impl MemoryReservations {
    pub fn new(clock: Arc<dyn Clock>, retention: Duration) -> Self {
        Self {
            projects: Mutex::new(HashMap::new()),
            clock,
            retention,
        }
    }
}

#[async_trait]
impl ReservationStore for MemoryReservations {
    async fn reserve(&self, activation: &Activation, limits: ProjectLimits) -> Result<()> {
        let now = self.clock.now_ms()?;
        let mut projects = self.projects.lock().unwrap();
        let project = projects
            .entry(activation.actor.project_id.clone())
            .or_default();
        project.prune(now);
        if let Some(state) = project.sessions.get(activation) {
            ensure!(!state.stopped, "actor activation has already stopped");
            return Ok(());
        }
        ensure!(
            project.active.len() < limits.max_active_actors as usize,
            AdmissionRejected::Quota {
                limit: limits.max_active_actors
            }
        );
        project.consume(limits, now)?;
        project
            .active
            .insert(activation.clone(), now.saturating_add(LAUNCH_LEASE_MS));
        project.sessions.insert(
            activation.clone(),
            Session {
                forget_at: self.forget_at(now),
                ..Session::default()
            },
        );
        Ok(())
    }

    async fn release(&self, activation: &Activation) -> Result<()> {
        let mut projects = self.projects.lock().unwrap();
        if let Some(project) = projects.get_mut(&activation.actor.project_id) {
            project.active.remove(activation);
            project.sessions.remove(activation);
        }
        Ok(())
    }

    async fn observe(&self, event: &LifecycleEvent) -> Result<()> {
        let now = self.clock.now_ms()?;
        let activation = Activation::new(&event.actor, &event.host_id, &event.session_id);
        let mut projects = self.projects.lock().unwrap();
        let project = projects.entry(event.actor.project_id.clone()).or_default();
        project.prune(now);
        let state = project.sessions.entry(activation.clone()).or_default();
        if state.stopped
            || (state.epoch > 0 && state.epoch != event.report.owner_epoch)
            || event.report.sequence <= state.sequence
        {
            return Ok(());
        }
        state.epoch = event.report.owner_epoch;
        state.sequence = event.report.sequence;
        state.stopped = event.report.kind == LifecycleKind::Stopped;
        state.forget_at = self.forget_at(now);
        if state.stopped {
            project.active.remove(&activation);
        } else {
            project
                .active
                .insert(activation, now.saturating_add(REPORT_LEASE_MS));
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
struct Project {
    active: HashMap<Activation, u64>,
    sessions: HashMap<Activation, Session>,
    tokens: Option<f64>,
    refilled_at: u64,
}

impl Project {
    fn prune(&mut self, now: u64) {
        self.active.retain(|_, expires_at| *expires_at > now);
        self.sessions.retain(|_, state| state.forget_at > now);
    }

    fn consume(&mut self, limits: ProjectLimits, now: u64) -> Result<()> {
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
