use std::{fmt, time::Duration};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::ControlPlaneService;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AccessSource {
    #[default]
    Admin,
    Billing,
}

impl AccessSource {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Billing => "billing",
        }
    }
}

#[derive(Debug)]
pub(super) struct AccessDisabled;

impl fmt::Display for AccessDisabled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("organization access is disabled")
    }
}

impl std::error::Error for AccessDisabled {}

impl ControlPlaneService {
    pub(super) async fn ensure_project_access(&self, project: &str) -> Result<()> {
        if !self.registry.project_access_enabled(project).await? {
            return Err(AccessDisabled.into());
        }
        Ok(())
    }

    pub(super) async fn ensure_organization_access(
        &self,
        organization: Option<&str>,
    ) -> Result<()> {
        if !self
            .registry
            .organization_access_enabled(organization)
            .await?
        {
            return Err(AccessDisabled.into());
        }
        Ok(())
    }

    pub(super) fn start_access_reconciliation(&self, stop: CancellationToken) {
        let service = self.clone();
        tokio::spawn(async move {
            let work = async {
                let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if let Err(error) = service.reconcile_access().await {
                        tracing::warn!(%error, "access reconciliation failed");
                    }
                }
            };
            tokio::select! { _ = stop.cancelled() => {}, _ = work => {} }
        });
    }

    pub(super) async fn reconcile_access(&self) -> Result<()> {
        self.close_disabled_sockets().await?;
        for spec in self.registry.disabled_deployments().await? {
            let result = tokio::time::timeout(
                super::CONTROL_PLANE_REQUEST_TIMEOUT,
                self.stop_disabled_deployment(&spec.project_id),
            )
            .await;
            if !matches!(result, Ok(Ok(()))) {
                tracing::warn!(project_id = %spec.project_id, ?result, "disabled deployment shutdown failed; will retry");
            }
        }
        Ok(())
    }

    async fn close_disabled_sockets(&self) -> Result<()> {
        let Some(gateway) = &self.gateway else {
            return Ok(());
        };
        let sockets = &gateway.connections.registry;
        for project in sockets.project_ids().await {
            if !self.registry.project_access_enabled(&project).await? {
                sockets
                    .close_project(&project, 4403, "organization access disabled")
                    .await;
            }
        }
        Ok(())
    }

    async fn stop_disabled_deployment(&self, project: &str) -> Result<()> {
        let _update = self.registry.lock_deployment(project).await?;
        let Some(spec) = self.registry.launch_spec(project).await? else {
            return Ok(());
        };
        if !self
            .registry
            .organization_access_enabled(Some(&spec.organization_id))
            .await?
        {
            self.terminate_deployment_hosts(&spec).await?;
        }
        Ok(())
    }
}
