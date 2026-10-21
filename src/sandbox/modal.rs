use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use ::modal::{
    App, AppLookupParams, Client, Image, Probe, Sandbox, SandboxCreateParams, SandboxListParams,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    remote::{RemoteSandboxProvider, SandboxCluster},
    *,
};

const READY_FILE: &str = "/tmp/durable-actors-spare-ready";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Debug)]
pub(crate) struct ModalConfig {
    pub app_name: String,
    pub regions: BTreeMap<String, String>,
    pub timeout: Duration,
}

pub(crate) async fn provider(
    config: ModalConfig,
    images: &[&str],
) -> Result<RemoteSandboxProvider> {
    let client = Client::from_env()?;
    let app = client
        .app_from_name(
            &config.app_name,
            AppLookupParams {
                create_if_missing: true,
                ..Default::default()
            },
        )
        .await?;
    let mut imported = HashMap::new();
    for reference in images {
        if imported.contains_key(*reference) {
            continue;
        }
        let image = Image::from_registry(*reference).dockerfile_commands(["ENTRYPOINT []"]);
        tokio::time::timeout(Duration::from_secs(600), image.build(&client, &app))
            .await
            .context("Modal image import timed out")??;
        imported.insert((*reference).to_owned(), image);
    }
    RemoteSandboxProvider::new(
        "modal",
        Arc::new(ModalCluster {
            client,
            app,
            config,
            images: imported,
        }),
    )
}

struct ModalCluster {
    client: Client,
    app: App,
    config: ModalConfig,
    images: HashMap<String, Image>,
}

#[async_trait]
impl SandboxCluster for ModalCluster {
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        let image = self
            .images
            .get(&request.image_ref)
            .context("Modal runtime image was not imported")?;
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let params = spare_params(request, &self.config, &token)?;
        let sandbox = tokio::time::timeout(
            STARTUP_TIMEOUT,
            self.client.create_sandbox(&self.app, image, params),
        )
        .await
        .context("Modal sandbox creation timed out")??;
        let mut cleanup = SandboxCleanup(Some(sandbox.clone()));
        let handle = tokio::time::timeout(STARTUP_TIMEOUT, ready_handle(&sandbox, request, token))
            .await
            .context("Modal spare readiness timed out")??;
        cleanup.0 = None;
        Ok(handle)
    }

    async fn retire_spare(&self, spare: &SpareHandle) -> Result<()> {
        let sandbox = if spare.resource_id.is_empty() {
            match self
                .client
                .sandbox_from_name(&self.config.app_name, &spare.name, None)
                .await
            {
                Ok(sandbox) => sandbox,
                Err(::modal::Error::NotFound(_)) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        } else {
            self.client.sandbox_from_id(&spare.resource_id)
        };
        match sandbox.terminate().await {
            Ok(()) | Err(::modal::Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        let running: HashSet<_> = self
            .client
            .list_sandboxes(SandboxListParams {
                app_id: Some(self.app.app_id.clone()),
                tags: HashMap::from([("managed-by".into(), "durable-actors".into())]),
                ..Default::default()
            })
            .await?
            .into_iter()
            .map(|sandbox| sandbox.sandbox_id)
            .collect();
        Ok(spares
            .iter()
            .filter(|spare| !running.contains(&spare.resource_id))
            .map(|spare| spare.resource_id.clone())
            .collect())
    }

    async fn secrets(&self, names: &[String]) -> Result<HashMap<String, String>> {
        ensure!(
            names.is_empty(),
            "Modal does not support deployment secretRefs"
        );
        Ok(HashMap::new())
    }
}

fn spare_params(
    request: &CreateSpareRequest,
    config: &ModalConfig,
    token: &str,
) -> Result<SandboxCreateParams> {
    let region = config
        .regions
        .get(&request.canonical_region)
        .context("no Modal placement for actor region")?;
    let control_plane = request
        .control_plane_url
        .as_ref()
        .context("spare control-plane URL required")?;
    Ok(SandboxCreateParams {
        name: Some(request.name.clone()),
        command: vec!["/usr/local/bin/durable-actors".into()],
        env: HashMap::from([
            ("DURABLE_ACTORS_PROCESS_ROLE".into(), "spare".into()),
            ("DURABLE_ACTORS_SPARE_TOKEN".into(), token.into()),
            (
                "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
                control_plane.clone(),
            ),
            ("DURABLE_ACTORS_SPARE_READY_FILE".into(), READY_FILE.into()),
            ("TOKIO_WORKER_THREADS".into(), "2".into()),
        ]),
        cpu: Some(f64::from(request.resources.cpu_millis) / 1000.0),
        cpu_limit: Some(f64::from(request.resources.cpu_millis) / 1000.0),
        memory_mib: Some(request.resources.memory_mib),
        memory_limit_mib: Some(request.resources.memory_mib),
        timeout: Some(config.timeout),
        cloud: Some("gcp".into()),
        regions: vec![region.clone()],
        h2_ports: vec![7101, 7102],
        readiness_probe: Some(
            Probe::exec(["/usr/bin/test", "-f", READY_FILE]).interval(Duration::from_millis(250)),
        ),
        tags: HashMap::from([("managed-by".into(), "durable-actors".into())]),
        ..Default::default()
    })
}

async fn ready_handle(
    sandbox: &Sandbox,
    request: &CreateSpareRequest,
    token: String,
) -> Result<SpareHandle> {
    let (_, tunnels) = tokio::try_join!(
        sandbox.wait_until_ready(STARTUP_TIMEOUT),
        sandbox.tunnels(STARTUP_TIMEOUT)
    )?;
    Ok(SpareHandle {
        name: request.name.clone(),
        resource_id: sandbox.id().into(),
        route: tunnels
            .get(&7101)
            .context("Modal invocation tunnel missing")?
            .url(),
        control_route: tunnels
            .get(&7102)
            .context("Modal assignment tunnel missing")?
            .url(),
        control_token: token,
        canonical_region: request.canonical_region.clone(),
    })
}

struct SandboxCleanup(Option<Sandbox>);

impl Drop for SandboxCleanup {
    fn drop(&mut self) {
        if let Some(sandbox) = self.0.take() {
            tokio::spawn(async move {
                let result =
                    tokio::time::timeout(Duration::from_secs(30), sandbox.terminate()).await;
                if !matches!(result, Ok(Ok(()))) {
                    tracing::warn!(
                        resource = sandbox.id(),
                        ?result,
                        "failed to retire Modal sandbox after startup failure"
                    );
                }
            });
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/modal.rs"]
mod tests;
