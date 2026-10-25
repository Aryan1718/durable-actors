use std::{future::Future, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

use super::config::{ActorHostConfig, executor_runtime};
use super::process::{HostStartupTimings, serve_assigned_host, spawn_executor_process};
use crate::actor::{ActorExecutorListener, WarmExecutor};
use crate::bucket::WarmStorage;
use crate::litestream::{Litestream, Replicator};

pub(super) struct WarmHost {
    pub operation: Option<crate::sandbox::assignment::InitialOperation>,
    pub readiness: Option<tokio::sync::oneshot::Sender<super::process::HostReadiness>>,
    pub listener: TcpListener,
    pub executor: WarmExecutor,
    pub javascript: tokio::process::Child,
    pub storage: WarmStorage,
    pub control_plane: Option<WarmControlPlane>,
    pub replication: Arc<dyn Replicator>,
}

pub(super) struct WarmControlPlane {
    pub url: String,
    pub client: crate::control_plane::ControlPlaneClient,
}

pub async fn serve_spare(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    super::protect_runtime_credentials()?;
    let replication = Arc::new(Litestream::default());
    let socket = std::env::var("DURABLE_ACTORS_EXECUTOR_SOCKET")
        .unwrap_or_else(|_| "/tmp/durable-actors-executor.sock".into());
    let token = std::env::var("DURABLE_ACTORS_SPARE_TOKEN").context("spare token missing")?;
    ensure!(token.len() >= 32, "spare token is too short");
    let control_bind =
        std::env::var("DURABLE_ACTORS_SPARE_BIND").unwrap_or_else(|_| "0.0.0.0:7102".into());
    let control = TcpListener::bind(control_bind).await?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let stop = tokio_util::sync::CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    let mut server = tokio::spawn(async move {
        axum::serve(control, super::assignment::router(token, send))
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
    });
    let ready = std::env::var("DURABLE_ACTORS_SPARE_READY_FILE")
        .unwrap_or_else(|_| "/tmp/durable-actors-spare-ready".into());
    let bind = std::env::var("DURABLE_ACTORS_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:7101".into());
    let listener = TcpListener::bind(bind).await?;
    let ipc = ActorExecutorListener::bind(&socket).await?;
    let configured_runtime = std::env::var("DURABLE_ACTORS_EXECUTOR_RUNTIME").ok();
    let runtime = executor_runtime(None, configured_runtime.as_deref())?;
    let mut javascript = spawn_executor_process(true, &socket, runtime)?;
    let (warmed, control_plane) = tokio::join!(
        async {
            tokio::try_join!(
                async { tokio::time::timeout(Duration::from_secs(30), ipc.accept_warm()).await? },
                async {
                    let storage = WarmStorage::new().await?;
                    storage.preconnect().await;
                    anyhow::Ok(storage)
                },
            )
        },
        prewarm_control_plane(std::env::var("DURABLE_ACTORS_CONTROL_PLANE_URL").ok()),
    );
    let (executor, storage) = warmed?;
    let control_plane = Some(control_plane?);
    tokio::fs::write(&ready, b"ready\n").await?;
    tokio::pin!(shutdown);
    let assigned = tokio::select! {
        value = receive => value?,
        result = &mut server => anyhow::bail!("spare assignment server stopped: {result:?}"),
        status = javascript.wait() => anyhow::bail!("generic actor executor exited: {}", status?),
        () = &mut shutdown => return Ok(()),
        () = storage.keep_warm() => unreachable!("storage warmup stopped"),
    };
    tokio::fs::remove_file(&ready).await?;
    let environment = assigned.environment;
    let timings = HostStartupTimings::start();
    let config = ActorHostConfig::from_lookup(|name| environment.get(name).cloned())?;
    let timings = timings.configuration_loaded();
    let entrypoint = config
        .executor
        .entrypoint
        .as_deref()
        .context("missing customer entrypoint")?;
    ensure!(
        entrypoint.starts_with("/customer/")
            && (entrypoint.ends_with(".mjs") || entrypoint.ends_with(".pyz"))
            && !Path::new(&entrypoint)
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "customer entrypoint must be a compiled module under /customer"
    );
    ensure!(
        config.executor.runtime == runtime,
        "actor artifact does not match the prewarmed executor runtime"
    );
    let warm = WarmHost {
        operation: assigned.operation,
        readiness: Some(assigned.ready),
        listener,
        executor,
        javascript,
        storage,
        control_plane,
        replication,
    };
    serve_assigned_host(config, timings, Some(warm), shutdown).await
}

async fn prewarm_control_plane(url: Option<String>) -> Result<WarmControlPlane> {
    let url = url.context("spare control-plane URL required")?;
    let client = crate::control_plane::ControlPlaneClient::prewarm(&url).await?;
    Ok(WarmControlPlane { url, client })
}

#[cfg(test)]
#[path = "../../tests/unit/host/spare.rs"]
mod tests;
