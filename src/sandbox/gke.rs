use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use super::{
    remote::{RemoteSandboxProvider, SandboxCluster},
    *,
};

mod kubernetes;
pub(crate) use super::remote::validate_image;
pub(crate) use kubernetes::GkeConfig;

pub(crate) async fn provider(config: GkeConfig) -> Result<RemoteSandboxProvider> {
    let cluster = kubernetes::Kubernetes::new(kube::Client::try_default().await?, config);
    RemoteSandboxProvider::new("gke", Arc::new(cluster))
}
