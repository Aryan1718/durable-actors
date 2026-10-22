use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client, ResourceExt};
use tokio::sync::OnceCell;

#[async_trait]
pub(crate) trait SecretResolver: Send + Sync {
    async fn resolve(&self, names: &[String]) -> Result<HashMap<String, String>>;
}

pub(crate) struct KubernetesSecrets {
    namespace: String,
    secrets: OnceCell<Api<Secret>>,
}

impl KubernetesSecrets {
    pub(crate) fn new(namespace: String) -> Self {
        Self {
            namespace,
            secrets: OnceCell::new(),
        }
    }
}

#[async_trait]
impl SecretResolver for KubernetesSecrets {
    async fn resolve(&self, names: &[String]) -> Result<HashMap<String, String>> {
        if names.is_empty() {
            return Ok(HashMap::new());
        }
        let secrets = self
            .secrets
            .get_or_try_init(|| async {
                Client::try_default()
                    .await
                    .map(|client| Api::namespaced(client, &self.namespace))
            })
            .await?;
        let loaded =
            futures_util::future::try_join_all(names.iter().map(|name| secrets.get(name))).await?;
        customer_environment(loaded)
    }
}

fn customer_environment(secrets: Vec<Secret>) -> Result<HashMap<String, String>> {
    let mut environment = HashMap::new();
    for secret in secrets {
        ensure!(
            secret
                .labels()
                .get("terse.ai/customer-secret")
                .is_some_and(|value| value == "true"),
            "secret is not marked for customer code"
        );
        for (name, value) in secret.data.unwrap_or_default() {
            ensure!(
                !environment.contains_key(&name),
                "duplicate customer environment variable {name}"
            );
            environment.insert(
                name,
                String::from_utf8(value.0).context("customer secret must be UTF-8")?,
            );
        }
    }
    Ok(environment)
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/secrets.rs"]
mod tests;
