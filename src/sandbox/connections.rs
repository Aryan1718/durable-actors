use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Result;
use hickory_resolver::{TokioResolver, config::LookupIpStrategy};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use super::SpareHandle;

#[derive(Clone)]
pub(crate) struct HostConnections {
    pub client: reqwest::Client,
    resolver: Arc<dyn Resolve>,
}

impl HostConnections {
    pub fn new() -> Result<Self> {
        let mut builder = TokioResolver::builder_tokio()?;
        builder.options_mut().ip_strategy = LookupIpStrategy::Ipv6AndIpv4;
        Self::with_resolver(Arc::new(DnsResolver(builder.build()?)))
    }

    fn with_resolver(resolver: Arc<dyn Resolve>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .dns_resolver(resolver.clone())
                .http2_prior_knowledge()
                .http2_keep_alive_interval(Duration::from_secs(20))
                .http2_keep_alive_timeout(Duration::from_secs(5))
                .http2_keep_alive_while_idle(true)
                .pool_idle_timeout(Duration::from_secs(60))
                .pool_max_idle_per_host(1)
                .connect_timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            resolver,
        })
    }

    pub async fn prewarm(&self, spare: &SpareHandle) {
        let warming = async {
            tokio::join!(
                self.resolve(&spare.route),
                self.preconnect(&spare.control_route)
            )
        };
        let _ = tokio::time::timeout(Duration::from_secs(2), warming).await;
    }

    async fn preconnect(&self, route: &str) -> Result<()> {
        self.client
            .get(format!("{}/healthz", route.trim_end_matches('/')))
            .send()
            .await?
            .bytes()
            .await?;
        Ok(())
    }

    async fn resolve(&self, route: &str) -> Result<()> {
        let url = reqwest::Url::parse(route)?;
        if let Some(domain) = url.domain() {
            let _ = self
                .resolver
                .resolve(domain.parse()?)
                .await
                .map_err(anyhow::Error::msg)?;
        }
        Ok(())
    }
}

struct DnsResolver(TokioResolver);

impl Resolve for DnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let resolver = self.0.clone();
        Box::pin(async move {
            let lookup = resolver.lookup_ip(name.as_str()).await?;
            let addresses = lookup
                .iter()
                .map(|ip| SocketAddr::new(ip, 0))
                .collect::<Vec<_>>();
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/connections.rs"]
mod tests;
