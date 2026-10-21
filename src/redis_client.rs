use anyhow::{Context, Result, anyhow};
use redis::aio::{ConnectionManager, ConnectionManagerConfig, PubSub};
use std::time::Duration;

pub struct RedisConfig {
    pub url: String,
    pub ca_cert: Option<String>,
}

const COMMAND_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub(crate) struct Redis {
    client: redis::Client,
    connection: ConnectionManager,
}

impl Redis {
    pub(crate) fn connect(config: &RedisConfig) -> Result<Self> {
        let client = match &config.ca_cert {
            Some(ca) => redis::Client::build_with_tls(
                config.url.as_str(),
                redis::TlsCertificates {
                    client_tls: None,
                    root_cert: Some(ca.as_bytes().to_vec()),
                },
            ),
            None => redis::Client::open(config.url.as_str()),
        }
        .context("configure Redis client")?;
        let connection = ConnectionManager::new_lazy_with_config(
            client.clone(),
            ConnectionManagerConfig::new()
                .set_connection_timeout(Some(Duration::from_secs(1)))
                .set_response_timeout(Some(COMMAND_TIMEOUT))
                .set_number_of_retries(1),
        )
        .context("configure Redis connection")?;
        Ok(Self { client, connection })
    }

    pub(crate) async fn pubsub(&self) -> Result<PubSub> {
        tokio::time::timeout(Duration::from_secs(5), self.client.get_async_pubsub())
            .await
            .context("Redis pub/sub connection timed out")?
            .context("open Redis pub/sub connection")
    }

    pub(crate) async fn run<T: redis::FromRedisValue>(&self, command: redis::Cmd) -> Result<T> {
        let mut connection = self.connection.clone();
        let reply = tokio::time::timeout(COMMAND_TIMEOUT, command.query_async(&mut connection))
            .await
            .map_err(|_| anyhow!("Redis did not answer within {COMMAND_TIMEOUT:?}"))?;
        Ok(reply?)
    }
}
