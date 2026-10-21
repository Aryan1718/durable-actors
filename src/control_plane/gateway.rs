use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message},
    },
    http::StatusCode,
    response::Response,
    routing::get,
};
use serde::Deserialize;
use std::sync::Arc;

use super::{
    ActorJwtVerifier, auth::ActorInvocationCapability, issuer::ActorJwtIssuer,
    service::ControlPlaneService, socket_gateway::SocketGateway,
    socket_ticket::SocketTicketVerifier,
};
use crate::{
    actor::ActorKey,
    control_plane::socket_gateway::GatewaySocketDispatcher,
    sockets::{
        MAX_MESSAGE_BYTES, READ_BUFFER_BYTES, WRITE_BUFFER_BYTES,
        browser::{SocketServerState, run},
    },
};

#[derive(Clone)]
pub(super) struct Gateway {
    pub origin: String,
    invocations: ActorJwtVerifier,
    sockets: SocketTicketVerifier,
    pub(super) connections: Arc<SocketGateway>,
}

impl Gateway {
    pub(super) fn new(
        issuer: &ActorJwtIssuer,
        origin: String,
        connections: Arc<SocketGateway>,
    ) -> Result<Self> {
        Ok(Self {
            origin,
            connections,
            invocations: issuer.invocation_verifier()?,
            sockets: issuer.socket_verifier()?,
        })
    }

    pub fn invocation_route(
        &self,
        actor: &ActorKey,
        epoch: u64,
        authorization: &str,
    ) -> Result<String> {
        let capability = self.capability(actor, authorization)?;
        ensure!(capability.owner_epoch == epoch, "invalid owner epoch");
        Ok(backend_origin(&capability.route)?.to_string())
    }

    pub fn authorize_publish(&self, actor: &ActorKey, authorization: &str) -> Result<u64> {
        let capability = self.capability(actor, authorization)?;
        ensure!(
            capability.grant.is_none(),
            "delegated tickets cannot publish socket effects"
        );
        Ok(capability.owner_epoch)
    }

    pub fn router(self, service: ControlPlaneService) -> Router {
        Router::new()
            .route("/v1/socket", get(connect))
            .with_state((self, service))
    }

    fn capability(
        &self,
        actor: &ActorKey,
        authorization: &str,
    ) -> Result<ActorInvocationCapability> {
        let principal = self.invocations.authenticate_authorization(authorization)?;
        let capability = principal
            .invocation
            .context("invocation capability missing")?;
        ensure!(
            principal.actor == *actor && capability.actor == *actor,
            "invocation target mismatch"
        );
        Ok(capability)
    }
}

pub(super) fn backend_origin(route: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(route)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none(),
        "invalid host origin"
    );
    Ok(url)
}

#[derive(Deserialize)]
struct SocketQuery {
    key: Option<String>,
}

async fn connect(
    State((gateway, service)): State<(Gateway, ControlPlaneService)>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let key = query.key.ok_or(StatusCode::UNAUTHORIZED)?;
    let ticket = gateway
        .sockets
        .verify(&key)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let connections = gateway.connections.clone();
    if !connections.accepts_sockets {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let generation = connections
        .generation()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let state = SocketServerState {
        registry: connections.registry.clone(),
        dispatcher: Arc::new(GatewaySocketDispatcher {
            gateway: connections.clone(),
            service,
            generation,
        }),
        stop: connections.stop.clone(),
    };
    Ok(upgrade
        .read_buffer_size(READ_BUFFER_BYTES)
        .write_buffer_size(WRITE_BUFFER_BYTES)
        .max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |mut socket| async move {
            let actor = ticket.actor.clone();
            if connections.bus.watch(&actor).await.is_err() {
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: 1013,
                        reason: "socket gateway unavailable".into(),
                    })))
                    .await;
                return;
            }
            run(socket, state, ticket).await;
            connections.bus.unwatch(&actor).await;
        }))
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/gateway.rs"]
mod tests;
