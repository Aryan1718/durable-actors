use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, oneshot};

use super::process::HostReadiness;
use crate::sandbox::assignment::{AssignmentRequest, InitialOperation, InitialRequest};

pub(super) struct Assignment {
    pub environment: HashMap<String, String>,
    pub operation: Option<InitialOperation>,
    pub ready: oneshot::Sender<Readiness>,
}

pub(super) struct NotStarted;

pub(super) type Readiness = Result<HostReadiness, NotStarted>;

pub(super) fn decline(readiness: Option<oneshot::Sender<Readiness>>) {
    if let Some(readiness) = readiness {
        let _ = readiness.send(Err(NotStarted));
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum AssignmentInput {
    Operation(AssignmentRequest),
    Environment(HashMap<String, String>),
}

struct AssignmentState {
    authorization: String,
    pending: Mutex<Option<oneshot::Sender<Assignment>>>,
}

pub(super) fn router(token: String, pending: oneshot::Sender<Assignment>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/assign", post(assign))
        .layer(DefaultBodyLimit::disable())
        .with_state(Arc::new(AssignmentState {
            authorization: format!("Bearer {token}"),
            pending: Mutex::new(Some(pending)),
        }))
}

async fn assign(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    Json(input): Json<AssignmentInput>,
) -> Result<Json<HostReadiness>, StatusCode> {
    let supplied = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(state.authorization.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let (environment, operation) = match input {
        AssignmentInput::Operation(request) => (request.environment, Some(request.operation)),
        AssignmentInput::Environment(environment) => (environment, None),
    };
    if let Some(operation) = &operation {
        validate_operation(&environment, operation).map_err(|_| StatusCode::BAD_REQUEST)?;
    }
    let pending = state
        .pending
        .lock()
        .await
        .take()
        .ok_or(StatusCode::CONFLICT)?;
    let (ready, receive) = oneshot::channel();
    pending
        .send(Assignment {
            environment,
            operation,
            ready,
        })
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    match receive.await {
        Ok(Ok(ready)) => Ok(Json(ready)),
        Ok(Err(NotStarted)) => Err(StatusCode::CONFLICT),
        Err(_) => Err(StatusCode::SERVICE_UNAVAILABLE),
    }
}

fn validate_operation(
    environment: &HashMap<String, String>,
    operation: &InitialOperation,
) -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    let actor: crate::actor::ActorKey = serde_json::from_str(
        environment
            .get("DURABLE_ACTORS_ACTOR")
            .context("actor identity missing")?,
    )?;
    let requested = match &operation.request {
        InitialRequest::Invoke { invocation, .. } => {
            invocation.validate()?;
            &invocation.actor
        }
        InitialRequest::Socket { invocation } => {
            invocation.actor.validate()?;
            &invocation.actor
        }
    };
    ensure!(requested == &actor, "assignment operation actor mismatch");
    ensure!(
        operation.routing_ms.is_finite() && operation.routing_ms >= 0.0,
        "invalid routing time"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/host/assignment.rs"]
mod tests;
