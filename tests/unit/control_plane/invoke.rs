use super::super::super::socket_gateway::{SocketEventReply, SocketEventRequest};
use super::*;
use crate::actor::{ActorSocketEffect, ActorSocketMessage};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

#[path = "shutdown_routing.rs"]
mod shutdown_routing;

#[tokio::test]
async fn combined_invocation_resolves_and_dispatches_with_a_scoped_host_token() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":null}),
    )])
    .await?;
    let response = fixture.call("api-key", json!({"requestId":"call-1", "method":"sendMessage", "args":["hello"], "homeRegion":"north-america-east"})).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let reply: Value = response.json().await?;
    assert_eq!(reply["outcome"]["type"], "completed");
    assert!(reply["outcome"]["result"].is_null());
    assert_eq!(reply["target"]["ownerEpoch"], 1);
    assert!(reply["target"].get("hostState").is_none());
    assert_eq!(reply["target"]["route"], fixture.origin);
    assert!(reply["target"]["expiresAtMs"].as_i64().unwrap() > 0);
    assert!(
        reply["target"]["route"]
            .as_str()
            .unwrap()
            .starts_with("http://127.0.0.1:")
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    let requests = fixture.host.requests.lock().unwrap();
    let routing_ms = requests[0].1["routingMs"].as_f64().unwrap();
    assert!(routing_ms >= 25.0);
    assert_eq!(
        requests[0].1,
        json!({"requestId":"call-1", "ownerEpoch":1, "method":"sendMessage", "args":["hello"], "hostState":"cold", "routingMs":routing_ms})
    );
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    assert_eq!(principal.actor.actor_id, "one");
    assert_eq!(
        requests[0].0,
        format!("Bearer {}", reply["target"]["token"].as_str().unwrap())
    );
    Ok(())
}

#[tokio::test]
async fn resolving_an_existing_host_marks_the_request_warm() -> Result<()> {
    let fixture = Fixture::start_with_existing_host(
        vec![(StatusCode::OK, json!({"type":"completed", "result":1}))],
        true,
    )
    .await?;
    fixture
        .call(
            "api-key",
            json!({"requestId":"warm", "method":"clear", "args":[]}),
        )
        .await?
        .error_for_status()?;
    assert_eq!(
        fixture.host.requests.lock().unwrap()[0].1["hostState"],
        "warm"
    );
    Ok(())
}

#[tokio::test]
async fn combined_invocation_authenticates_and_validates_requests() -> Result<()> {
    let fixture = Fixture::start(vec![(
        StatusCode::OK,
        json!({"type":"completed", "result":7}),
    )])
    .await?;
    let request = json!({"requestId":"call-1", "method":"sendMessage", "args":[]});
    assert_eq!(
        fixture.call("invalid", request.clone()).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    for body in [
        json!({"requestId":"", "method":"sendMessage", "args":[]}),
        json!({"requestId":"r", "method":"sendMessage", "args":{}}),
        json!({"requestId":"r", "method":"sendMessage", "args":[], "homeRegion":false}),
    ] {
        assert_eq!(
            fixture.call("api-key", body).await?.status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    let expiry = (super::super::super::auth::unix_seconds()? + 30) * 1000;
    let session = fixture
        .admin
        .issue_session("default".into(), "subject".into(), expiry)?;
    let response = fixture.call(&session.token, request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let requests = fixture.host.requests.lock().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&requests[0].0)?;
    let grant = principal.invocation.unwrap().grant.unwrap();
    assert_eq!(grant.subject, "subject");
    Ok(())
}

#[tokio::test]
async fn combined_invocation_retries_known_non_execution_through_handoff() -> Result<()> {
    for (status, outcome) in [
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"host_unavailable"}),
        ),
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"upstream_not_reached"}),
        ),
        (StatusCode::UNAUTHORIZED, json!({})),
    ] {
        let mut replies = vec![(status, outcome); 4];
        replies.push((StatusCode::OK, json!({"type":"completed", "result":42})));
        let fixture = Fixture::start(replies).await?;
        let started = std::time::Instant::now();
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"same-id", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(reply["outcome"], json!({"type":"completed", "result":42}));
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let requests = fixture.host.requests.lock().unwrap();
        let routing_ms = requests.last().unwrap().1["routingMs"].as_f64().unwrap();
        assert!(routing_ms >= 400.0 && routing_ms <= elapsed_ms);
        drop(requests);
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 5);
        assert!(fixture.host.requests.lock().unwrap().iter().all(
            |(_, request)| request["requestId"] == "same-id" && request["hostState"] == "cold"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn combined_invocation_does_not_replay_failed_or_ambiguous_dispatches() -> Result<()> {
    for (status, body, code) in [
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"actor_error", "message":"boom"}),
            "actor_error",
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"not_executed", "reason":"unknown"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"completed"}),
            "outcome_unknown",
        ),
        (
            StatusCode::OK,
            json!({"type":"failed", "code":"", "message":"boom"}),
            "outcome_unknown",
        ),
    ] {
        let fixture = Fixture::start(vec![(status, body)]).await?;
        let reply: Value = fixture
            .call(
                "api-key",
                json!({"requestId":"r", "method":"clear", "args":[]}),
            )
            .await?
            .json()
            .await?;
        assert_eq!(
            reply["outcome"]
                .get("code")
                .unwrap_or(&reply["error"]["code"]),
            code
        );
        assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn gateway_forwards_cached_calls_and_rejects_wrong_capabilities() -> Result<()> {
    let fixture = Fixture::start(vec![
        (StatusCode::OK, json!({"type":"completed","result":1})),
        (StatusCode::OK, json!({"type":"completed","result":2})),
    ])
    .await?;
    let first: Value = fixture
        .call(
            "api-key",
            json!({"requestId":"first","method":"clear","args":[]}),
        )
        .await?
        .json()
        .await?;
    let token = first["target"]["token"].as_str().unwrap();
    let request = json!({"requestId":"cached","ownerEpoch":1,"method":"clear","args":[]});
    assert_eq!(
        fixture.call("api-key", request.clone()).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let mut wrong_epoch = request.clone();
    wrong_epoch["ownerEpoch"] = 2.into();
    assert_eq!(
        fixture.call(token, wrong_epoch).await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let wrong_actor = reqwest::Client::new()
        .post(format!(
            "{}/v1/projects/default/actors/ChatRoom/other/invoke",
            fixture.origin
        ))
        .bearer_auth(token)
        .json(&request)
        .send()
        .await?;
    assert_eq!(wrong_actor.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 1);
    let cached: Value = fixture
        .call(token, request)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(cached, json!({"type":"completed","result":2}));
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn backend_socket_effects_reach_connected_clients() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let mut socket = fixture.connect().await?;
    let target = fixture.find_actor().await?;
    let token = target["token"].as_str().unwrap();
    let response = fixture
        .post("socket-effects", token, broadcast(&target))
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        next_message(&mut socket).await?,
        Message::Text("hello".into())
    );
    Ok(())
}

#[tokio::test]
async fn backend_socket_effects_reach_clients_connected_to_another_replica() -> Result<()> {
    let fixture = Fixture::start(vec![]).await?;
    let mut socket = fixture.connect().await?;
    let target = fixture.find_actor().await?;
    let token = target["token"].as_str().unwrap();
    let response = post_actor(
        &fixture.replica,
        "socket-effects",
        token,
        broadcast(&target),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        next_message(&mut socket).await?,
        Message::Text("hello".into())
    );
    Ok(())
}

struct Fixture {
    service: ControlPlaneService,
    placements: Arc<LocalObjectPlacementStore>,
    origin: String,
    replica: String,
    admin: AdminService,
    verifier: ActorJwtVerifier,
    host: Arc<HostFixture>,
    servers: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Fixture {
    async fn start(replies: Vec<(StatusCode, Value)>) -> Result<Self> {
        Self::start_with_existing_host(replies, false).await
    }

    async fn start_with_existing_host(
        replies: Vec<(StatusCode, Value)>,
        existing: bool,
    ) -> Result<Self> {
        Self::start_with_routing(replies, existing, None).await
    }

    async fn start_with_routing(
        replies: Vec<(StatusCode, Value)>,
        existing: bool,
        routing: Option<Arc<dyn crate::actor_lifecycle::routing::RoutingHints>>,
    ) -> Result<Self> {
        let host = Arc::new(HostFixture {
            socket_requests: Mutex::new(vec![]),
            replies: Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(vec![]),
        });
        let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let host_origin = format!("http://{}", host_listener.local_addr()?);
        let routes = Router::new()
            .route(
                "/v1/projects/default/actors/ChatRoom/one/invoke",
                post(host_invoke),
            )
            .route(
                "/v1/projects/default/actors/ChatRoom/one/socket-events",
                post(host_socket_event),
            )
            .with_state(host.clone());
        let host_server = tokio::spawn(async move { axum::serve(host_listener, routes).await });
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let verifier = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
        let contract =
            super::super::super::contracts::PublicActorContract::new(serde_json::from_str(
                include_str!("../../../sdk/tests/fixtures/public-contract.json"),
            )?)?;
        admin
            .register_deployment(
                &HostLaunchSpec {
                    organization_id: Some("org-test".into()),
                    sandboxes: Default::default(),
                    project_id: "default".into(),
                    source: None,
                    image_ref: "image".into(),
                    code_snapshot: None,
                    working_directory: "/app".into(),
                    actor_entrypoint: None,
                    secret_refs: vec![],
                },
                Some(&contract),
            )
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        if existing {
            let spec = registry.launch_spec("default").await?.unwrap();
            let actor = ActorKey {
                project_id: "default".into(),
                actor_name: "ChatRoom".into(),
                actor_id: "one".into(),
            };
            placements.set_owner(
                &actor.storage_key(),
                HostLease {
                    route: host_origin.clone(),
                    ..test_lease(&HostId::new(format!(
                        "host.v3.{}.fixture",
                        spec.host_config_key()
                    )))
                },
                "north-america-east",
            )?;
        }
        let mut service = ControlPlaneService::new(
            placements.clone(),
            auth,
            registry,
            issuer.clone(),
            Arc::new(InvocationProvisioner {
                route: host_origin,
                placements: placements.clone(),
            }),
        );
        if let Some(routing) = routing {
            service.lifecycle = service.lifecycle.with_routing(routing);
        }
        let hub = super::super::super::socket_bus::MemorySocketHub::default();
        let (origin, server) = serve_replica(&service, &admin, &issuer, &hub).await?;
        let (replica, replica_server) = serve_replica(&service, &admin, &issuer, &hub).await?;
        Ok(Self {
            service,
            placements,
            origin,
            replica,
            admin,
            verifier,
            host,
            servers: vec![server, replica_server, host_server],
        })
    }

    async fn call(&self, token: &str, body: Value) -> Result<reqwest::Response> {
        self.post("invoke", token, body).await
    }

    async fn post(&self, endpoint: &str, token: &str, body: Value) -> Result<reqwest::Response> {
        post_actor(&self.origin, endpoint, token, body).await
    }

    async fn connect(&self) -> Result<Socket> {
        let grant: Value = self
            .post("find-websocket", "api-key", json!({"metadata":{}}))
            .await?
            .error_for_status()?
            .json()
            .await?;
        let (mut socket, _) =
            tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().unwrap()).await?;
        ensure!(
            next_message(&mut socket).await? == Message::Text("ready".into()),
            "socket connection was not admitted"
        );
        Ok(socket)
    }

    async fn find_actor(&self) -> Result<Value> {
        Ok(self
            .post("find-actor", "api-key", json!({}))
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
    }
}

async fn serve_replica(
    service: &ControlPlaneService,
    admin: &AdminService,
    issuer: &ActorJwtIssuer,
    hub: &super::super::super::socket_bus::MemorySocketHub,
) -> Result<(String, tokio::task::JoinHandle<std::io::Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let sockets = super::super::super::socket_gateway::SocketGateway::start(
        hub.join(32768),
        true,
        tokio_util::sync::CancellationToken::new(),
        crate::sandbox::connections::HostConnections::new()?.client,
    )?;
    let mut service = service.clone();
    service.gateway = Some(super::super::super::gateway::Gateway::new(
        issuer,
        origin.clone(),
        sockets,
    )?);
    let routes = super::super::super::public_api::router(service, admin.clone());
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    Ok((origin, server))
}

async fn post_actor(
    origin: &str,
    endpoint: &str,
    token: &str,
    body: Value,
) -> Result<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!(
            "{origin}/v1/projects/default/actors/ChatRoom/one/{endpoint}"
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?)
}

fn broadcast(target: &Value) -> Value {
    json!({"ownerEpoch":target["ownerEpoch"], "effects":[{"type":"broadcast", "message":{"type":"text", "data":"hello"}, "except_connection_ids":[], "tags":[]}]})
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_message(socket: &mut Socket) -> Result<Message> {
    let message = tokio::time::timeout(Duration::from_secs(5), socket.next()).await?;
    Ok(message.context("socket closed")??)
}

struct HostFixture {
    socket_requests: Mutex<Vec<String>>,
    replies: Mutex<VecDeque<(StatusCode, Value)>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<(String, Value)>>,
}

async fn host_invoke(
    State(state): State<Arc<HostFixture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> axum::response::Response {
    state.calls.fetch_add(1, Ordering::SeqCst);
    state
        .requests
        .lock()
        .unwrap()
        .push((headers["authorization"].to_str().unwrap().into(), body));
    let (status, reply) = state
        .replies
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected replay");
    (status, Json(reply)).into_response()
}

async fn host_socket_event(
    State(state): State<Arc<HostFixture>>,
    headers: HeaderMap,
    Json(request): Json<SocketEventRequest>,
) -> Json<SocketEventReply> {
    state
        .socket_requests
        .lock()
        .unwrap()
        .push(headers["authorization"].to_str().unwrap().into());
    let effects = match request.invocation.event {
        ActorSocketEvent::Connect { connection } => vec![ActorSocketEffect::Send {
            connection_id: connection.id,
            message: ActorSocketMessage::Text {
                data: "ready".into(),
            },
        }],
        _ => vec![],
    };
    Json(SocketEventReply::Completed { effects })
}

struct InvocationProvisioner {
    route: String,
    placements: Arc<LocalObjectPlacementStore>,
}
#[async_trait]
impl HostProvisioner for InvocationProvisioner {
    fn host_idle_timeout_ms(&self) -> u64 {
        10_000
    }

    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _: Option<&HostLaunchSpec>,
        _: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::super::super::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }
    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        _: bool,
        _: Option<&crate::bucket::OwnershipHint>,
    ) -> Result<(HostLease, u64)> {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let lease = HostLease {
            route: self.route.clone(),
            session_id: uuid::Uuid::new_v4().to_string(),
            ..test_lease(&HostId::new(format!(
                "host.v3.{}.fixture",
                spec.host_config_key()
            )))
        };
        self.placements
            .set_owner(&actor.storage_key(), lease.clone(), region)?;
        Ok((lease, 1))
    }
    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}
