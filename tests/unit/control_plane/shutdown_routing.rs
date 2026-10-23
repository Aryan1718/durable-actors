use super::*;
use crate::actor_lifecycle::{
    LifecycleKind, LifecycleReport,
    routing::{RedisRoutingHints, RoutingHints},
};

#[tokio::test]
async fn shutdown_hints_protect_discovery_and_cached_http_and_socket_routes() -> Result<()> {
    handoff(None).await
}

#[tokio::test]
async fn redis_shutdown_hints_reach_another_control_plane() -> Result<()> {
    let Ok(url) = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL") else {
        return Ok(());
    };
    let redis = crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url,
        ca_cert: None,
    })?;
    handoff(Some(Arc::new(RedisRoutingHints::new(
        redis,
        &format!("test:shutdown:{}", uuid::Uuid::new_v4()),
        Duration::from_secs(60),
    ))))
    .await
}

async fn handoff(routing: Option<Arc<dyn RoutingHints>>) -> Result<()> {
    let fixture = Fixture::start_with_routing(vec![], true, routing).await?;
    let mut socket = fixture.connect().await?;
    let target = fixture.find_actor().await?;
    let token = target["token"].as_str().unwrap();
    let principal = fixture
        .verifier
        .authenticate_authorization(&format!("Bearer {token}"))?;
    report(&fixture, &principal, LifecycleKind::Draining, 1).await?;

    let cached = post_actor(
        &fixture.replica,
        "invoke",
        token,
        json!({"requestId":"cached", "ownerEpoch":1, "method":"clear", "args":[]}),
    )
    .await?
    .error_for_status()?
    .json::<Value>()
    .await?;
    assert_eq!(
        cached,
        json!({"type":"not_executed", "reason":"host_unavailable"})
    );
    assert_eq!(fixture.host.calls.load(Ordering::SeqCst), 0);
    socket.send(Message::Text("during-shutdown".into())).await?;
    let mut pending = Box::pin(post_actor(
        &fixture.replica,
        "find-actor",
        "api-key",
        json!({}),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(350), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(fixture.host.socket_requests.lock().unwrap().len(), 1);

    report(&fixture, &principal, LifecycleKind::Stopped, 2).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(350), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(fixture.host.socket_requests.lock().unwrap().len(), 1);
    let key = principal.actor.storage_key();
    let mut placement = fixture.placements.get_owner(&key).await?.unwrap();
    placement.lease.expires_at_ms = 0;
    fixture
        .placements
        .set_owner(&key, placement.lease, &placement.home_region)?;

    let replacement: Value = tokio::time::timeout(Duration::from_secs(3), pending)
        .await??
        .error_for_status()?
        .json()
        .await?;
    let next = fixture.verifier.authenticate_authorization(&format!(
        "Bearer {}",
        replacement["token"].as_str().unwrap()
    ))?;
    assert_ne!(principal.session_id, next.session_id);
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.host.socket_requests.lock().unwrap().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let requests = fixture.host.socket_requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    let socket_host = fixture.verifier.authenticate_authorization(&requests[1])?;
    assert_ne!(principal.session_id, socket_host.session_id);

    report(&fixture, &principal, LifecycleKind::Heartbeat, 1).await?;
    report(&fixture, &principal, LifecycleKind::Draining, 1).await?;
    let lifecycle = &fixture.service.lifecycle;
    assert_eq!(
        lifecycle
            .routing(&key, principal.host_id.as_str(), &principal.session_id, 1)
            .await,
        Some(LifecycleKind::Stopped)
    );
    assert_eq!(
        lifecycle
            .routing(&key, next.host_id.as_str(), &next.session_id, 1)
            .await,
        None
    );
    assert_eq!(
        lifecycle
            .routing(&key, principal.host_id.as_str(), &principal.session_id, 2)
            .await,
        None
    );
    let other = ActorKey {
        project_id: "other-project".into(),
        ..principal.actor.clone()
    };
    assert_eq!(
        lifecycle
            .routing(
                &other.storage_key(),
                principal.host_id.as_str(),
                &principal.session_id,
                1
            )
            .await,
        None
    );
    Ok(())
}

async fn report(
    fixture: &Fixture,
    principal: &ActorPrincipal,
    kind: LifecycleKind,
    sequence: u64,
) -> Result<()> {
    fixture
        .service
        .execute_command(
            principal,
            ControlPlaneCommand::Lifecycle {
                report: LifecycleReport {
                    kind,
                    owner_epoch: 1,
                    sequence,
                    started_at_ms: 1000,
                    occurred_at_ms: sequence * 1000,
                    resources: Default::default(),
                    resident: Some(kind != LifecycleKind::Stopped),
                    waiting: vec![],
                },
            },
        )
        .await?;
    Ok(())
}
