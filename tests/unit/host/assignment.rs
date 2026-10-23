use super::*;
use crate::host::HostId;

#[tokio::test]
async fn assignment_is_authenticated_single_use_and_waits_for_readiness() -> anyhow::Result<()> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router("secret".into(), send))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let health = client
        .get(url.replace("/assign", "/healthz"))
        .send()
        .await?;
    assert_eq!(health.status(), 200);
    let response = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await?;
    assert_eq!(response.status(), 401);
    let request = client
        .post(&url)
        .bearer_auth("secret")
        .json(&serde_json::json!({"actor": "one"}));
    let call = tokio::spawn(async move { request.send().await });
    let assigned = tokio::time::timeout(std::time::Duration::from_secs(1), receive).await??;
    assert_eq!(assigned.environment["actor"], "one");
    assert!(
        !call.is_finished(),
        "assignment must wait for hydration and ownership"
    );
    let duplicate = client
        .post(&url)
        .bearer_auth("secret")
        .json(&serde_json::json!({}))
        .send()
        .await?;
    assert_eq!(duplicate.status(), 409);
    assert!(
        assigned
            .ready
            .send(super::super::process::HostReadiness {
                host_id: HostId::new("host.v3.test.one"),
                session_id: "session".into(),
                route: "https://host.test".into(),
                canonical_region: "north-america-east".into(),
                owner_epoch: 42,
                lease: crate::host_leases::HostLease {
                    id: HostId::new("host.v3.test.one"),
                    session_id: "session".into(),
                    route: "https://host.test".into(),
                    expires_at_ms: 60_000,
                },
            })
            .is_ok()
    );
    let reply = call.await??;
    assert_eq!(reply.status(), 200);
    assert_eq!(reply.json::<serde_json::Value>().await?["ownerEpoch"], 42);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn failed_initialization_never_reports_ready() -> anyhow::Result<()> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/assign", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router("secret".into(), send))
            .await
            .unwrap();
    });
    let request = reqwest::Client::new()
        .post(url)
        .bearer_auth("secret")
        .json(&serde_json::json!({}));
    let call = tokio::spawn(async move { request.send().await });
    drop(receive.await?);
    assert_eq!(call.await??.status(), 503);
    server.abort();
    Ok(())
}
