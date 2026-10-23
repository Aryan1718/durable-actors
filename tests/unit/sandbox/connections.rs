use super::*;
use axum::{
    Router,
    extract::{ConnectInfo, State},
    http::Version,
    routing::{get, post},
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

#[tokio::test]
async fn assignment_reuses_the_warmed_http2_connection() -> Result<()> {
    let (send, mut receive) = unbounded_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}", listener.local_addr()?);
    let app = Router::new()
        .route("/healthz", get(observe))
        .route("/assign", post(observe))
        .with_state(send);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let connections = HostConnections::new()?;
    connections
        .prewarm(&SpareHandle {
            name: "spare".into(),
            resource_id: "sandbox".into(),
            route: route.clone(),
            control_route: route.clone(),
            control_token: "secret".into(),
            canonical_region: "north-america-west".into(),
        })
        .await;
    let client = connections.clone().client;
    assert_eq!(
        client
            .post(format!("{route}/assign"))
            .send()
            .await?
            .text()
            .await?,
        "ok"
    );
    let warm = receive.try_recv()?;
    assert_eq!(warm.1, Version::HTTP_2);
    assert_eq!(receive.try_recv()?, warm);
    server.abort();
    Ok(())
}

async fn observe(
    State(send): State<UnboundedSender<(SocketAddr, Version)>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    version: Version,
) -> &'static str {
    send.send((peer, version)).unwrap();
    "ok"
}
