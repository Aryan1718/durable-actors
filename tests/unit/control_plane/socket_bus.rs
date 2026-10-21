use super::*;
use crate::{
    actor::{ActorSocketMessage, ActorSocketTagMatch},
    sockets::{OutboundMessage, SocketReceiver, socket_channel},
};

fn actor(id: &str) -> ActorKey {
    ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: id.into(),
    }
}

async fn open(
    registry: &SocketRegistry,
    actor: &ActorKey,
    id: &str,
    tags: &[&str],
) -> SocketReceiver {
    let (sender, receiver) = socket_channel();
    let connection = ActorSocketConnection {
        id: id.into(),
        metadata: serde_json::json!({ "socket": id }),
        tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
    };
    assert!(registry.insert(actor, connection, sender).await);
    registry.activate(actor, id).await;
    receiver
}

fn broadcast(data: &str) -> Vec<ActorSocketEffect> {
    vec![ActorSocketEffect::Broadcast {
        message: ActorSocketMessage::Text { data: data.into() },
        except_connection_ids: vec![],
        tags: vec![],
        tag_match: ActorSocketTagMatch::default(),
    }]
}

async fn received(receiver: &mut SocketReceiver) -> Result<String> {
    match tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await?
        .context("socket closed")?
    {
        OutboundMessage::Message(ActorSocketMessage::Text { data }) => Ok(data),
        _ => anyhow::bail!("unexpected outbound message"),
    }
}

#[tokio::test]
async fn memory_publishes_reach_every_gateway_holding_the_actors_sockets() -> Result<()> {
    let hub = MemorySocketHub::default();
    let (first, second) = (hub.join(10), hub.join(10));
    let room = actor("one");
    let mut a = open(first.registry(), &room, "a", &["red"]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;
    second.publish(&room, broadcast("hello")).await?;
    assert_eq!(received(&mut a).await?, "hello");
    assert_eq!(received(&mut b).await?, "hello");
    assert_eq!(first.count(&room).await?, 2);
    let red = first.connections(&room, Some("red")).await?;
    assert_eq!(red.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a"]);
    let inventory = second.inventory("p").await?;
    assert!(inventory.complete);
    assert_eq!(inventory.rooms.len(), 1);
    assert_eq!(inventory.rooms[0].connections.len(), 2);
    Ok(())
}

async fn redis_bus(
    url: &str,
    namespace: &str,
    accepts_sockets: bool,
) -> Result<Arc<dyn SocketBus>> {
    let redis = crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url: url.to_owned(),
        ca_cert: None,
    })?;
    let bus = RedisSocketBus::start(
        redis,
        namespace,
        10,
        accepts_sockets,
        CancellationToken::new(),
    );
    connected(&bus).await?;
    Ok(bus)
}

async fn connected(bus: &Arc<dyn SocketBus>) -> Result<u64> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(generation) = bus.generation() {
                return generation;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("socket bus did not connect")
}

fn test_redis() -> Option<(String, String)> {
    let url = std::env::var("DURABLE_ACTORS_TEST_REDIS_URL").ok()?;
    Some((url, format!("test:{}", uuid::Uuid::new_v4())))
}

#[tokio::test]
async fn redis_publishes_reach_every_gateway_holding_the_actors_sockets() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let first = redis_bus(&url, &namespace, true).await?;
    let second = redis_bus(&url, &namespace, true).await?;
    let control = redis_bus(&url, &namespace, false).await?;
    let room = actor("one");
    first.watch(&room).await?;
    second.watch(&room).await?;
    let mut a = open(first.registry(), &room, "a", &["red"]).await;
    let mut b = open(second.registry(), &room, "b", &[]).await;

    control.publish(&room, broadcast("hello")).await?;
    assert_eq!(received(&mut a).await?, "hello");
    assert_eq!(received(&mut b).await?, "hello");
    assert_eq!(control.count(&room).await?, 2);
    let red = control.connections(&room, Some("red")).await?;
    assert_eq!(red.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a"]);
    let inventory = control.inventory("p").await?;
    assert!(inventory.complete);
    assert_eq!(inventory.rooms.len(), 1);
    assert_eq!(inventory.rooms[0].connections.len(), 2);

    second.registry().remove(&room, "b").await;
    second.unwatch(&room).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(control.count(&room).await?, 1);
    assert_eq!(control.count(&actor("nobody")).await?, 0);
    Ok(())
}

#[tokio::test]
async fn redis_auto_responses_reach_gateways_that_open_sockets_later() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let gateway = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    control
        .publish(
            &room,
            vec![ActorSocketEffect::SetAutoResponse {
                request: Some("ping".into()),
                response: Some("pong".into()),
            }],
        )
        .await?;
    gateway.watch(&room).await?;
    assert_eq!(
        gateway
            .registry()
            .auto_response(&room, "ping")
            .await
            .as_deref(),
        Some("pong")
    );
    Ok(())
}

#[tokio::test]
async fn redis_auto_responses_cleared_while_unwatched_do_not_return() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let gateway = redis_bus(&url, &namespace, true).await?;
    let room = actor("one");
    let set = |pair: Option<(&str, &str)>| {
        vec![ActorSocketEffect::SetAutoResponse {
            request: pair.map(|(request, _)| request.into()),
            response: pair.map(|(_, response)| response.into()),
        }]
    };
    control.publish(&room, set(Some(("ping", "pong")))).await?;
    gateway.watch(&room).await?;
    gateway.unwatch(&room).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    control.publish(&room, set(None)).await?;
    gateway.watch(&room).await?;
    assert_eq!(gateway.registry().auto_response(&room, "ping").await, None);
    Ok(())
}

#[tokio::test]
async fn redis_questions_fail_or_report_incomplete_when_a_gateway_does_not_answer() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let control = redis_bus(&url, &namespace, false).await?;
    let room = actor("one");
    let redis = crate::redis_client::Redis::connect(&crate::redis_client::RedisConfig {
        url,
        ca_cert: None,
    })?;
    let mut silent = redis.pubsub().await?;
    silent.subscribe(format!("{namespace}:gateways")).await?;
    silent
        .subscribe(format!("{namespace}:actor:{}", room.storage_key().as_str()))
        .await?;
    assert!(!control.inventory("p").await?.complete);
    assert!(control.count(&room).await.is_err());
    Ok(())
}

struct Proxy {
    address: std::net::SocketAddr,
    connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(target: String) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let connections: Arc<Mutex<Vec<tokio::task::AbortHandle>>> = Arc::default();
        let tracked = connections.clone();
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let target = target.clone();
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                tracked.lock().unwrap().push(task.abort_handle());
            }
        });
        Ok(Self {
            address,
            connections,
        })
    }

    fn cut(&self) {
        for connection in self.connections.lock().unwrap().drain(..) {
            connection.abort();
        }
    }
}

#[tokio::test]
async fn redis_a_lost_subscription_starts_a_new_generation() -> Result<()> {
    let Some((url, namespace)) = test_redis() else {
        return Ok(());
    };
    let target = url
        .strip_prefix("redis://")
        .context("test Redis URL must be redis://host:port")?
        .trim_end_matches('/')
        .to_owned();
    let proxy = Proxy::start(target).await?;
    let gateway = redis_bus(&format!("redis://{}", proxy.address), &namespace, true).await?;
    let before = gateway.generation().context("connected")?;
    proxy.cut();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.generation() == Some(before) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("the bus did not notice its lost subscription")?;
    let after = connected(&gateway).await?;
    assert!(after > before);
    Ok(())
}
