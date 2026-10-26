use super::socket_inventory::SocketInventory;
use crate::{
    actor::{ActorKey, ActorSocketConnection, ActorSocketEffect},
    host_leases::ActorSocketInventory,
    sockets::SocketRegistry,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(super) trait SocketBus: Send + Sync {
    fn registry(&self) -> &SocketRegistry;
    fn generation(&self) -> Option<u64>;
    /// Call before a socket for `actor` opens here.
    async fn watch(&self, actor: &ActorKey) -> Result<()>;
    async fn unwatch(&self, actor: &ActorKey);
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()>;
    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>>;
    async fn count(&self, actor: &ActorKey) -> Result<usize>;
    async fn inventory(&self, project: &str) -> Result<SocketInventory>;
}

#[derive(Clone, Default)]
pub(super) struct MemorySocketHub {
    gateways: Arc<Mutex<Vec<SocketRegistry>>>,
}

impl MemorySocketHub {
    pub(super) fn join(&self, max_connections: usize) -> Arc<dyn SocketBus> {
        let registry = SocketRegistry::with_max_connections(max_connections);
        self.gateways.lock().unwrap().push(registry.clone());
        Arc::new(MemorySocketBus {
            hub: self.clone(),
            registry,
        })
    }

    fn gateways(&self) -> Vec<SocketRegistry> {
        self.gateways.lock().unwrap().clone()
    }
}

struct MemorySocketBus {
    hub: MemorySocketHub,
    registry: SocketRegistry,
}

#[async_trait]
impl SocketBus for MemorySocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.registry
    }

    fn generation(&self) -> Option<u64> {
        Some(0)
    }

    async fn watch(&self, _: &ActorKey) -> Result<()> {
        Ok(())
    }

    async fn unwatch(&self, _: &ActorKey) {}

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        for gateway in self.hub.gateways() {
            gateway.apply(actor, effects.clone()).await;
        }
        Ok(())
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let mut connections = Vec::new();
        for gateway in self.hub.gateways() {
            connections.extend(gateway.connections_with_tag(actor, tag).await);
        }
        Ok(connections)
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let mut count = 0;
        for gateway in self.hub.gateways() {
            count += gateway.count(actor).await;
        }
        Ok(count)
    }

    async fn inventory(&self, project: &str) -> Result<SocketInventory> {
        let mut rooms = Vec::new();
        for gateway in self.hub.gateways() {
            rooms.extend(gateway.inventory(project).await);
        }
        Ok(SocketInventory {
            rooms: merge_inventory(rooms),
            complete: true,
        })
    }
}

fn merge_inventory(rooms: Vec<ActorSocketInventory>) -> Vec<ActorSocketInventory> {
    let mut merged: BTreeMap<(String, String), ActorSocketInventory> = BTreeMap::new();
    for room in rooms {
        let key = (room.actor.actor_name.clone(), room.actor.actor_id.clone());
        match merged.get_mut(&key) {
            Some(entry) => entry.connections.extend(room.connections),
            None => {
                merged.insert(key, room);
            }
        }
    }
    let mut rooms: Vec<_> = merged.into_values().collect();
    for room in &mut rooms {
        room.connections.sort_by(|a, b| a.id.cmp(&b.id));
    }
    rooms
}

const ANSWER_TIMEOUT: Duration = Duration::from_secs(2);
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const AUTO_RESPONSE_TTL: Duration = Duration::from_secs(7 * 24 * 3600);

pub(super) struct RedisSocketBus {
    shared: Arc<Shared>,
    commands: mpsc::UnboundedSender<Command>,
    watched: Mutex<HashMap<ActorKey, usize>>,
}

struct Shared {
    redis: crate::redis_client::Redis,
    registry: SocketRegistry,
    namespace: String,
    replies: String,
    connected: AtomicBool,
    generation: AtomicU64,
    pending: Mutex<HashMap<String, mpsc::UnboundedSender<Answer>>>,
    subscribed: Mutex<Vec<ActorKey>>,
}

enum Command {
    Subscribe {
        actor: ActorKey,
        load: bool,
        done: oneshot::Sender<Result<()>>,
    },
    Unsubscribe(String),
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Envelope {
    Effects {
        actor: ActorKey,
        effects: Vec<ActorSocketEffect>,
    },
    Question {
        id: String,
        reply_to: String,
        question: Question,
    },
    Answer {
        id: String,
        answer: Answer,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Question {
    Count {
        actor: ActorKey,
    },
    Connections {
        actor: ActorKey,
        tag: Option<String>,
    },
    Inventory {
        project: String,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Answer {
    Count {
        count: usize,
    },
    Connections {
        connections: Vec<ActorSocketConnection>,
    },
    Inventory {
        rooms: Vec<ActorSocketInventory>,
    },
}

#[derive(Serialize, Deserialize)]
struct AutoResponse {
    request: Option<String>,
    response: Option<String>,
}

impl RedisSocketBus {
    pub(super) fn start(
        redis: crate::redis_client::Redis,
        namespace: &str,
        max_connections: usize,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) -> Arc<dyn SocketBus> {
        let shared = Arc::new(Shared {
            redis,
            registry: SocketRegistry::with_max_connections(max_connections),
            namespace: namespace.to_owned(),
            replies: format!("{namespace}:replies:{}", uuid::Uuid::new_v4()),
            connected: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            pending: Mutex::default(),
            subscribed: Mutex::default(),
        });
        let (commands, receiver) = mpsc::unbounded_channel();
        tokio::spawn(shared.clone().listen(receiver, accepts_sockets, stop));
        Arc::new(Self {
            shared,
            commands,
            watched: Mutex::default(),
        })
    }

    async fn ask(&self, channel: &str, question: Question) -> Result<Vec<Answer>> {
        let (answers, complete) = self.ask_partial(channel, question).await?;
        ensure!(complete, "a socket gateway did not answer");
        Ok(answers)
    }

    async fn ask_partial(&self, channel: &str, question: Question) -> Result<(Vec<Answer>, bool)> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let id = uuid::Uuid::new_v4().to_string();
        let (sender, mut answers) = mpsc::unbounded_channel();
        self.shared
            .pending
            .lock()
            .unwrap()
            .insert(id.clone(), sender);
        let asked = async {
            let receivers = self
                .shared
                .publish(
                    channel,
                    &Envelope::Question {
                        id: id.clone(),
                        reply_to: self.shared.replies.clone(),
                        question,
                    },
                )
                .await?;
            let mut received = Vec::new();
            let deadline = tokio::time::Instant::now() + ANSWER_TIMEOUT;
            while received.len() < receivers {
                match tokio::time::timeout_at(deadline, answers.recv()).await {
                    Ok(Some(answer)) => received.push(answer),
                    _ => break,
                }
            }
            let complete = received.len() == receivers;
            Ok((received, complete))
        }
        .await;
        self.shared.pending.lock().unwrap().remove(&id);
        asked
    }
}

impl Shared {
    fn actor_channel(&self, actor: &ActorKey) -> String {
        format!("{}:actor:{}", self.namespace, actor.storage_key().as_str())
    }

    fn gateways_channel(&self) -> String {
        format!("{}:gateways", self.namespace)
    }

    fn auto_response_key(&self, actor: &ActorKey) -> String {
        format!(
            "{}:auto-response:{}",
            self.namespace,
            actor.storage_key().as_str()
        )
    }

    async fn publish(&self, channel: &str, envelope: &Envelope) -> Result<usize> {
        let mut command = redis::cmd("PUBLISH");
        command.arg(channel).arg(serde_json::to_string(envelope)?);
        self.redis.run::<usize>(command).await
    }

    // Runs on the listener after subscribing, so changes published meanwhile apply after it.
    async fn load_auto_response(&self, actor: &ActorKey) -> Result<()> {
        let mut get = redis::cmd("GET");
        get.arg(self.auto_response_key(actor));
        let AutoResponse { request, response } = match self.redis.run::<Option<String>>(get).await?
        {
            Some(stored) => serde_json::from_str(&stored)?,
            None => AutoResponse {
                request: None,
                response: None,
            },
        };
        self.registry
            .apply(
                actor,
                vec![ActorSocketEffect::SetAutoResponse { request, response }],
            )
            .await;
        Ok(())
    }

    /// Each reconnect starts a new generation, closing sockets that may have missed publishes.
    async fn listen(
        self: Arc<Self>,
        mut commands: mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: CancellationToken,
    ) {
        loop {
            let result = self
                .clone()
                .session(&mut commands, accepts_sockets, &stop)
                .await;
            self.connected.store(false, Ordering::SeqCst);
            match result {
                Ok(()) => return,
                Err(error) => tracing::warn!(error = %format!("{error:#}"),
                    "socket bus lost its Redis subscription; its sockets close and it reconnects"),
            }
            self.registry
                .close_all(1012, "socket gateway lost its subscription")
                .await;
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    async fn session(
        self: Arc<Self>,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        accepts_sockets: bool,
        stop: &CancellationToken,
    ) -> Result<()> {
        let (mut sink, mut messages) = self.redis.pubsub().await?.split();
        sink.subscribe(&self.replies).await?;
        if accepts_sockets {
            sink.subscribe(self.gateways_channel()).await?;
        }
        let resubscribe = self.subscribed.lock().unwrap().clone();
        for actor in resubscribe {
            sink.subscribe(self.actor_channel(&actor)).await?;
            self.load_auto_response(&actor).await?;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                message = messages.next() => {
                    let message = message.context("Redis closed the pub/sub connection")?;
                    self.clone().receive(message.get_payload_bytes()).await;
                }
                command = commands.recv() => match command {
                    None => return Ok(()),
                    Some(Command::Subscribe { actor, load, done }) => {
                        let channel = self.actor_channel(&actor);
                        if let Err(error) = sink.subscribe(&channel).await {
                            let _ = done.send(Err(error.into()));
                            anyhow::bail!("subscribe to {channel} failed");
                        }
                        let loaded = match load {
                            true => self.load_auto_response(&actor).await,
                            false => Ok(()),
                        };
                        let _ = done.send(loaded);
                    }
                    Some(Command::Unsubscribe(channel)) => sink.unsubscribe(&channel).await?,
                },
            }
        }
    }

    async fn receive(self: Arc<Self>, payload: &[u8]) {
        let envelope = match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                tracing::warn!(%error, "ignored an unreadable socket bus message");
                return;
            }
        };
        match envelope {
            // In order, on the listener: an actor's effects apply as published.
            Envelope::Effects { actor, effects } => self.registry.apply(&actor, effects).await,
            Envelope::Question {
                id,
                reply_to,
                question,
            } => {
                tokio::spawn(async move {
                    let answer = match question {
                        Question::Count { actor } => Answer::Count {
                            count: self.registry.count(&actor).await,
                        },
                        Question::Connections { actor, tag } => Answer::Connections {
                            connections: self
                                .registry
                                .connections_with_tag(&actor, tag.as_deref())
                                .await,
                        },
                        Question::Inventory { project } => Answer::Inventory {
                            rooms: self.registry.inventory(&project).await,
                        },
                    };
                    if let Err(error) = self
                        .publish(&reply_to, &Envelope::Answer { id, answer })
                        .await
                    {
                        tracing::warn!(error = %format!("{error:#}"), "socket bus answer failed");
                    }
                });
            }
            Envelope::Answer { id, answer } => {
                if let Some(waiting) = self.pending.lock().unwrap().get(&id) {
                    let _ = waiting.send(answer);
                }
            }
        }
    }
}

#[async_trait]
impl SocketBus for RedisSocketBus {
    fn registry(&self) -> &SocketRegistry {
        &self.shared.registry
    }

    fn generation(&self) -> Option<u64> {
        self.shared
            .connected
            .load(Ordering::SeqCst)
            .then(|| self.shared.generation.load(Ordering::SeqCst))
    }

    async fn watch(&self, actor: &ActorKey) -> Result<()> {
        ensure!(
            self.shared.connected.load(Ordering::SeqCst),
            "socket bus is reconnecting"
        );
        let (done, subscribed) = oneshot::channel();
        {
            // Sent under the lock, so commands reach Redis in the order the counts changed.
            let mut watched = self.watched.lock().unwrap();
            let sockets = watched.entry(actor.clone()).or_default();
            *sockets += 1;
            let load = *sockets == 1;
            if load {
                self.shared.subscribed.lock().unwrap().push(actor.clone());
            }
            // Each watch waits for its own subscribe, even one that arrives while another is in flight.
            let _ = self.commands.send(Command::Subscribe {
                actor: actor.clone(),
                load,
                done,
            });
        }
        let subscribed = tokio::time::timeout(SUBSCRIBE_TIMEOUT, subscribed).await;
        if !matches!(subscribed, Ok(Ok(Ok(())))) {
            self.unwatch(actor).await;
            anyhow::bail!("socket bus could not subscribe to the actor");
        }
        Ok(())
    }

    async fn unwatch(&self, actor: &ActorKey) {
        let mut watched = self.watched.lock().unwrap();
        let Some(sockets) = watched.get_mut(actor) else {
            return;
        };
        *sockets -= 1;
        if *sockets == 0 {
            watched.remove(actor);
            self.shared
                .subscribed
                .lock()
                .unwrap()
                .retain(|subscribed| subscribed != actor);
            let _ = self
                .commands
                .send(Command::Unsubscribe(self.shared.actor_channel(actor)));
        }
    }

    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        if let Some((request, response)) = effects.iter().rev().find_map(|effect| match effect {
            ActorSocketEffect::SetAutoResponse { request, response } => {
                Some((request.clone(), response.clone()))
            }
            _ => None,
        }) {
            let key = self.shared.auto_response_key(actor);
            let command = if request.is_some() && response.is_some() {
                let mut set = redis::cmd("SET");
                set.arg(key)
                    .arg(serde_json::to_string(&AutoResponse { request, response })?)
                    .arg("PX")
                    .arg(AUTO_RESPONSE_TTL.as_millis() as u64);
                set
            } else {
                let mut delete = redis::cmd("DEL");
                delete.arg(key);
                delete
            };
            self.shared.redis.run::<redis::Value>(command).await?;
        }
        self.shared
            .publish(
                &self.shared.actor_channel(actor),
                &Envelope::Effects {
                    actor: actor.clone(),
                    effects,
                },
            )
            .await?;
        Ok(())
    }

    async fn connections(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Result<Vec<ActorSocketConnection>> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Connections {
                    actor: actor.clone(),
                    tag: tag.map(str::to_owned),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .flat_map(|answer| match answer {
                Answer::Connections { connections } => connections,
                _ => vec![],
            })
            .collect())
    }

    async fn count(&self, actor: &ActorKey) -> Result<usize> {
        let answers = self
            .ask(
                &self.shared.actor_channel(actor),
                Question::Count {
                    actor: actor.clone(),
                },
            )
            .await?;
        Ok(answers
            .into_iter()
            .map(|answer| match answer {
                Answer::Count { count } => count,
                _ => 0,
            })
            .sum())
    }

    async fn inventory(&self, project: &str) -> Result<SocketInventory> {
        let (answers, complete) = self
            .ask_partial(
                &self.shared.gateways_channel(),
                Question::Inventory {
                    project: project.to_owned(),
                },
            )
            .await?;
        let rooms = answers
            .into_iter()
            .flat_map(|answer| match answer {
                Answer::Inventory { rooms } => rooms,
                _ => vec![],
            })
            .collect();
        Ok(SocketInventory {
            rooms: merge_inventory(rooms),
            complete,
        })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/socket_bus.rs"]
mod tests;
