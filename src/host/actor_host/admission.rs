use super::execution::{self, Completion};
use super::*;
use crate::actor::ActorMethodEviction;
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};

pub(super) async fn run(
    actor: ActorKey,
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<HostDatabase>,
    publisher: Arc<dyn ActorSocketPublisher>,
    mut requests: mpsc::UnboundedReceiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
    mut admission: Option<watch::Receiver<()>>,
) {
    let stopped = database.stopped.clone();
    let mut mailbox = Mailbox {
        actor,
        endpoint,
        executor,
        database,
        publisher,
        completed,
        running: FuturesUnordered::new(),
        next_invocation: 0,
        blocked_on: None,
        interleaved: false,
        hydrating: false,
    };
    let mut open = true;
    while open || !mailbox.running.is_empty() {
        tokio::select! {
            biased;
            Some(completion) = mailbox.running.next(), if !mailbox.running.is_empty() => {
                if !mailbox.executed(completion).await { return; }
            }
            _ = stopped.cancelled() => { mailbox.evict().await; return; }
            permission = admitted(&mut admission), if mailbox.blocked_on.is_some() && !mailbox.hydrating => {
                if permission.is_err() { return; }
                mailbox.interleaved = true;
                mailbox.blocked_on = None;
            }
            request = requests.recv(), if open && mailbox.blocked_on.is_none() => match request {
                Some(request) => {
                    if let Some(admission) = &mut admission { admission.borrow_and_update(); }
                    let accepting = *accepting.borrow();
                    mailbox.admit(request, accepting).await;
                }
                None => open = false,
            },
        }
    }
}

async fn admitted(
    admission: &mut Option<watch::Receiver<()>>,
) -> Result<(), watch::error::RecvError> {
    match admission {
        Some(admission) => admission.changed().await,
        None => std::future::pending().await,
    }
}

struct Mailbox {
    actor: ActorKey,
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<HostDatabase>,
    publisher: Arc<dyn ActorSocketPublisher>,
    completed: mpsc::Sender<ActorCompletion>,
    running: FuturesUnordered<BoxFuture<'static, Completion>>,
    next_invocation: u64,
    blocked_on: Option<u64>,
    interleaved: bool,
    hydrating: bool,
}

impl Mailbox {
    async fn admit(&mut self, mut request: ActorRequest, accepting: bool) {
        if !accepting && !request.operation.is_disconnect() {
            self.finish(request, Ok(ActorExecutionResult::HostUnavailable))
                .await;
            return;
        }
        drop(request.waiting.take());
        request.tracker.admitted();
        self.next_invocation += 1;
        let id = self.next_invocation;
        self.blocked_on = Some(id);
        self.hydrating = matches!(request.operation, ActorOperation::Activate { .. });
        self.running.push(
            execution::run(
                id,
                request,
                self.executor.clone(),
                self.database.clone(),
                self.publisher.clone(),
            )
            .boxed(),
        );
    }

    async fn executed(&mut self, completion: Completion) -> bool {
        let Completion {
            id,
            request,
            result,
            interleaved,
            stop,
            failure_code,
            executed,
        } = completion;
        self.hydrating = false;
        let unexpected = executed
            && self.interleaved
            && !interleaved
            && failure_code.as_deref().is_none_or(|code| {
                !matches!(
                    code,
                    "actor_method_failed"
                        | "actor_socket_failed"
                        | "method_not_found"
                        | "method_not_callable"
                )
            });
        self.interleaved |= interleaved;
        if stop || unexpected || self.database.stopped.is_cancelled() {
            self.evict().await;
            self.finish(
                request,
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                }),
            )
            .await;
            return false;
        }
        self.finish(request, result).await;
        if self.blocked_on == Some(id) {
            self.blocked_on = None;
        }
        true
    }

    async fn evict(&mut self) {
        if let Err(error) = self
            .executor
            .evict(ActorMethodEviction {
                actor: self.actor.clone(),
            })
            .await
        {
            tracing::warn!(%error, "failed to evict stopped actor");
        }
    }

    async fn finish(&mut self, mut request: ActorRequest, result: Result<ActorExecutionResult>) {
        request.tracker.complete(&result);
        log_invocation(
            &self.endpoint,
            &request.operation.invocation(),
            request.tracker.completion(),
            &result,
        );
        let _ = self
            .completed
            .send(ActorCompletion {
                object: self.actor.storage_key(),
                reply: request.reply,
                result,
            })
            .await;
    }
}
