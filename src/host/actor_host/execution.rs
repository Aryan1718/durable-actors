use super::*;
use crate::actor::{
    ActorDatabase, ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome,
    ActorSocketEffect, ActorSocketOutcome, validate_socket_effects,
};
use crate::host::actor_database::{
    DatabaseInvocation, PreparedActivation, PreparedInvocation, PublicationError,
};
use crate::request_tracking::{RequestCompletion, RequestStage};
use serde_json::Value;
use tracing::{info, warn};

pub(super) struct Completion {
    pub id: u64,
    pub request: ActorRequest,
    pub result: Result<ActorExecutionResult>,
    pub interleaved: bool,
    pub stop: bool,
    pub failure_code: Option<String>,
    pub executed: bool,
}

pub(super) async fn run(
    id: u64,
    mut request: ActorRequest,
    executor: Arc<dyn ActorExecutor>,
    database: Arc<HostDatabase>,
    publisher: Arc<dyn ActorSocketPublisher>,
) -> Completion {
    if matches!(request.operation, ActorOperation::Activate { .. }) {
        let ActorOperation::Activate { actor, reply } = request.operation else {
            unreachable!()
        };
        request.operation = ActorOperation::Method(ActorInvocation {
            actor: actor.clone(),
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        });
        let result = activate(actor, &executor, &database).await;
        let stop = result.is_err();
        request.tracker.state_version(database.state_version());
        let _ = reply.send(result);
        return Completion {
            id,
            request,
            result: Ok(ActorExecutionResult::Completed {
                result: Value::Null,
                effects: vec![],
            }),
            interleaved: false,
            stop,
            failure_code: None,
            executed: false,
        };
    }
    let scope = database.invocation(
        request.operation.invocation().into_owned(),
        request.owner_epoch,
        request.operation.commit_origin(),
    );
    let _guard = ScopeGuard(scope.clone());
    let prepared = scope
        .prepare(
            matches!(request.operation, ActorOperation::Method(_)),
            &mut request.tracker,
        )
        .await;
    let outcome = match prepared {
        Ok(PreparedInvocation::Execute) => {
            execute(&*executor, &request.operation, scope.clone()).await
        }
        Ok(PreparedInvocation::Completed(result)) => {
            request.tracker.state_version(database.state_version());
            return Completion {
                id,
                request,
                result: Ok(result),
                interleaved: false,
                stop: false,
                failure_code: None,
                executed: false,
            };
        }
        Err(error) => {
            return Completion {
                id,
                request,
                result: Err(error),
                interleaved: false,
                stop: true,
                failure_code: None,
                executed: false,
            };
        }
    };
    scope.close();
    request.tracker.mark(RequestStage::ActorExecutionCompleted);
    let interleaved = matches!(outcome, Ok(ActorMethodOutcome::Interleaved(_)));
    let failure_code = match &outcome {
        Ok(ActorMethodOutcome::Failed(failure)) => Some(failure.code.clone()),
        _ => None,
    };
    let result = finish(
        &request.operation,
        outcome,
        &executor,
        &database,
        &scope,
        &publisher,
    )
    .await;
    request
        .tracker
        .mark(RequestStage::StatePublicationCompleted);
    request.tracker.state_version(database.state_version());
    let stop = interleaved && !matches!(&result, Ok(ActorExecutionResult::Completed { .. }));
    Completion {
        id,
        request,
        result,
        interleaved,
        stop,
        failure_code,
        executed: true,
    }
}

struct ScopeGuard(Arc<DatabaseInvocation>);
impl Drop for ScopeGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

async fn activate(
    actor: ActorKey,
    executor: &Arc<dyn ActorExecutor>,
    database: &Arc<HostDatabase>,
) -> Result<ActorActivation> {
    let activation = match database.activate(actor.clone()).await? {
        PreparedActivation::Completed(activation) => return Ok(activation),
        PreparedActivation::Hydrate { activation } => activation,
    };
    let scope = database.invocation(
        ActorInvocation {
            actor: actor.clone(),
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        },
        activation.owner_epoch,
        Default::default(),
    );
    let _guard = ScopeGuard(scope.clone());
    let result = executor
        .hydrate(actor, "activate".into(), scope.clone())
        .await;
    scope.close();
    database.hydrated(activation, result).await
}

async fn execute(
    executor: &dyn ActorExecutor,
    operation: &ActorOperation,
    database: Arc<dyn ActorDatabase>,
) -> Result<ActorMethodOutcome> {
    match operation {
        ActorOperation::Method(invocation) => {
            executor
                .invoke(
                    ActorMethodInvocation {
                        request_id: invocation.request_id.clone(),
                        actor: invocation.actor.clone(),
                        method: invocation.method.clone(),
                        args: invocation.args.clone(),
                    },
                    database,
                )
                .await
        }
        ActorOperation::Socket(invocation) => Ok(
            match executor.handle_socket(invocation.clone(), database).await? {
                ActorSocketOutcome::Interleaved(outcome) => {
                    ActorMethodOutcome::Interleaved(outcome)
                }
                ActorSocketOutcome::Handled { effects } => ActorMethodOutcome::Completed {
                    result: Value::Null,
                    effects,
                },
                ActorSocketOutcome::Failed(failure) => ActorMethodOutcome::Failed(failure),
            },
        ),
        ActorOperation::Activate { .. } => unreachable!(),
    }
}

async fn finish(
    operation: &ActorOperation,
    outcome: Result<ActorMethodOutcome>,
    executor: &Arc<dyn ActorExecutor>,
    database: &Arc<HostDatabase>,
    scope: &DatabaseInvocation,
    publisher: &Arc<dyn ActorSocketPublisher>,
) -> Result<ActorExecutionResult> {
    let (result, effects, sequence) = match outcome {
        Ok(ActorMethodOutcome::Completed { result, effects }) => (result, effects, None),
        Ok(ActorMethodOutcome::Interleaved(outcome)) => {
            (outcome.result, outcome.effects, Some(outcome.sequence))
        }
        Ok(ActorMethodOutcome::Failed(failure)) => {
            if !matches!(
                failure.code.as_str(),
                "actor_method_failed" | "actor_socket_failed"
            ) {
                evict(operation.actor(), executor, database, true).await;
            }
            let code = if failure.code == "resource_exhausted" {
                "resource_exhausted"
            } else {
                "actor_error"
            };
            return Ok(failed(code, failure.message));
        }
        Err(error) => {
            evict(operation.actor(), executor, database, true).await;
            return Ok(failed(
                "actor_error",
                format!("actor executor failed: {error:#}"),
            ));
        }
    };
    if let Err(error) = validate_socket_effects(&effects) {
        evict(operation.actor(), executor, database, true).await;
        return Ok(failed(
            "actor_error",
            format!("actor returned invalid socket effects: {error:#}"),
        ));
    }
    let version = match scope.complete(result.clone(), sequence).await {
        Ok(version) => version,
        Err(error) if error.is::<PublicationError>() || sequence.is_some() => {
            warn!(error = %format!("{error:#}"), "actor completed but state publication could not be confirmed");
            evict(operation.actor(), executor, database, false).await;
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        Err(error) => return Err(error),
    };
    complete_with_state(
        operation.actor(),
        version,
        result,
        effects,
        database,
        publisher,
    )
    .await
}

async fn evict(
    actor: &ActorKey,
    executor: &Arc<dyn ActorExecutor>,
    database: &HostDatabase,
    reset: bool,
) {
    if let Err(error) = executor
        .evict(ActorMethodEviction {
            actor: actor.clone(),
        })
        .await
    {
        warn!(%error, "failed to evict actor after invocation failure");
    }
    if reset {
        let _ = database.reset().await;
    }
}

async fn complete_with_state(
    actor: &ActorKey,
    state_version: u64,
    result: Value,
    effects: Vec<ActorSocketEffect>,
    database: &HostDatabase,
    publisher: &Arc<dyn ActorSocketPublisher>,
) -> Result<ActorExecutionResult> {
    database.ensure_authority()?;
    let (mut automatic, effects): (Vec<_>, Vec<_>) = effects.into_iter().partition(|effect| {
        matches!(
            effect,
            ActorSocketEffect::StateSnapshot { .. } | ActorSocketEffect::StateUpdate { .. }
        )
    });
    for effect in &mut automatic {
        match effect {
            ActorSocketEffect::StateSnapshot { version, .. }
            | ActorSocketEffect::StateUpdate { version, .. } => *version = Some(state_version),
            _ => unreachable!(),
        }
    }
    if !automatic.is_empty()
        && let Err(error) = publisher.publish(actor, automatic).await
    {
        warn!(%error, "committed state notification failed");
        return Ok(ActorExecutionResult::Failed {
            failure: ActorInvocationFailure::outcome_unknown_after_execution(),
        });
    }
    Ok(ActorExecutionResult::Completed { result, effects })
}

pub(super) fn socket_event_name(event: &ActorSocketEvent) -> &'static str {
    match event {
        ActorSocketEvent::Connect { .. } => "onConnect",
        ActorSocketEvent::Message { .. } => "onMessage",
        ActorSocketEvent::Disconnect { .. } => "onDisconnect",
    }
}

fn failed(code: &str, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}

pub(super) fn log_invocation(
    endpoint: &HostEndpoint,
    invocation: &ActorInvocation,
    completion: &RequestCompletion,
    outcome: &Result<ActorExecutionResult>,
) {
    match outcome {
        Ok(result) => log_invocation_result(
            endpoint,
            invocation,
            completion,
            actor_execution_outcome(result),
            actor_execution_failure_code(result).unwrap_or(""),
            None,
        ),
        Err(error) => log_invocation_result(
            endpoint,
            invocation,
            completion,
            "host_error",
            "",
            Some(format!("{error:#}")),
        ),
    }
}

fn log_invocation_result(
    endpoint: &HostEndpoint,
    invocation: &ActorInvocation,
    completion: &RequestCompletion,
    outcome: &str,
    failure_code: &str,
    error: Option<String>,
) {
    info!(
            event = "actor_host_invocation",
            request_id = %invocation.request_id,

            actor_name = %invocation.actor.actor_name,
            actor_id = %invocation.actor.actor_id,
            method = %invocation.method,
            host_id = %endpoint.id,
            started_at_ms = 0,
            host_state = completion.timings.host_state.as_str(),
            routing_ms = completion.timings.routing_ms,
            queue_admitted_at_ms = completion.timings.queue_wait_ms,
            state_cache_checked_at_ms = completion.checkpoints.state_cache_checked_at_ms,
            state_downloaded_at_ms = completion.checkpoints.state_downloaded_at_ms,
            state_decoded_at_ms = completion.checkpoints.state_decoded_at_ms,
            pending_commit_resolved_at_ms = completion.checkpoints.pending_commit_resolved_at_ms,
            actor_execution_completed_at_ms = completion.checkpoints.actor_execution_completed_at_ms,
            state_publication_completed_at_ms = completion.checkpoints.state_publication_completed_at_ms,
            completed_at_ms = completion.timings.duration_ms,
            outcome,
            failure_code,
            error,
            "actor host invocation completed"
    );
}

fn actor_execution_outcome(result: &ActorExecutionResult) -> &'static str {
    match result {
        ActorExecutionResult::Completed { .. } => "completed",
        ActorExecutionResult::Failed { .. } => "failed",
        ActorExecutionResult::Reroute => "reroute",
        ActorExecutionResult::HostUnavailable => "host_unavailable",
    }
}

fn actor_execution_failure_code(result: &ActorExecutionResult) -> Option<&str> {
    match result {
        ActorExecutionResult::Failed { failure } => Some(&failure.code),
        _ => None,
    }
}
