use crate::{
    actor::{ActorInvocation, ActorSocketInvocation},
    control_plane::session::InvocationGrant,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AssignmentRequest {
    pub environment: HashMap<String, String>,
    pub operation: InitialOperation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct InitialOperation {
    pub request: InitialRequest,
    pub routing_ms: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum InitialRequest {
    Invoke {
        invocation: ActorInvocation,
        grant: Option<InvocationGrant>,
    },
    Socket {
        invocation: ActorSocketInvocation,
    },
}

#[derive(Debug)]
pub(crate) struct AssignmentOutcomeUnknown;

impl std::fmt::Display for AssignmentOutcomeUnknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("assignment operation may have executed; its outcome could not be confirmed")
    }
}

impl std::error::Error for AssignmentOutcomeUnknown {}
