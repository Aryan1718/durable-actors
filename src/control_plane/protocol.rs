use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::grpc::proto::{ControlPlaneReply, ControlPlaneRequest};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommand {
    SocketOperation {
        actor: crate::actor::ActorKey,
        operation: super::socket_gateway::SocketOperation,
    },
    RefreshStorageAccess,
    ExpressLog {
        request: crate::bucket::s3::ExpressLogRequest,
    },
    Lifecycle {
        report: crate::actor_lifecycle::LifecycleReport,
    },
    GetAlarm,
    UpdateAlarm {
        timestamp_ms: Option<i64>,
    },
    RequestTraces {
        traces: Vec<crate::request_traces::RequestTrace>,
        dropped: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ControlPlaneCommandReply {
    SocketOperation {
        reply: super::socket_gateway::SocketOperationReply,
    },
    StorageAccess {
        // Flattened so the reply keeps the `tokens` field that already-running hosts read.
        #[serde(flatten)]
        access: Option<crate::bucket::access::StorageAccess>,
        replacement_token: String,
    },
    ExpressLog {
        reply: crate::bucket::s3::ExpressLogReply,
    },
    Alarm {
        timestamp_ms: Option<i64>,
    },
    Unit,
}

pub(crate) fn encode_command(command: ControlPlaneCommand) -> Result<ControlPlaneRequest> {
    Ok(ControlPlaneRequest {
        command_json: serde_json::to_vec(&command)?,
    })
}

pub(crate) fn decode_command(request: ControlPlaneRequest) -> Result<ControlPlaneCommand> {
    Ok(serde_json::from_slice(&request.command_json)?)
}

pub(crate) fn encode_reply(reply: ControlPlaneCommandReply) -> Result<ControlPlaneReply> {
    Ok(ControlPlaneReply {
        reply_json: serde_json::to_vec(&reply)?,
    })
}

pub(crate) fn decode_reply(reply: ControlPlaneReply) -> Result<ControlPlaneCommandReply> {
    Ok(serde_json::from_slice(&reply.reply_json)?)
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/protocol.rs"]
mod tests;
