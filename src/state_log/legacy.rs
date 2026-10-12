use super::{RequestCompletion, SqliteSnapshot, StateAttribution, StateSnapshot};
use serde::Deserialize;
use serde_json::Value;

pub(super) fn decode(bytes: &[u8]) -> serde_json::Result<StateSnapshot> {
    let legacy: LegacySnapshot = serde_json::from_slice(bytes)?;
    Ok(StateSnapshot {
        attribution: legacy.attribution,
        state_version: legacy.state_version,
        owner_epoch: legacy.owner_epoch,
        request_id: legacy.request_id,
        sqlite: legacy.sqlite,
        completion: Some(RequestCompletion {
            result: legacy.result,
        }),
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LegacySnapshot {
    attribution: Option<StateAttribution>,
    state_version: u64,
    owner_epoch: u64,
    request_id: String,
    sqlite: SqliteSnapshot,
    #[serde(deserialize_with = "Value::deserialize")]
    result: Value,
}

#[cfg(test)]
#[path = "../../tests/unit/state_log_legacy.rs"]
mod tests;
