use super::*;

#[test]
fn storage_access_refresh_roundtrips_through_the_control_plane_wire_format() -> Result<()> {
    let encoded = encode_command(ControlPlaneCommand::RefreshStorageAccess)?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encoded.command_json)?,
        serde_json::json!({"type":"refresh_storage_access"})
    );
    assert!(matches!(
        decode_command(encoded)?,
        ControlPlaneCommand::RefreshStorageAccess
    ));
    Ok(())
}

#[test]
fn storage_access_replies_keep_the_format_running_hosts_read() -> Result<()> {
    let legacy = serde_json::json!({
        "type": "storage_access",
        "tokens": {
            "ownership": {"accessToken": "owner", "expiresAtMs": 1},
            "state": {"accessToken": "state", "expiresAtMs": 2}
        },
        "replacement_token": "host"
    });
    let reply = decode_reply(ControlPlaneReply {
        reply_json: serde_json::to_vec(&legacy)?,
    })?;
    assert!(matches!(
        &reply,
        ControlPlaneCommandReply::StorageAccess {
            access: Some(crate::bucket::access::StorageAccess::Gcs(_)),
            ..
        }
    ));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&encode_reply(reply)?.reply_json)?,
        legacy
    );
    Ok(())
}
