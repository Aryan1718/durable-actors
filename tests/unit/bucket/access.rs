use super::*;

#[test]
fn host_configs_keep_the_format_running_hosts_read() -> Result<()> {
    let legacy = serde_json::json!({
        "bucket": {"type": "gcs", "bucket": "state", "artifact_bucket": "code"},
        "persistence": {"type": "local"},
        "region": "north-america-west",
        "tokens": {
            "ownership": {"accessToken": "owner", "expiresAtMs": 1},
            "state": {"accessToken": "state", "expiresAtMs": 2}
        }
    });
    let config: HostStorageConfig = serde_json::from_value(legacy.clone())?;
    assert!(matches!(config.access, Some(StorageAccess::Gcs(_))));
    assert_eq!(serde_json::to_value(&config)?, legacy);
    Ok(())
}
