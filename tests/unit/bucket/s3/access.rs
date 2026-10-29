use super::*;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

fn actor(id: &str) -> crate::actor::ActorKey {
    crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: id.into(),
    }
}

#[test]
fn explicit_denies_confine_ownership_and_state_even_with_resource_grants() -> Result<()> {
    let owner = ownership_policy("state", &actor("one"))?;
    let state = state_policy("state", &crate::storage_paths::snapshots(&actor("one"))?)?;
    let own_key = crate::storage_paths::owner(&actor("one").storage_key())?;
    let other_key = crate::storage_paths::owner(&actor("two").storage_key())?;
    let prefix = crate::storage_paths::snapshots(&actor("one"))?;
    let other_prefix = crate::storage_paths::snapshots(&actor("two"))?;
    for action in ["s3:GetObject", "s3:PutObject"] {
        assert!(explicitly_allows(
            &owner,
            action,
            &format!("arn:aws:s3:::state/{own_key}"),
            None
        ));
    }
    assert!(!explicitly_denies(
        &owner,
        "s3:PutObject",
        &format!("arn:aws:s3:::state/{own_key}"),
        None
    ));
    assert!(explicitly_denies(
        &owner,
        "s3:GetObject",
        &format!("arn:aws:s3:::state/{other_key}"),
        None
    ));
    assert!(explicitly_denies(
        &owner,
        "s3:DeleteObject",
        &format!("arn:aws:s3:::state/{own_key}"),
        None
    ));
    assert!(explicitly_denies(
        &owner,
        "sts:AssumeRole",
        "arn:aws:iam::123456789012:role/broad",
        None
    ));
    for action in ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"] {
        assert!(explicitly_allows(
            &state,
            action,
            &format!("arn:aws:s3:::state/{prefix}part"),
            None
        ));
        assert!(!explicitly_denies(
            &state,
            action,
            &format!("arn:aws:s3:::state/{prefix}part"),
            None
        ));
        assert!(explicitly_denies(
            &state,
            action,
            &format!("arn:aws:s3:::state/{other_prefix}part"),
            None
        ));
    }
    assert!(explicitly_allows(
        &state,
        "s3:ListBucket",
        "arn:aws:s3:::state",
        Some(&prefix)
    ));
    assert!(!explicitly_denies(
        &state,
        "s3:ListBucket",
        "arn:aws:s3:::state",
        Some(&prefix)
    ));
    assert!(explicitly_denies(
        &state,
        "s3:ListBucket",
        "arn:aws:s3:::state",
        Some(&other_prefix)
    ));
    assert!(explicitly_denies(
        &state,
        "s3:ListBucket",
        "arn:aws:s3:::state",
        None
    ));
    assert!(explicitly_denies(
        &state,
        "s3:ListBucket",
        "arn:aws:s3:::elsewhere",
        Some(&prefix)
    ));
    Ok(())
}

#[test]
fn artifact_batches_keep_exact_keys_and_fit_sts_limits() -> Result<()> {
    let keys: Vec<_> = (0..50)
        .map(|index| format!("durable-actors/v3/artifacts/deployment/{index:04}/actors.mjs"))
        .collect();
    let batches = artifact_policies("code", &keys)?;
    assert!(batches.len() > 1);
    let collected: Vec<_> = batches
        .iter()
        .flat_map(|batch| batch.keys.clone())
        .collect();
    assert_eq!(collected, keys);
    for batch in &batches {
        assert!(batch.policy.as_str().len() <= POLICY_LIMIT);
        for key in &batch.keys {
            let resource = format!("arn:aws:s3:::code/{key}");
            assert!(explicitly_allows(
                &batch.policy,
                "s3:GetObject",
                &resource,
                None
            ));
            assert!(!explicitly_denies(
                &batch.policy,
                "s3:GetObject",
                &resource,
                None
            ));
            assert!(explicitly_denies(
                &batch.policy,
                "s3:PutObject",
                &resource,
                None
            ));
        }
        assert!(explicitly_denies(
            &batch.policy,
            "s3:GetObject",
            "arn:aws:s3:::code/undeclared",
            None
        ));
        assert!(explicitly_denies(
            &batch.policy,
            "s3:ListBucket",
            "arn:aws:s3:::code",
            None
        ));
    }
    assert!(artifact_policies("code", &[])?.is_empty());
    Ok(())
}

#[test]
fn ambiguous_resources_and_oversized_policies_fail_closed() -> Result<()> {
    for key in ["path/*", "path/?", "path/${aws:username}", "bad\nkey"] {
        assert!(artifact_policies("code", &[key.into()]).is_err());
    }
    for bucket in ["*", "code?", "code/${bad}", "bad:bucket"] {
        assert!(ownership_policy(bucket, &actor("one")).is_err());
    }
    let long_key = "a".repeat(1024);
    assert!(artifact_policies("code", &[long_key]).is_err());
    Ok(())
}

struct FixedClock;

impl crate::clock::Clock for FixedClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(1_000_000)
    }
}

struct RecordingSts {
    requests: Mutex<Vec<AssumeRoleRequest>>,
    failures: AtomicUsize,
}

#[async_trait]
impl AssumeRole for RecordingSts {
    async fn assume_role(&self, request: AssumeRoleRequest) -> Result<TemporaryCredentials> {
        self.requests.lock().unwrap().push(request);
        if self.failures.fetch_add(1, Ordering::SeqCst) > 0 {
            anyhow::bail!("secret-sentinel");
        }
        Ok(TemporaryCredentials {
            access_key_id: "key-sentinel".into(),
            secret_access_key: "secret-sentinel".into(),
            session_token: "token-sentinel".into(),
            expires_at_ms: 1_900_000,
        })
    }
}

#[tokio::test]
async fn issuance_uses_fixed_role_scoped_policy_and_redacts_credentials() -> Result<()> {
    let sts = Arc::new(RecordingSts {
        requests: Mutex::new(Vec::new()),
        failures: AtomicUsize::new(0),
    });
    let issuer = AwsCredentialIssuer::new(
        "arn:aws:iam::123456789012:role/actor".into(),
        sts.clone(),
        Arc::new(FixedClock),
    )?;
    let policy = ownership_policy("state", &actor("one"))?;
    let credentials = issuer.issue(&policy).await?;
    let debug = format!("{credentials:?}");
    assert!(!debug.contains("sentinel"));
    assert!(debug.contains("1900000"));
    let requests = sts.requests.lock().unwrap();
    assert_eq!(requests[0].role_arn, "arn:aws:iam::123456789012:role/actor");
    assert_eq!(requests[0].policy, policy.as_str());
    assert_eq!(requests[0].duration_seconds, 900);
    drop(requests);
    let failure = issuer.issue(&policy).await.unwrap_err();
    assert!(!format!("{failure:#}").contains("sentinel"));
    Ok(())
}

#[tokio::test]
async fn s3_bootstrap_with_code_issues_exact_artifact_access() -> Result<()> {
    struct Sts;
    #[async_trait]
    impl AssumeRole for Sts {
        async fn assume_role(&self, _: AssumeRoleRequest) -> Result<TemporaryCredentials> {
            Ok(TemporaryCredentials {
                access_key_id: "key".into(),
                secret_access_key: "secret".into(),
                session_token: "token".into(),
                expires_at_ms: 2_000_000,
            })
        }
    }
    struct NoExpress;
    #[async_trait]
    impl ExpressLogs for NoExpress {
        async fn handle(&self, _: ExpressLogRequest) -> Result<ExpressLogReply> {
            anyhow::bail!("unused")
        }
    }
    let access = crate::bucket::access::RuntimeAccess::new(
        Box::new(S3Access::new(
            "state".into(),
            "us-west-2".into(),
            "code".into(),
            Arc::new(AwsCredentialIssuer::new(
                "arn:aws:iam::123456789012:role/actor".into(),
                Arc::new(Sts),
                Arc::new(FixedClock),
            )?),
            Arc::new(NoExpress),
        )),
        SnapshotStoreConfig::Local,
    );
    let key = "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/actors.mjs";
    let code = crate::bucket::s3::S3Bundle {
        bucket: "code".into(),
        region: "us-west-2".into(),
        files: vec![crate::bucket::s3::bundle::S3BundleFile {
            path: "actors.mjs".into(),
            object: key.into(),
            etag: "etag".into(),
            sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
        }],
    }
    .encode()?;

    let config: crate::bucket::access::HostStorageConfig = serde_json::from_str(
        &access
            .bootstrap("us-west-2", &actor("one"), Some(&code))
            .await?,
    )?;

    let Some(StorageAccess::S3(aws)) = config.access else {
        anyhow::bail!("AWS credentials");
    };
    assert_eq!(aws.artifacts.len(), 1);
    assert_eq!(aws.artifacts[0].keys, [key]);
    Ok(())
}

fn explicitly_allows(
    policy: &SessionPolicy,
    action: &str,
    resource: &str,
    prefix: Option<&str>,
) -> bool {
    let value: serde_json::Value = serde_json::from_str(policy.as_str()).unwrap();
    value["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Effect"] == "Allow")
        .any(|statement| {
            matches_values(&statement["Action"], action)
                && matches_values(&statement["Resource"], resource)
                && statement.get("Condition").is_none_or(|condition| {
                    prefix.is_some_and(|prefix| {
                        matches_pattern(
                            condition["StringLike"]["s3:prefix"].as_str().unwrap(),
                            prefix,
                        )
                    })
                })
        })
}

fn explicitly_denies(
    policy: &SessionPolicy,
    action: &str,
    resource: &str,
    prefix: Option<&str>,
) -> bool {
    let value: serde_json::Value = serde_json::from_str(policy.as_str()).unwrap();
    value["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Effect"] == "Deny")
        .any(|statement| {
            let action_matches = statement.get("Action").map_or_else(
                || !matches_values(&statement["NotAction"], action),
                |actions| matches_values(actions, action),
            );
            let resource_matches = statement.get("Resource").map_or_else(
                || !matches_values(&statement["NotResource"], resource),
                |resources| matches_values(resources, resource),
            );
            let condition_matches = statement.get("Condition").is_none_or(|condition| {
                let pattern = condition["StringNotLike"]["s3:prefix"].as_str().unwrap();
                prefix.is_none_or(|prefix| !matches_pattern(pattern, prefix))
            });
            action_matches && resource_matches && condition_matches
        })
}

fn matches_values(value: &serde_json::Value, target: &str) -> bool {
    match value {
        serde_json::Value::String(pattern) => matches_pattern(pattern, target),
        serde_json::Value::Array(values) => {
            values.iter().any(|value| matches_values(value, target))
        }
        _ => false,
    }
}

fn matches_pattern(pattern: &str, target: &str) -> bool {
    pattern
        .strip_suffix('*')
        .map_or(pattern == target, |prefix| target.starts_with(prefix))
}
