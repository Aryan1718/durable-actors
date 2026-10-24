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
    let state = state_policy("state", &actor("one"))?;
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
async fn issuance_rejects_expired_short_lived_or_incomplete_credentials() -> Result<()> {
    struct StaticSts(TemporaryCredentials);
    #[async_trait]
    impl AssumeRole for StaticSts {
        async fn assume_role(&self, _: AssumeRoleRequest) -> Result<TemporaryCredentials> {
            Ok(self.0.clone())
        }
    }
    for (expires, key, secret, token) in [
        (999_999, "key", "secret", "token"),
        (1_120_000, "key", "secret", "token"),
        (1_900_000, "", "secret", "token"),
        (1_900_000, "key", "", "token"),
        (1_900_000, "key", "secret", ""),
    ] {
        let credential = TemporaryCredentials {
            access_key_id: key.into(),
            secret_access_key: secret.into(),
            session_token: token.into(),
            expires_at_ms: expires,
        };
        let issuer = AwsCredentialIssuer::new(
            "arn:aws:iam::123456789012:role/actor".into(),
            Arc::new(StaticSts(credential)),
            Arc::new(FixedClock),
        )?;
        assert!(
            issuer
                .issue(&ownership_policy("state", &actor("one"))?)
                .await
                .is_err()
        );
    }
    for role in [
        "arn:aws:iam::123:role/actor",
        "arn:aws:iam::123456789012:role/*",
        "arn:aws:iam::123456789012:user/actor",
    ] {
        assert!(
            AwsCredentialIssuer::new(
                role.into(),
                Arc::new(StaticSts(TemporaryCredentials {
                    access_key_id: "key".into(),
                    secret_access_key: "secret".into(),
                    session_token: "token".into(),
                    expires_at_ms: 1_900_000,
                })),
                Arc::new(FixedClock)
            )
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn stalled_issuer_times_out() -> Result<()> {
    struct Stalled;
    #[async_trait]
    impl AssumeRole for Stalled {
        async fn assume_role(&self, _: AssumeRoleRequest) -> Result<TemporaryCredentials> {
            std::future::pending().await
        }
    }
    let issuer = AwsCredentialIssuer::new(
        "arn:aws:iam::123456789012:role/actor".into(),
        Arc::new(Stalled),
        Arc::new(FixedClock),
    )?;
    assert!(
        issuer
            .issue(&ownership_policy("state", &actor("one"))?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn sdk_uses_injected_sts_endpoint_and_redacts_service_errors() -> Result<()> {
    use aws_sdk_sts::config::{BehaviorVersion, Credentials, Region};

    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let app = axum::Router::new().route("/", axum::routing::post(move |body: bytes::Bytes| {
        let captured = captured.clone();
        async move {
            let mut requests = captured.lock().unwrap();
            requests.push(String::from_utf8(body.to_vec()).unwrap());
            if requests.len() > 1 {
                return (axum::http::StatusCode::FORBIDDEN, "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>secret-sentinel</Message></Error><RequestId>request</RequestId></ErrorResponse>");
            }
            (axum::http::StatusCode::OK, "<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>key-sentinel</AccessKeyId><SecretAccessKey>secret-sentinel</SecretAccessKey><SessionToken>token-sentinel</SessionToken><Expiration>2030-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>request</RequestId></ResponseMetadata></AssumeRoleResponse>")
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let sdk = aws_sdk_sts::Client::from_conf(
        aws_sdk_sts::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-west-2"))
            .credentials_provider(Credentials::new(
                "injected-key",
                "injected-secret",
                None,
                None,
                "test",
            ))
            .endpoint_url(endpoint)
            .build(),
    );
    let issuer = AwsCredentialIssuer::new(
        "arn:aws:iam::123456789012:role/actor".into(),
        Arc::new(SdkAssumeRole::new(sdk)),
        Arc::new(FixedClock),
    )?;
    let policy = ownership_policy("state", &actor("one"))?;
    let success = issuer.issue(&policy).await;
    let failure = issuer.issue(&policy).await;
    server.abort();
    let credentials = success?;
    assert_eq!(credentials.access_key_id, "key-sentinel");
    assert_eq!(credentials.expires_at_ms, 1_893_456_000_000);
    assert!(!format!("{:#}", failure.unwrap_err()).contains("sentinel"));
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains("Action=AssumeRole"));
    assert!(requests[0].contains("DurationSeconds=900"));
    assert!(requests[0].contains("Policy="));
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
