use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_sdk_sts::config::{retry::RetryConfig, timeout::TimeoutConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ExpressLogReply, ExpressLogRequest, ExpressLogs, S3Bucket, S3ExpressLogs};
use crate::{
    actor::ActorKey,
    bucket::{
        SnapshotStoreConfig,
        access::{AccessIssuer, BucketLocation, StorageAccess},
    },
    bundle::ArtifactBundle,
    clock::Clock,
};

const POLICY_LIMIT: usize = 2_048;
const ISSUANCE_TIMEOUT: Duration = Duration::from_secs(10);
const SESSION_SECONDS: i32 = 900;

/// The control plane's AWS storage: its own bucket client and the issuer of actor credentials.
pub(crate) struct AwsStorage {
    pub bucket: Arc<S3Bucket>,
    pub client: aws_sdk_s3::Client,
    name: String,
    region: String,
    issuer: Arc<AwsCredentialIssuer>,
}

impl AwsStorage {
    pub(crate) async fn connect(bucket: &str, region: &str, role_arn: &str) -> Result<Self> {
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_owned()))
            .load()
            .await;
        let client = aws_sdk_s3::Client::new(&sdk);
        Ok(Self {
            bucket: Arc::new(S3Bucket::new(bucket, client.clone())?),
            client,
            name: bucket.into(),
            region: region.into(),
            issuer: Arc::new(AwsCredentialIssuer::new(
                role_arn.to_owned(),
                Arc::new(SdkAssumeRole::new(aws_sdk_sts::Client::new(&sdk))),
                Arc::new(crate::clock::SystemClock),
            )?),
        })
    }

    pub(crate) fn access(&self, artifact_bucket: &str) -> S3Access {
        S3Access::new(
            self.name.clone(),
            self.region.clone(),
            artifact_bucket.into(),
            self.issuer.clone(),
            Arc::new(S3ExpressLogs::new(self.client.clone())),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AwsStorageCredentials {
    pub ownership: TemporaryCredentials,
    pub state: TemporaryCredentials,
    pub artifacts: Vec<AwsArtifactCredentials>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AwsArtifactCredentials {
    pub keys: Vec<String>,
    pub credentials: TemporaryCredentials,
}

impl AwsStorageCredentials {
    pub(crate) fn expires_at_ms(&self) -> u64 {
        self.artifacts.iter().fold(
            self.ownership.expires_at_ms.min(self.state.expires_at_ms),
            |expires, artifact| expires.min(artifact.credentials.expires_at_ms),
        )
    }
}

pub(crate) struct S3Access {
    bucket: String,
    region: String,
    artifact_bucket: String,
    issuer: Arc<AwsCredentialIssuer>,
    express: Arc<dyn ExpressLogs>,
}

impl S3Access {
    pub(crate) fn new(
        bucket: String,
        region: String,
        artifact_bucket: String,
        issuer: Arc<AwsCredentialIssuer>,
        express: Arc<dyn ExpressLogs>,
    ) -> Self {
        Self {
            bucket,
            region,
            artifact_bucket,
            issuer,
            express,
        }
    }
}

#[async_trait]
impl AccessIssuer for S3Access {
    fn location(&self) -> BucketLocation {
        BucketLocation::S3 {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            artifact_bucket: self.artifact_bucket.clone(),
        }
    }

    fn validate_code(&self, code: &ArtifactBundle) -> Result<()> {
        let ArtifactBundle::S3(code) = code else {
            anyhow::bail!("code artifact provider does not match storage provider");
        };
        ensure!(
            code.bucket == self.artifact_bucket && code.region == self.region,
            "code artifact belongs to another storage location"
        );
        Ok(())
    }

    async fn issue(
        &self,
        actor: &ActorKey,
        persistence: &SnapshotStoreConfig,
        code: Option<ArtifactBundle>,
    ) -> Result<Option<StorageAccess>> {
        let ownership = ownership_policy(&self.bucket, actor)?;
        let snapshots = crate::storage_paths::snapshots(actor)?;
        let state = match persistence.rapid_settings() {
            Some((_, archive)) => state_policy(
                archive,
                &crate::bucket::append_log::object_name(&snapshots)?,
            )?,
            None => state_policy(&self.bucket, &snapshots)?,
        };
        let artifact_batches = match code {
            Some(ArtifactBundle::S3(bundle)) => {
                ensure!(
                    bundle.bucket == self.artifact_bucket,
                    "code artifact belongs to another bucket"
                );
                artifact_policies(
                    &self.artifact_bucket,
                    &bundle
                        .files
                        .into_iter()
                        .map(|file| file.object)
                        .collect::<Vec<_>>(),
                )?
            }
            Some(ArtifactBundle::Gcs(_)) => anyhow::bail!("S3 storage requires S3 code artifacts"),
            None => Vec::new(),
        };
        let (ownership, state) =
            tokio::try_join!(self.issuer.issue(&ownership), self.issuer.issue(&state))?;
        let mut artifacts = Vec::with_capacity(artifact_batches.len());
        for batch in artifact_batches {
            artifacts.push(AwsArtifactCredentials {
                keys: batch.keys,
                credentials: self.issuer.issue(&batch.policy).await?,
            });
        }
        Ok(Some(StorageAccess::S3(AwsStorageCredentials {
            ownership,
            state,
            artifacts,
        })))
    }

    /// Signs or fences Express log chunks for a host, only within its own actor's log segments.
    async fn express_log(
        &self,
        actor: &ActorKey,
        persistence: &SnapshotStoreConfig,
        request: ExpressLogRequest,
    ) -> Result<ExpressLogReply> {
        super::express::authorize(&request, actor, persistence)?;
        self.express.handle(request).await
    }
}

pub(crate) struct AwsCredentialIssuer {
    role_arn: String,
    sts: Arc<dyn AssumeRole>,
    clock: Arc<dyn Clock>,
}

impl AwsCredentialIssuer {
    pub(crate) fn new(
        role_arn: String,
        sts: Arc<dyn AssumeRole>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        validate_role(&role_arn)?;
        Ok(Self {
            role_arn,
            sts,
            clock,
        })
    }

    pub(crate) async fn issue(&self, policy: &SessionPolicy) -> Result<TemporaryCredentials> {
        let request = AssumeRoleRequest {
            role_arn: self.role_arn.clone(),
            session_name: format!("actor-{}", uuid::Uuid::new_v4().simple()),
            policy: policy.0.clone(),
            duration_seconds: SESSION_SECONDS,
        };
        let credential = tokio::time::timeout(ISSUANCE_TIMEOUT, self.sts.assume_role(request))
            .await
            .map_err(|_| anyhow::anyhow!("AWS credential issuance timed out"))?
            .map_err(|_| anyhow::anyhow!("AWS credential issuance failed"))?;
        ensure!(
            !credential.access_key_id.is_empty()
                && !credential.secret_access_key.is_empty()
                && !credential.session_token.is_empty()
                && credential.expires_at_ms > self.clock.now_ms()?.saturating_add(120_000),
            "AWS credential response is unusable"
        );
        Ok(credential)
    }
}

#[async_trait]
pub(crate) trait AssumeRole: Send + Sync {
    async fn assume_role(&self, request: AssumeRoleRequest) -> Result<TemporaryCredentials>;
}

pub(crate) struct AssumeRoleRequest {
    pub role_arn: String,
    pub session_name: String,
    pub policy: String,
    pub duration_seconds: i32,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TemporaryCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at_ms: u64,
}

impl std::fmt::Debug for TemporaryCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TemporaryCredentials")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

pub(crate) struct SdkAssumeRole(aws_sdk_sts::Client);

impl SdkAssumeRole {
    pub(crate) fn new(client: aws_sdk_sts::Client) -> Self {
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(2))
            .read_timeout(Duration::from_secs(5))
            .operation_attempt_timeout(Duration::from_secs(5))
            .operation_timeout(ISSUANCE_TIMEOUT)
            .build();
        let retries = RetryConfig::standard()
            .with_max_attempts(3)
            .with_initial_backoff(Duration::from_millis(100))
            .with_max_backoff(Duration::from_secs(1));
        Self(aws_sdk_sts::Client::from_conf(
            client
                .config()
                .to_builder()
                .timeout_config(timeouts)
                .retry_config(retries)
                .build(),
        ))
    }
}

#[async_trait]
impl AssumeRole for SdkAssumeRole {
    async fn assume_role(&self, request: AssumeRoleRequest) -> Result<TemporaryCredentials> {
        let response = self
            .0
            .assume_role()
            .role_arn(request.role_arn)
            .role_session_name(request.session_name)
            .policy(request.policy)
            .duration_seconds(request.duration_seconds)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("AWS credential issuance failed"))?;
        let credentials = response.credentials.context("AWS credentials missing")?;
        let expires_at_ms = u64::try_from(
            credentials
                .expiration
                .to_millis()
                .map_err(|_| anyhow::anyhow!("AWS credential expiration invalid"))?,
        )
        .context("AWS credential expiration invalid")?;
        Ok(TemporaryCredentials {
            access_key_id: credentials.access_key_id,
            secret_access_key: credentials.secret_access_key,
            session_token: credentials.session_token,
            expires_at_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SessionPolicy(String);

impl SessionPolicy {
    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

struct ArtifactPolicyBatch {
    pub keys: Vec<String>,
    pub policy: SessionPolicy,
}

fn ownership_policy(bucket: &str, actor: &ActorKey) -> Result<SessionPolicy> {
    let resource = object_arn(bucket, &crate::storage_paths::owner(&actor.storage_key())?)?;
    object_policy(&["s3:GetObject", "s3:PutObject"], &[resource])
}

fn state_policy(bucket: &str, prefix: &str) -> Result<SessionPolicy> {
    let object = format!("{}*", object_arn(bucket, prefix)?);
    let bucket = bucket_arn(bucket)?;
    let actions = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"];
    let mut statements = object_statements(&actions, &[object]);
    statements[1]["NotAction"] = json!([
        "s3:GetObject",
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:ListBucket"
    ]);
    statements.extend([
        json!({"Effect":"Allow", "Action":"s3:ListBucket", "Resource":bucket,
            "Condition":{"StringLike":{"s3:prefix":format!("{prefix}*")}}}),
        json!({"Effect":"Deny", "Action":"s3:ListBucket", "NotResource":bucket}),
        json!({"Effect":"Deny", "Action":"s3:ListBucket", "Resource":bucket,
            "Condition":{"StringNotLike":{"s3:prefix":format!("{prefix}*")}}}),
    ]);
    serialize_policy(statements)
}

fn artifact_policies(bucket: &str, keys: &[String]) -> Result<Vec<ArtifactPolicyBatch>> {
    bucket_arn(bucket)?;
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for key in keys.iter().collect::<BTreeSet<_>>() {
        object_arn(bucket, key)?;
        current.push(key.clone());
        if artifact_policy(bucket, &current).is_err() {
            current.pop();
            ensure!(
                !current.is_empty(),
                "artifact resource exceeds AWS session policy limit"
            );
            batches.push(ArtifactPolicyBatch {
                policy: artifact_policy(bucket, &current)?,
                keys: std::mem::take(&mut current),
            });
            current.push(key.clone());
            artifact_policy(bucket, &current)?;
        }
    }
    if !current.is_empty() {
        batches.push(ArtifactPolicyBatch {
            policy: artifact_policy(bucket, &current)?,
            keys: current,
        });
    }
    Ok(batches)
}

fn artifact_policy(bucket: &str, keys: &[String]) -> Result<SessionPolicy> {
    let resources = keys
        .iter()
        .map(|key| object_arn(bucket, key))
        .collect::<Result<Vec<_>>>()?;
    object_policy(&["s3:GetObject"], &resources)
}

fn object_policy(actions: &[&str], resources: &[String]) -> Result<SessionPolicy> {
    serialize_policy(object_statements(actions, resources))
}

fn object_statements(actions: &[&str], resources: &[String]) -> Vec<Value> {
    vec![
        json!({"Effect":"Allow", "Action":actions, "Resource":resources}),
        json!({"Effect":"Deny", "NotAction":actions, "Resource":"*"}),
        json!({"Effect":"Deny", "Action":actions, "NotResource":resources}),
    ]
}

fn serialize_policy(statements: Vec<Value>) -> Result<SessionPolicy> {
    let json = serde_json::to_string(&json!({"Version":"2012-10-17", "Statement":statements}))?;
    let mut ascii = String::new();
    for character in json.chars() {
        if character.is_ascii() {
            ascii.push(character);
        } else {
            for unit in character.encode_utf16(&mut [0; 2]) {
                use std::fmt::Write;
                write!(ascii, "\\u{unit:04x}")?;
            }
        }
    }
    ensure!(
        ascii.len() <= POLICY_LIMIT,
        "AWS session policy exceeds 2048 characters"
    );
    Ok(SessionPolicy(ascii))
}

fn bucket_arn(bucket: &str) -> Result<String> {
    ensure!(
        (3..=63).contains(&bucket.len())
            && bucket.bytes().all(|byte| byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'-'))
            && bucket.as_bytes()[0].is_ascii_alphanumeric()
            && bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric(),
        "invalid AWS storage bucket"
    );
    Ok(format!("arn:aws:s3:::{bucket}"))
}

fn object_arn(bucket: &str, key: &str) -> Result<String> {
    ensure!(
        !key.is_empty()
            && key.len() <= 1024
            && !key.chars().any(char::is_control)
            && !key.contains(['*', '?'])
            && !key.contains("${"),
        "invalid AWS storage object"
    );
    Ok(format!("{}/{key}", bucket_arn(bucket)?))
}

fn validate_role(role: &str) -> Result<()> {
    let remainder = role
        .strip_prefix("arn:aws:iam::")
        .context("invalid AWS session role")?;
    let (account, path) = remainder
        .split_once(":role/")
        .context("invalid AWS session role")?;
    ensure!(
        account.len() == 12
            && account.bytes().all(|byte| byte.is_ascii_digit())
            && !path.is_empty()
            && path.len() <= 576
            && path
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"/+=,.@_-".contains(&byte)),
        "invalid AWS session role"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/unit/bucket/s3/access.rs"]
mod tests;
