use super::*;

#[tokio::test]
async fn aws_clients_follow_refreshed_scope_specific_credentials() -> Result<()> {
    use crate::bucket::s3::access::{
        AwsArtifactCredentials, AwsStorageCredentials, TemporaryCredentials,
    };

    let access = Arc::new(std::sync::RwLock::new(Some(StorageAccess::S3(aws_access(
        "first",
    )))));
    let ownership = AwsHostCredentials::new(access.clone(), AwsScope::Ownership);
    let state = AwsHostCredentials::new(access.clone(), AwsScope::State);
    let artifact =
        AwsHostCredentials::new(access.clone(), AwsScope::Artifact("code/actors.mjs".into()));
    assert_eq!(ownership.load().await?.access_key_id(), "first-owner");
    assert_eq!(state.load().await?.access_key_id(), "first-state");
    assert_eq!(artifact.load().await?.access_key_id(), "first-artifact");

    *access.write().unwrap() = Some(StorageAccess::S3(aws_access("refreshed")));
    assert_eq!(ownership.load().await?.access_key_id(), "refreshed-owner");
    assert_eq!(state.load().await?.access_key_id(), "refreshed-state");
    assert_eq!(artifact.load().await?.access_key_id(), "refreshed-artifact");
    assert!(
        AwsHostCredentials::new(access, AwsScope::Artifact("other".into()))
            .load()
            .await
            .is_err()
    );

    fn credential(id: &str) -> TemporaryCredentials {
        TemporaryCredentials {
            access_key_id: id.into(),
            secret_access_key: "secret".into(),
            session_token: "token".into(),
            expires_at_ms: 2_000_000,
        }
    }
    fn aws_access(prefix: &str) -> AwsStorageCredentials {
        AwsStorageCredentials {
            ownership: credential(&format!("{prefix}-owner")),
            state: credential(&format!("{prefix}-state")),
            artifacts: vec![AwsArtifactCredentials {
                keys: vec!["code/actors.mjs".into()],
                credentials: credential(&format!("{prefix}-artifact")),
            }],
        }
    }
    Ok(())
}
