use super::*;
use crate::bucket::{
    access::{BucketLocation, HostStorageConfig, RuntimeAccess, StorageAccess},
    s3::{
        ExpressLogReply, ExpressLogRequest, ExpressLogs,
        access::{
            AssumeRole, AssumeRoleRequest, AwsCredentialIssuer, S3Access, TemporaryCredentials,
        },
    },
};

#[tokio::test]
async fn s3_image_hosts_bootstrap_and_refresh_without_gcs_artifact_access() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let access = Arc::new(RuntimeAccess::new(
        Box::new(S3Access::new(
            "state".into(),
            "us-west-2".into(),
            "unused".into(),
            test_aws_issuer()?,
            Arc::new(NoExpress),
        )),
        crate::bucket::SnapshotStoreConfig::Local,
    ));
    let provisioner = SandboxHostProvisioner::new(
        Arc::new(ImageHost),
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invocation".into(),
            host_idle_timeout_ms: 10_000,
        },
        issuer.clone(),
        None,
    )
    .with_runtime_access(access.clone());
    let spec: HostLaunchSpec = serde_json::from_value(serde_json::json!({
        "organizationId":"organization", "projectId":"default", "imageRef":"image", "workingDirectory":"/app", "secretRefs":[]
    }))?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let (lease, epoch, _) = provisioner
        .ensure_actor_host(&spec, "north-america-west", &actor, true, None, None)
        .await?;
    assert_eq!(epoch, 1);
    let placements = Arc::new(LocalObjectPlacementStore::default());
    placements.set_owner(&actor.storage_key(), lease.clone(), "north-america-west")?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry.register_test_deployment(&spec).await?;
    let principal = ActorPrincipal {
        organization_id: Some("organization".into()),
        actor,
        host_id: lease.id,
        session_id: lease.session_id,
        region: "north-america-west".into(),
        host_config_key: Some(spec.host_config_key()),
        invocation: None,
    };
    let service = ControlPlaneService::new(
        placements,
        auth.clone(),
        registry,
        issuer,
        Arc::new(provisioner),
    )
    .with_runtime_access(access);
    for _ in 0..2 {
        let reply = service
            .execute_command(&principal, ControlPlaneCommand::RefreshStorageAccess)
            .await?;
        let ControlPlaneCommandReply::StorageAccess {
            access,
            replacement_token,
        } = reply
        else {
            anyhow::bail!("expected storage access reply")
        };
        assert!(matches!(access, Some(StorageAccess::S3(_))));
        let refreshed = auth.authenticate_authorization(&format!("Bearer {replacement_token}"))?;
        assert_eq!(refreshed.actor, principal.actor);
        assert_eq!(refreshed.host_id, principal.host_id);
        assert_eq!(refreshed.session_id, principal.session_id);
        assert_eq!(refreshed.host_config_key, principal.host_config_key);
    }
    Ok(())
}

struct ImageHost;

#[async_trait]
impl SandboxProvider for ImageHost {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }

    async fn ensure_host(
        &self,
        request: &EnsureHostRequest,
    ) -> Result<crate::sandbox::ActorHostHandle> {
        assert!(request.code_snapshot.is_none());
        assert!(request.spare.is_none());
        let config: HostStorageConfig = serde_json::from_str(
            request
                .runtime_config
                .as_deref()
                .context("storage config")?,
        )?;
        assert!(matches!(config.bucket, BucketLocation::S3 { bucket, .. } if bucket == "state"));
        assert!(matches!(config.access, Some(StorageAccess::S3(_))));
        let route = "https://image-host.example.com".to_owned();
        Ok(crate::sandbox::ActorHostHandle {
            lease: Some(HostLease {
                id: request.host_id.clone(),
                session_id: request.session_id.clone(),
                route: route.clone(),
                expires_at_ms: crate::clock::Clock::now_ms(&crate::clock::SystemClock)? + 10_000,
            }),
            host_id: request.host_id.clone(),
            owner_epoch: 1,
            route,
            canonical_region: request.canonical_region.clone(),
            provisioning: None,
            initial_reply: None,
        })
    }

    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected host termination")
    }
}

fn test_aws_issuer() -> Result<Arc<AwsCredentialIssuer>> {
    struct Sts;
    #[async_trait]
    impl AssumeRole for Sts {
        async fn assume_role(&self, _: AssumeRoleRequest) -> Result<TemporaryCredentials> {
            Ok(TemporaryCredentials {
                access_key_id: "key".into(),
                secret_access_key: "secret".into(),
                session_token: "token".into(),
                expires_at_ms: crate::clock::Clock::now_ms(&crate::clock::SystemClock)? + 900_000,
            })
        }
    }
    Ok(Arc::new(AwsCredentialIssuer::new(
        "arn:aws:iam::123456789012:role/actor".into(),
        Arc::new(Sts),
        Arc::new(crate::clock::SystemClock),
    )?))
}

struct NoExpress;

#[async_trait]
impl ExpressLogs for NoExpress {
    async fn handle(&self, _: ExpressLogRequest) -> Result<ExpressLogReply> {
        anyhow::bail!("unused")
    }
}
