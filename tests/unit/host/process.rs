use super::*;

#[cfg(unix)]
#[tokio::test]
async fn executor_environment_excludes_cloud_credentials_and_host_tokens() -> Result<()> {
    let mut command = Command::new("env");
    let inherited = [
        ("AWS_ACCESS_KEY_ID", "platform-key"),
        ("AWS_SECRET_ACCESS_KEY", "platform-secret"),
        ("AWS_SESSION_TOKEN", "platform-session"),
        ("AWS_PROFILE", "platform-profile"),
        ("AWS_CONFIG_FILE", "/platform/config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "/platform/credentials"),
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/platform/identity"),
        (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://platform/identity",
        ),
        (
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "/platform/identity",
        ),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "platform-auth"),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/platform/auth"),
        ("AWS_ROLE_ARN", "platform-role"),
        ("aws_secret_access_key", "platform-lowercase-secret"),
        ("AWS_FUTURE_CREDENTIAL_PROVIDER", "platform-future"),
        ("BOTO_CONFIG", "/platform/boto"),
        ("BOTO_PATH", "/platform/boto-path"),
        ("GOOGLE_APPLICATION_CREDENTIALS", "/platform/google"),
        ("DURABLE_ACTORS_HOST_TOKEN", "host-token"),
        ("DURABLE_ACTORS_SPARE_TOKEN", "spare-token"),
        ("DURABLE_ACTORS_RUNTIME_CONFIG", "storage-secret"),
        ("PATH", "/usr/bin:/bin"),
        ("CUSTOMER_SETTING", "customer-value"),
    ]
    .into_iter()
    .map(|(name, value)| (name.into(), value.into()));
    configure_executor_environment(&mut command, inherited);
    let output = command.output().await?;
    assert!(output.status.success());
    let environment = String::from_utf8(output.stdout)?;
    assert!(environment.contains("CUSTOMER_SETTING=customer-value\n"));
    assert!(environment.contains("PATH=/usr/bin:/bin\n"));
    assert!(environment.contains("AWS_EC2_METADATA_DISABLED=true\n"));
    for forbidden in [
        "AWS_ACCESS_KEY_ID=",
        "AWS_SECRET_ACCESS_KEY=",
        "AWS_SESSION_TOKEN=",
        "AWS_PROFILE=",
        "AWS_CONFIG_FILE=",
        "AWS_SHARED_CREDENTIALS_FILE=",
        "AWS_WEB_IDENTITY_TOKEN_FILE=",
        "AWS_CONTAINER_CREDENTIALS_",
        "AWS_CONTAINER_AUTHORIZATION_",
        "AWS_ROLE_ARN=",
        "aws_secret_access_key=",
        "AWS_FUTURE_CREDENTIAL_PROVIDER=",
        "BOTO_",
        "GOOGLE_APPLICATION_CREDENTIALS=",
        "DURABLE_ACTORS_HOST_TOKEN=",
        "DURABLE_ACTORS_SPARE_TOKEN=",
        "DURABLE_ACTORS_RUNTIME_CONFIG=",
    ] {
        assert!(!environment.lines().any(|line| line.starts_with(forbidden)));
    }
    Ok(())
}

fn busy() -> ActorStatus {
    ActorStatus {
        idle_since: None,
        ..Default::default()
    }
}

#[tokio::test]
async fn admitted_requests_prevent_idle_host_shutdown() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (actor, mut status) = tokio::sync::watch::channel(busy());
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        &mut status,
        Duration::from_millis(100),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stopped.as_mut())
            .await
            .is_err()
    );
    actor.send_modify(|status| status.idle_since = Some(tokio::time::Instant::now()));
    tokio::time::timeout(Duration::from_secs(1), stopped).await??;
    Ok(())
}

#[tokio::test]
async fn failed_activation_stops_a_busy_host() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (actor, mut status) = tokio::sync::watch::channel(busy());
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        &mut status,
        Duration::from_millis(10),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stopped.as_mut())
            .await
            .is_err()
    );
    actor.send_modify(|status| status.phase = ActorPhase::Stopped);
    let error = tokio::time::timeout(Duration::from_secs(1), stopped.as_mut())
        .await?
        .unwrap_err();
    assert!(error.to_string().contains("activation stopped"));
    drop(stopped);
    javascript.kill().await?;
    Ok(())
}

#[test]
fn executor_outlasts_a_rapid_fallback() {
    let command = executor_command(true, "/tmp/executor.sock", ActorRuntime::Typescript);
    let startup = command
        .as_std()
        .get_envs()
        .find(|(name, _)| *name == "DURABLE_ACTORS_HOST_STARTUP_MS")
        .and_then(|(_, value)| value);
    assert_eq!(startup, Some(std::ffi::OsStr::new("30000")));
}
