use super::*;

fn config() -> ModalConfig {
    ModalConfig {
        app_name: "durable-actors-test".into(),
        regions: BTreeMap::from([("north-america-west".into(), "us-west".into())]),
        timeout: Duration::from_secs(600),
    }
}

fn request() -> CreateSpareRequest {
    CreateSpareRequest {
        control_plane_url: Some("https://control.example.com".into()),
        kind: SpareKind::Actor,
        name: "spare-one".into(),
        image_ref: "registry/runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 250,
            memory_mib: 256,
        },
    }
}

#[test]
fn modal_spare_preserves_resource_requests_and_limits() -> Result<()> {
    let request = request();
    let params = spare_params(&request, &config(), "assignment-token")?;
    assert_eq!((params.cpu, params.cpu_limit), (Some(0.25), Some(0.25)));
    assert_eq!(
        (params.memory_mib, params.memory_limit_mib),
        (Some(256), Some(256))
    );
    assert_eq!(params.command, ["/usr/local/bin/durable-actors"]);
    assert_eq!(params.env["DURABLE_ACTORS_SPARE_TOKEN"], "assignment-token");
    assert_eq!(
        params.env["DURABLE_ACTORS_CONTROL_PLANE_URL"],
        "https://control.example.com"
    );
    assert_eq!(params.h2_ports, [7101, 7102]);
    assert!(params.unencrypted_ports.is_empty());
    assert_eq!(params.cloud.as_deref(), Some("gcp"));
    assert_eq!(params.regions, ["us-west"]);
    assert_eq!(params.timeout, Some(Duration::from_secs(600)));
    Ok(())
}

#[test]
fn modal_spare_requires_reachable_control_plane_and_configured_region() {
    let mut request = request();
    request.control_plane_url = None;
    assert!(spare_params(&request, &config(), "token").is_err());
    request.control_plane_url = Some("https://control.example.com".into());
    request.canonical_region = "north-america-east".into();
    assert!(spare_params(&request, &config(), "token").is_err());
}
