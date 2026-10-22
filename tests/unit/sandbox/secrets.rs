use super::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use axum::{Json, Router, routing::get};
use k8s_openapi::{ByteString, apimachinery::pkg::apis::meta::v1::ObjectMeta};

#[tokio::test]
async fn empty_references_do_not_initialize_kubernetes() -> Result<()> {
    let resolver = KubernetesSecrets::new("sandboxes".into());
    assert!(resolver.resolve(&[]).await?.is_empty());
    assert!(!resolver.secrets.initialized());
    Ok(())
}

#[tokio::test]
async fn references_read_current_values_from_the_configured_namespace() -> Result<()> {
    let value = Arc::new(Mutex::new(secret("API_KEY", b"first")));
    let current = value.clone();
    let routes = Router::new().route(
        "/api/v1/namespaces/project-secrets/secrets/customer",
        get(move || {
            let current = current.clone();
            async move { Json(current.lock().unwrap().clone()) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let _server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, routes).await
    }));
    let resolver = KubernetesSecrets {
        namespace: "project-secrets".into(),
        secrets: OnceCell::new_with(Some(Api::namespaced(
            Client::try_from(config)?,
            "project-secrets",
        ))),
    };
    let names = ["customer".into()];
    assert_eq!(resolver.resolve(&names).await?["API_KEY"], "first");
    *value.lock().unwrap() = secret("API_KEY", b"rotated");
    assert_eq!(resolver.resolve(&names).await?["API_KEY"], "rotated");
    assert!(resolver.resolve(&["missing".into()]).await.is_err());
    Ok(())
}

#[test]
fn labeled_secrets_merge_customer_environment() -> Result<()> {
    let values = customer_environment(vec![secret("FIRST", b"one"), secret("SECOND", b"two")])?;
    assert_eq!(
        values,
        HashMap::from([
            ("FIRST".into(), "one".into()),
            ("SECOND".into(), "two".into())
        ])
    );
    let mut empty = secret("UNUSED", b"");
    empty.data = None;
    assert!(customer_environment(vec![empty])?.is_empty());
    Ok(())
}

#[test]
fn secrets_require_the_customer_code_label() {
    for label in [None, Some("false")] {
        let mut value = secret("API_KEY", b"private-value");
        value.metadata.labels =
            label.map(|value| BTreeMap::from([("terse.ai/customer-secret".into(), value.into())]));
        let error = customer_environment(vec![value]).unwrap_err();
        assert_eq!(error.to_string(), "secret is not marked for customer code");
    }
}

#[test]
fn duplicate_environment_keys_are_rejected() {
    let error = customer_environment(vec![
        secret("API_KEY", b"first"),
        secret("API_KEY", b"second"),
    ])
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "duplicate customer environment variable API_KEY"
    );
}

#[test]
fn secret_values_must_be_utf8() {
    let error = customer_environment(vec![secret("API_KEY", &[0xff])]).unwrap_err();
    assert_eq!(error.to_string(), "customer secret must be UTF-8");
}

fn secret(name: &str, value: &[u8]) -> Secret {
    Secret {
        metadata: ObjectMeta {
            labels: Some(BTreeMap::from([(
                "terse.ai/customer-secret".into(),
                "true".into(),
            )])),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(name.into(), ByteString(value.to_vec()))])),
        ..Default::default()
    }
}
