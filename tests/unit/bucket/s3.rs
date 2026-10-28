use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};

use super::*;

#[tokio::test]
async fn state_written_through_s3_restores_in_a_fresh_runtime() -> Result<()> {
    use crate::{
        actor::ActorKey, bucket::RuntimeStorage, clock::SystemClock, host::HostId,
        host_leases::HostLeaseRequest, state_log::StateSnapshot, state_transport::SnapshotWriter,
    };

    let server = TestServer::start().await?;
    let writer = RuntimeStorage::new(Arc::new(server.bucket()?), Arc::new(SystemClock))?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "s3-restore".into(),
    };
    let activation = writer
        .register_activation(
            &actor,
            &HostLeaseRequest {
                id: HostId::new("host-one"),
                session_id: "session-one".into(),
                route: "http://host-one".into(),
                duration_ms: 10_000,
            },
            "us-west-2",
            true,
            None,
        )
        .await?;
    let plan = writer
        .prepare_actor_write(&actor, &activation.placement.lease, 1, 1)
        .await?;
    let state = StateSnapshot::decode(
        br#"{"stateVersion":1,"ownerEpoch":1,"requestId":"write-one","sqlite":{"txid":1,"files":[{"level":0,"first":1,"last":1,"data":"AQ=="}]},"completion":{"result":42}}"#,
    )?
    .encode()?;
    writer.write_snapshot(&plan, state.clone()).await?;

    let reader = RuntimeStorage::new(Arc::new(server.bucket()?), Arc::new(SystemClock))?;
    let restored = reader
        .activate_actor(&actor, &activation.placement.lease, "us-west-2")
        .await?;

    assert_eq!(restored.placement.state_version, 1);
    assert_eq!(restored.state.unwrap(), state);
    Ok(())
}

#[tokio::test]
async fn conditional_writes_preserve_etags_and_reconcile_uncertain_responses() -> Result<()> {
    let server = TestServer::start().await?;
    let bucket = server.bucket()?;
    let key = "runtime/lease.json";
    assert!(
        bucket
            .compare_and_swap(key, None, Bytes::from_static(b"new"))
            .await?
    );
    assert!(
        bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::ETag("\"etag-42\"".into())),
                Bytes::from_static(b"new")
            )
            .await?
    );
    assert!(
        !bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::ETag("\"stale\"".into())),
                Bytes::from_static(b"new")
            )
            .await?
    );
    assert!(
        !bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::ETag("\"conflict\"".into())),
                Bytes::from_static(b"new")
            )
            .await?
    );
    assert!(
        bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::ETag("\"other-conflict\"".into())),
                Bytes::from_static(b"new")
            )
            .await
            .is_err()
    );
    assert_eq!(
        bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::Generation(1)),
                Bytes::from_static(b"new")
            )
            .await
            .unwrap_err()
            .to_string(),
        "S3 ETag required"
    );
    assert!(
        crate::bucket::replace(&bucket, "uncertain", None, Bytes::from_static(b"stored")).await?
    );

    let requests = server.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.if_none_match.as_deref() == Some("*"))
    );
    assert!(
        requests
            .iter()
            .any(|request| request.if_match.as_deref() == Some("\"etag-42\""))
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.method == "PUT")
            .all(|request| request.checksum.as_deref() == Some("CRC32C"))
    );
    Ok(())
}

#[tokio::test]
async fn large_writes_use_conditional_multipart_publication() -> Result<()> {
    let server = TestServer::start().await?;
    let bytes = Bytes::from(vec![7; 5 * 1024 * 1024 + 1]);

    assert!(
        server
            .bucket()?
            .compare_and_swap("runtime/large", None, bytes.clone())
            .await?
    );
    assert!(
        server
            .bucket()?
            .compare_and_swap(
                "runtime/large",
                Some(StorageRevision::ETag("\"large-etag\"".into())),
                bytes,
            )
            .await?
    );

    let requests = server.requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| { request.method == "POST" && request.uri.contains("?uploads") })
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "PUT" && request.uri.contains("partNumber="))
            .count(),
        4
    );
    assert!(requests.iter().any(|request| {
        request.method == "POST"
            && request.uri.contains("uploadId=test-upload")
            && request.if_none_match.as_deref() == Some("*")
    }));
    assert!(requests.iter().any(|request| {
        request.method == "POST"
            && request.uri.contains("uploadId=test-upload")
            && request.if_match.as_deref() == Some("\"large-etag\"")
    }));
    assert!(
        requests
            .iter()
            .filter(|request| request.uri.contains("partNumber="))
            .all(|request| request.checksum.as_deref() == Some("CRC32C"))
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_publications_are_reconciled_without_sdk_replay() -> Result<()> {
    let server = TestServer::start().await?;
    let bucket = server.bucket()?;
    assert!(
        crate::bucket::replace(&bucket, "uncertain", None, Bytes::from_static(b"stored")).await?
    );
    assert!(
        crate::bucket::replace(&bucket, "uncertain", None, Bytes::from_static(b"different"))
            .await
            .is_err()
    );
    assert!(
        crate::bucket::replace(
            &bucket,
            "runtime/large-uncertain",
            None,
            Bytes::from(vec![7; 5 * 1024 * 1024 + 1])
        )
        .await?
    );
    assert!(
        bucket
            .compare_and_swap(
                "runtime/failed-create",
                None,
                Bytes::from(vec![7; 5 * 1024 * 1024 + 1])
            )
            .await
            .is_err()
    );

    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method == "PUT" && r.uri.contains("/uncertain"))
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method == "POST"
                && r.uri.contains("large-uncertain")
                && r.uri.contains("uploadId="))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method == "POST" && r.uri.contains("failed-create"))
            .count(),
        1
    );
    Ok(())
}

#[derive(Clone, Default)]
struct ServerState {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    objects: Arc<Mutex<HashMap<String, StoredObject>>>,
}

#[derive(Clone)]
struct StoredObject {
    etag: String,
    bytes: Bytes,
}

#[derive(Debug)]
struct RecordedRequest {
    method: String,
    uri: String,
    if_match: Option<String>,
    if_none_match: Option<String>,
    checksum: Option<String>,
}

struct TestServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    async fn start() -> Result<Self> {
        let state = ServerState::default();
        let requests = state.requests.clone();
        let routes = Router::new()
            .route("/test-bucket", get(list))
            .route("/test-bucket/", get(list))
            .route(
                "/test-bucket/{*key}",
                get(read).put(write).post(multipart).delete(abort),
            )
            .layer(DefaultBodyLimit::disable())
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move { axum::serve(listener, routes).await });
        Ok(Self {
            endpoint,
            requests,
            task,
        })
    }

    fn bucket(&self) -> Result<S3Bucket> {
        let credentials =
            aws_sdk_s3::config::Credentials::new("test", "secret", None, None, "tests");
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .credentials_provider(credentials)
            .region(aws_sdk_s3::config::Region::new("us-west-2"))
            .endpoint_url(&self.endpoint)
            .force_path_style(true)
            .build();
        S3Bucket::new("test-bucket", aws_sdk_s3::Client::from_conf(config))
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read(
    State(state): State<ServerState>,
    Path(key): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    record(&state, "GET", uri.to_string(), &headers);
    if let Some(object) = state.objects.lock().unwrap().get(&key).cloned() {
        return ([("etag", object.etag)], object.bytes).into_response();
    }
    match key.as_str() {
        "uncertain" => ([("etag", "\"etag-stored\"")], "stored").into_response(),
        "runtime/large-uncertain" => {
            ([("etag", "\"large\"")], vec![7; 5 * 1024 * 1024 + 1]).into_response()
        }
        _ => s3_error(StatusCode::NOT_FOUND, "NoSuchKey"),
    }
}

async fn write(
    State(state): State<ServerState>,
    Path(key): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    record(&state, "PUT", uri.to_string(), &headers);
    if uri
        .query()
        .is_some_and(|query| query.contains("partNumber="))
    {
        let checksum = header(&headers, "x-amz-checksum-crc32c").unwrap();
        return (
            StatusCode::OK,
            [
                ("etag", "\"part-etag\"".to_owned()),
                ("x-amz-checksum-crc32c", checksum),
            ],
        )
            .into_response();
    }
    match (key.as_str(), header(&headers, "if-match").as_deref()) {
        ("uncertain", _) => s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError"),
        ("runtime/lease.json", Some("\"stale\"")) => {
            s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed")
        }
        ("runtime/lease.json", Some("\"conflict\"")) => {
            s3_error(StatusCode::CONFLICT, "ConditionalRequestConflict")
        }
        ("runtime/lease.json", Some("\"other-conflict\"")) => {
            s3_error(StatusCode::CONFLICT, "OperationAborted")
        }
        ("runtime/lease.json", _) => (StatusCode::OK, [("etag", "\"etag-43\"")]).into_response(),
        _ => store(&state, key, &headers, body),
    }
}

fn store(state: &ServerState, key: String, headers: &HeaderMap, body: Bytes) -> Response {
    let mut objects = state.objects.lock().unwrap();
    let current = objects.get(&key);
    let matches = match (
        header(headers, "if-match"),
        header(headers, "if-none-match"),
    ) {
        (Some(expected), _) => current.is_some_and(|object| object.etag == expected),
        (_, Some(expected)) if expected == "*" => current.is_none(),
        _ => true,
    };
    if !matches {
        return s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
    }
    let etag = format!("\"stored-{}\"", objects.len() + 1);
    objects.insert(
        key,
        StoredObject {
            etag: etag.clone(),
            bytes: body,
        },
    );
    (StatusCode::OK, [("etag", etag)]).into_response()
}

async fn multipart(
    State(state): State<ServerState>,
    Path(key): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    _body: Bytes,
) -> Response {
    record(&state, "POST", uri.to_string(), &headers);
    if uri
        .query()
        .is_some_and(|query| query.trim_end_matches('=') == "uploads")
    {
        if key == "runtime/failed-create" {
            return s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
        }
        return (
            StatusCode::OK,
            [("content-type", "application/xml")],
            format!("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>test-bucket</Bucket><Key>{key}</Key><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>"),
        )
            .into_response();
    }
    if key == "runtime/large-uncertain" {
        return s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
    }
    (
        StatusCode::OK,
        [("content-type", "application/xml")],
        format!("<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>{key}</Key><ETag>&quot;large-etag&quot;</ETag></CompleteMultipartUploadResult>"),
    )
        .into_response()
}

async fn abort(
    State(state): State<ServerState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    record(&state, "DELETE", uri.to_string(), &headers);
    StatusCode::NO_CONTENT.into_response()
}

async fn list(
    State(state): State<ServerState>,
    Query(query): Query<HashMap<String, String>>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    record(&state, "GET", uri.to_string(), &headers);
    let prefix = query.get("prefix").map(String::as_str).unwrap_or_default();
    let contents = state
        .objects
        .lock()
        .unwrap()
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(key, object)| format!("<Contents><Key>{key}</Key><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>", object.etag.replace('"', "&quot;"), object.bytes.len()))
        .collect::<String>();
    (
        StatusCode::OK,
        [("content-type", "application/xml")],
        format!("<ListBucketResult><Name>test-bucket</Name><Prefix>{prefix}</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"),
    )
        .into_response()
}

fn record(state: &ServerState, method: &str, uri: String, headers: &HeaderMap) {
    state.requests.lock().unwrap().push(RecordedRequest {
        method: method.into(),
        uri,
        if_match: header(headers, "if-match"),
        if_none_match: header(headers, "if-none-match"),
        checksum: header(headers, "x-amz-sdk-checksum-algorithm"),
    });
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .map(|value| value.to_str().unwrap().into())
}

fn s3_error(status: StatusCode, code: &str) -> Response {
    (
        status,
        [("content-type", "application/xml")],
        format!("<Error><Code>{code}</Code><Message>{code}</Message></Error>"),
    )
        .into_response()
}
