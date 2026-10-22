use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Context;
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
async fn reads_preserve_etags_and_validate_ranges_and_checksums() -> Result<()> {
    let server = TestServer::start().await?;
    let bucket = server.bucket()?;
    let object = bucket
        .get("runtime/lease.json")
        .await
        .context("read object")?
        .unwrap();
    assert_eq!(object.revision, StorageRevision::ETag("\"etag-42\"".into()));
    assert_eq!(object.bytes, Bytes::from_static(b"lease"));
    assert!(bucket.get("missing").await?.is_none());
    assert!(bucket.get("forbidden").await.is_err());
    assert!(bucket.get("corrupt").await.is_err());

    let range = bucket
        .range("runtime/lease.json", 1, 3)
        .await
        .context("read range")?
        .unwrap();
    assert_eq!(range.revision, object.revision);
    assert_eq!(range.bytes, Bytes::from_static(b"eas"));
    assert!(bucket.range("short", 0, 3).await.is_err());
    assert!(bucket.range("misaligned", 1, 3).await.is_err());
    assert!(bucket.range("ignored-range", 1, 3).await.is_err());
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.range.as_deref() == Some("bytes=1-3"))
    );
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
        bucket
            .compare_and_swap(
                key,
                Some(StorageRevision::ETag("\"conflict\"".into())),
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
async fn multipart_writes_require_part_checksums() -> Result<()> {
    let server = TestServer::start().await?;
    let result = server
        .bucket()?
        .compare_and_swap(
            "runtime/large-no-checksum",
            None,
            Bytes::from(vec![7; 5 * 1024 * 1024 + 1]),
        )
        .await;

    assert_eq!(
        result.unwrap_err().to_string(),
        "S3 multipart part missing CRC32C checksum"
    );
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.method == "DELETE")
    );
    Ok(())
}

#[tokio::test]
async fn listings_follow_continuation_tokens() -> Result<()> {
    let server = TestServer::start().await?;
    assert_eq!(
        server
            .bucket()?
            .list("runtime/")
            .await
            .context("list objects")?,
        ["runtime/one.json", "runtime/two.json"]
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
    range: Option<String>,
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
        "runtime/lease.json" if headers.get("range").is_some() => (
            StatusCode::PARTIAL_CONTENT,
            [("etag", "\"etag-42\""), ("content-range", "bytes 1-3/5")],
            "eas",
        )
            .into_response(),
        "runtime/lease.json" => ([("etag", "\"etag-42\"")], "lease").into_response(),
        "short" => (
            StatusCode::PARTIAL_CONTENT,
            [("etag", "\"etag-short\""), ("content-range", "bytes 0-1/2")],
            "sh",
        )
            .into_response(),
        "misaligned" => (
            StatusCode::PARTIAL_CONTENT,
            [
                ("etag", "\"etag-misaligned\""),
                ("content-range", "bytes 2-4/5"),
            ],
            "eas",
        )
            .into_response(),
        "ignored-range" => ([("etag", "\"etag-ignored\"")], "eas").into_response(),
        "corrupt" => (
            [
                ("etag", "\"etag-corrupt\""),
                ("x-amz-checksum-crc32c", "AAAAAA=="),
            ],
            "corrupt",
        )
            .into_response(),
        "uncertain" => ([("etag", "\"etag-stored\"")], "stored").into_response(),
        "missing" => s3_error(StatusCode::NOT_FOUND, "NoSuchKey"),
        _ => s3_error(StatusCode::FORBIDDEN, "AccessDenied"),
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
        if key == "runtime/large-no-checksum" {
            return (StatusCode::OK, [("etag", "\"part-etag\"")]).into_response();
        }
        return (
            StatusCode::OK,
            [
                ("etag", "\"part-etag\"".to_owned()),
                ("x-amz-checksum-crc32c", checksum),
            ],
        )
            .into_response();
    }
    if key == "uncertain" {
        return s3_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
    }
    if key != "runtime/lease.json" {
        let mut objects = state.objects.lock().unwrap();
        let current = objects.get(&key);
        let matches = match (
            header(&headers, "if-match"),
            header(&headers, "if-none-match"),
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
        return (StatusCode::OK, [("etag", etag)]).into_response();
    }
    match headers
        .get("if-match")
        .and_then(|value| value.to_str().ok())
    {
        Some("\"stale\"") => s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed"),
        Some("\"conflict\"") => s3_error(StatusCode::CONFLICT, "ConditionalRequestConflict"),
        _ => (StatusCode::OK, [("etag", "\"etag-43\"")]).into_response(),
    }
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
        return (
            StatusCode::OK,
            [("content-type", "application/xml")],
            format!("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>test-bucket</Bucket><Key>{key}</Key><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>"),
        )
            .into_response();
    }
    if uri.query() == Some("uploadId=test-upload") {
        return (
            StatusCode::OK,
            [("content-type", "application/xml")],
            format!("<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>{key}</Key><ETag>&quot;large-etag&quot;</ETag></CompleteMultipartUploadResult>"),
        )
            .into_response();
    }
    s3_error(StatusCode::BAD_REQUEST, "InvalidArgument")
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
    if query.get("list-type").map(String::as_str) != Some("2") {
        return s3_error(StatusCode::BAD_REQUEST, "InvalidArgument");
    }
    let prefix = query.get("prefix").map(String::as_str).unwrap_or_default();
    if prefix != "runtime/" {
        let contents = state
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| format!("<Contents><Key>{key}</Key><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>", object.etag.replace('"', "&quot;"), object.bytes.len()))
            .collect::<String>();
        return (
            StatusCode::OK,
            [("content-type", "application/xml")],
            format!("<ListBucketResult><Name>test-bucket</Name><Prefix>{prefix}</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"),
        )
            .into_response();
    }
    let (key, truncated, token) = match query.get("continuation-token").map(String::as_str) {
        None => (
            "runtime/one.json",
            true,
            "<NextContinuationToken>next</NextContinuationToken>",
        ),
        Some("next") => ("runtime/two.json", false, ""),
        _ => return s3_error(StatusCode::BAD_REQUEST, "InvalidArgument"),
    };
    (
        StatusCode::OK,
        [("content-type", "application/xml")],
        format!("<ListBucketResult><Name>test-bucket</Name><Prefix>runtime/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>{truncated}</IsTruncated><Contents><Key>{key}</Key><ETag>&quot;etag&quot;</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>{token}</ListBucketResult>"),
    ).into_response()
}

fn record(state: &ServerState, method: &str, uri: String, headers: &HeaderMap) {
    state.requests.lock().unwrap().push(RecordedRequest {
        method: method.into(),
        uri,
        range: header(headers, "range"),
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
