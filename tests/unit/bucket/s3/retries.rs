use super::*;
use std::time::Duration;

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

#[tokio::test]
async fn reads_and_listings_retry_transient_failures_with_a_fixed_attempt_limit() -> Result<()> {
    let server = TestServer::start().await?;
    let bucket = server.bucket()?;
    assert_eq!(bucket.get("retry-read").await?.unwrap().bytes, "recovered");
    assert!(bucket.get("failed-read").await.is_err());
    assert!(bucket.list("retry-list").await?.is_empty());
    let requests = server.requests.lock().unwrap();
    for key in ["retry-read", "failed-read", "retry-list"] {
        assert_eq!(requests.iter().filter(|r| r.uri.contains(key)).count(), 3);
    }
    Ok(())
}

#[tokio::test]
async fn upload_parts_retry_without_replaying_publication() -> Result<()> {
    let server = TestServer::start().await?;
    assert!(
        server
            .bucket()?
            .compare_and_swap(
                "runtime/retry-parts",
                None,
                Bytes::from(vec![7; 5 * 1024 * 1024 + 1])
            )
            .await?
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|r| r.method == "PUT").count(), 6);
    assert_eq!(requests.iter().filter(|r| r.method == "POST").count(), 2);
    Ok(())
}

#[tokio::test]
async fn transport_disconnects_stop_after_three_attempts() -> Result<()> {
    use tokio::io::AsyncReadExt;
    let server = TestServer::start().await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut bucket = server.bucket()?;
    bucket.client = aws_sdk_s3::Client::from_conf(
        bucket
            .client
            .config()
            .to_builder()
            .endpoint_url(format!("http://{}", listener.local_addr()?))
            .build(),
    );
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let received = count.clone();
    let disconnect = tokio::spawn(async move {
        loop {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 8192];
            connection.read(&mut buffer).await.unwrap();
            received.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(15), bucket.get("disconnect")).await;
    disconnect.abort();
    assert!(result?.is_err());
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn timed_out_publications_reconcile_stored_content_without_replay() -> Result<()> {
    for (key, method, fragment, bytes, seconds) in [
        (
            "timed-put",
            "PUT",
            "timed-put",
            Bytes::from_static(b"stored"),
            6,
        ),
        (
            "runtime/large-timed",
            "POST",
            "uploadId=",
            Bytes::from(vec![7; 5 * 1024 * 1024 + 1]),
            301,
        ),
    ] {
        let server = TestServer::start().await?;
        let bucket = server.bucket()?;
        let operation =
            tokio::spawn(async move { crate::bucket::replace(&bucket, key, None, bytes).await });
        wait_for_request(&server, method, fragment).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(seconds)).await;
        tokio::time::resume();
        assert!(tokio::time::timeout(Duration::from_secs(2), operation).await???);
        assert_eq!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.method == method && r.uri.contains(fragment))
                .count(),
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn stalled_responses_and_endless_listings_have_total_deadlines() -> Result<()> {
    for key in ["stalled-headers", "stalled-body", "endless-list"] {
        let server = TestServer::start().await?;
        let mut bucket = server.bucket()?;
        bucket.client = aws_sdk_s3::Client::from_conf(
            bucket
                .client
                .config()
                .to_builder()
                .stalled_stream_protection(
                    aws_sdk_s3::config::StalledStreamProtectionConfig::disabled(),
                )
                .build(),
        );
        let operation = tokio::spawn(async move {
            match key {
                "endless-list" => bucket.list(key).await.map(|_| ()),
                _ => bucket.get(key).await.map(|_| ()),
            }
        });
        wait_for_request(&server, "GET", "").await;
        tokio::time::pause();
        let seconds = match key {
            "stalled-body" => 301,
            "endless-list" => 31,
            _ => 11,
        };
        tokio::time::advance(Duration::from_secs(seconds)).await;
        tokio::time::resume();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), operation)
                .await??
                .is_err(),
            "{key}"
        );
    }
    Ok(())
}

async fn wait_for_request(server: &TestServer, method: &str, fragment: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.method == method && r.uri.contains(fragment))
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request reached the test server");
}
