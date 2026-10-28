use super::*;

#[tokio::test]
async fn fencing_a_segment_stops_its_previous_writer() -> Result<()> {
    let Ok(bucket) = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_BUCKET") else {
        return Ok(());
    };
    let region = std::env::var("DURABLE_ACTORS_TEST_EXPRESS_REGION")?;
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region))
        .load()
        .await;
    let zone = ExpressZone {
        bucket,
        logs: Arc::new(S3ExpressLogs::new(aws_sdk_s3::Client::new(&sdk))),
        http: reqwest::Client::new(),
    };
    let segment = format!("test-{}.segment", uuid::Uuid::new_v4());
    let (replica, mut writer) = zone.open(&segment).await?;
    assert_eq!(
        writer.append_and_flush(Bytes::from_static(b"one")).await?,
        3
    );
    assert_eq!(
        writer.append_and_flush(Bytes::from_static(b"two")).await?,
        6
    );
    assert_eq!(
        zone.read_range(&replica, 2, 3).await?,
        Bytes::from_static(b"etw")
    );
    // Crosses the first batch of upload URLs, so the background batch is used.
    for _ in 0..UPLOADS_PER_REQUEST {
        writer.append_and_flush(Bytes::from_static(b"x")).await?;
    }
    let written = [b"onetwo".as_slice(), &[b'x'; UPLOADS_PER_REQUEST as usize]].concat();

    assert_eq!(zone.read(&replica, true).await?, written);
    assert!(
        writer
            .append_and_flush(Bytes::from_static(b"late"))
            .await
            .is_err()
    );
    assert!(zone.open(&segment).await.is_err());
    assert_eq!(zone.read(&replica, false).await?, written);
    zone.delete(&replica).await
}
