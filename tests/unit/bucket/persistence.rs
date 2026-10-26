use super::*;
use serde_json::json;

struct StreamingBucket;

#[async_trait]
impl Bucket for StreamingBucket {
    async fn get(&self, _: &str) -> Result<Option<super::super::BucketObject>> {
        anyhow::bail!("snapshot used buffered read")
    }

    async fn download(&self, _: &str) -> Result<Option<super::super::BucketObject>> {
        Ok(Some(super::super::BucketObject {
            revision: super::super::StorageRevision::Generation(1),
            bytes: Bytes::from_static(b"snapshot"),
        }))
    }

    async fn range(&self, _: &str, _: u64, _: u64) -> Result<Option<super::super::BucketObject>> {
        unreachable!()
    }

    async fn compare_and_swap(
        &self,
        _: &str,
        _: Option<super::super::StorageRevision>,
        _: Bytes,
    ) -> Result<bool> {
        unreachable!()
    }

    async fn list(&self, _: &str) -> Result<Vec<String>> {
        unreachable!()
    }
}

#[tokio::test]
async fn standard_snapshots_use_streaming_bucket_reads() -> Result<()> {
    let snapshots = BucketSnapshots(Arc::new(StreamingBucket));
    assert_eq!(
        snapshots.get("snapshot").await?,
        Some(Bytes::from_static(b"snapshot"))
    );
    Ok(())
}

#[test]
fn rapid_accepts_one_or_two_distinct_zones_and_separate_archive() -> Result<()> {
    let good = json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
        {"bucket":"rapid-test-a", "zone":"us-west4-a"},
        {"bucket":"rapid-test-b", "zone":"us-west4-b"}
    ]});
    serde_json::from_value::<SnapshotStoreConfig>(good.clone())?.validate()?;
    let mut one = good.clone();
    one["buckets"].as_array_mut().unwrap().pop();
    let single = serde_json::from_value::<SnapshotStoreConfig>(one)?;
    single.validate()?;
    assert!(!single.same_backend(&serde_json::from_value(good.clone())?));
    let mut empty = good.clone();
    empty["buckets"] = json!([]);
    assert!(
        serde_json::from_value::<SnapshotStoreConfig>(empty)?
            .validate()
            .is_err()
    );
    let mut three = good.clone();
    three["buckets"]
        .as_array_mut()
        .unwrap()
        .push(json!({"bucket":"rapid-test-c", "zone":"us-west4-c"}));
    assert!(
        serde_json::from_value::<SnapshotStoreConfig>(three)?
            .validate()
            .is_err()
    );
    let mut same_archive = good.clone();
    same_archive["archive_bucket"] = json!("rapid-test-a");
    assert!(
        serde_json::from_value::<SnapshotStoreConfig>(same_archive)?
            .validate()
            .is_err()
    );
    for field in ["bucket", "zone"] {
        let mut config = good.clone();
        config["buckets"][1][field] = config["buckets"][0][field].clone();
        assert!(
            serde_json::from_value::<SnapshotStoreConfig>(config)?
                .validate()
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn nearest_placement_needs_two_zones_and_the_default_is_not_persisted() -> Result<()> {
    let config = |placement: Option<&str>, zones: usize| -> Result<SnapshotStoreConfig> {
        let mut value = json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
            {"bucket":"rapid-test-a", "zone":"us-west1-a"},
            {"bucket":"rapid-test-b", "zone":"us-west4-a"}
        ]});
        value["buckets"].as_array_mut().unwrap().truncate(zones);
        if let Some(placement) = placement {
            value["placement"] = json!(placement);
        }
        Ok(serde_json::from_value(value)?)
    };
    let all = config(None, 2)?;
    let nearest = config(Some("nearest"), 2)?;
    nearest.validate()?;
    assert!(config(Some("nearest"), 1)?.validate().is_err());
    assert_eq!(config(Some("all"), 2)?, all);
    assert!(all.same_backend(&nearest));
    assert!(serde_json::to_value(&all)?.get("placement").is_none());
    assert_eq!(serde_json::to_value(&nearest)?["placement"], "nearest");
    Ok(())
}
