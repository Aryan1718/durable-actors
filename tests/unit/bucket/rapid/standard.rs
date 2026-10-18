use super::*;
use crate::{actor::ActorKey, bucket::FileBucket};
use std::sync::Arc;

#[tokio::test]
async fn injected_bucket_preserves_standard_snapshot_layout_and_behavior() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(FileBucket::new(directory.path().into())?);
    let snapshots = StandardSnapshots::new(bucket.clone());
    let actor = ActorKey {
        project_id: "standard-tests".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let first = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
    let second = crate::storage::snapshot_object_name(&actor, 2, &format!("{:032x}", 1))?;
    let prefix = first.strip_suffix("1.json").unwrap();

    snapshots.put(&first, Bytes::from_static(b"first")).await?;
    snapshots
        .put(&second, Bytes::from_static(b"second"))
        .await?;

    assert_eq!(
        bucket.list("").await?,
        [object_name(&first)?, object_name(&second)?]
    );
    assert_eq!(
        snapshots.list(prefix).await?,
        [first.clone(), second.clone()]
    );
    assert_eq!(
        snapshots.get(&first).await?,
        Some(Bytes::from_static(b"first"))
    );
    assert_eq!(
        snapshots.latest(prefix).await?,
        Some((second.clone(), Bytes::from_static(b"second")))
    );
    snapshots.put(&first, Bytes::from_static(b"first")).await?;
    assert!(
        snapshots
            .put(&first, Bytes::from_static(b"conflict"))
            .await
            .is_err()
    );
    Ok(())
}
