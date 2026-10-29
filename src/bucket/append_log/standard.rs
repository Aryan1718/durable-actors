use super::*;

pub(super) struct StandardSnapshots(Arc<dyn Bucket>);

impl StandardSnapshots {
    pub fn new(bucket: Arc<dyn Bucket>) -> Self {
        Self(bucket)
    }
}

#[async_trait]
impl SnapshotStore for StandardSnapshots {
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        ensure!(
            replace(self.0.as_ref(), &object_name(object)?, None, bytes).await?,
            "conflicting immutable checkpoint"
        );
        Ok(())
    }
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        Ok(self
            .0
            .get(&object_name(object)?)
            .await?
            .map(|copy| copy.bytes.into()))
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let physical = object_name(prefix)?;
        self.0
            .list(&physical)
            .await?
            .into_iter()
            .map(|key| {
                let suffix = key
                    .strip_prefix(&physical)
                    .context("snapshot list escaped its prefix")?;
                Ok(format!("{prefix}{}", suffix.replace('~', "/")))
            })
            .collect()
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let key = self
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|key| super::super::snapshots::version(&key).map(|v| (v, key)))
            .max_by_key(|(v, _)| *v);
        match key {
            Some((_, key)) => Ok(self.get(&key).await?.map(|bytes| (key, bytes))),
            None => Ok(None),
        }
    }
}
