use super::*;
use aws_lc_rs::digest::SHA256;
use std::sync::Mutex;

fn bundle() -> S3Bundle {
    S3Bundle {
        bucket: "code-bucket".into(),
        region: "us-west-2".into(),
        files: vec![artifact("actors.mjs", b"code")],
    }
}

fn artifact(path: &str, bytes: &[u8]) -> S3BundleFile {
    S3BundleFile {
        path: path.into(),
        object: format!("durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/{path}"),
        etag: "\"opaque-etag\"".into(),
        sha256: URL_SAFE_NO_PAD.encode(aws_lc_rs::digest::digest(&SHA256, bytes).as_ref()),
    }
}

#[test]
fn s3_manifests_reject_unsafe_or_inconsistent_identities() {
    for path in [
        "../outside",
        "/absolute",
        "a//b",
        "a/./b",
        "a\\b",
        "a:b",
        "a?b",
        "a*b",
        "${name}",
    ] {
        let mut bundle = bundle();
        bundle.files.push(artifact(path, b"dependency"));
        assert!(bundle.encode().is_err(), "accepted {path}");
    }
    for object in [
        "other/actors.mjs",
        "durable-actors/v3/artifacts/not-a-uuid/actors.mjs",
        "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/other.mjs",
    ] {
        let mut bundle = bundle();
        bundle.files[0].object = object.into();
        assert!(bundle.encode().is_err());
    }
    let mut bundle = bundle();
    bundle.files.push(artifact("dependency", b"dependency"));
    bundle.files[1].object = bundle.files[1].object.replace("12345678", "87654321");
    assert!(bundle.encode().is_err());
    let mut empty_etag = self::bundle();
    empty_etag.files[0].etag.clear();
    assert!(empty_etag.encode().is_err());
}

#[tokio::test]
async fn s3_install_verifies_all_files_before_publishing() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut bundle = bundle();
    bundle
        .files
        .push(artifact("python/dependency.so", b"dependency"));
    let reader = Reader::new(&bundle, vec![Ok(b"code"), Ok(b"wrong")]);
    assert!(bundle.install(root.path(), &reader).await.is_err());
    assert!(!root.path().join("actors.mjs").exists());
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    let reader = Reader::new(&bundle, vec![Ok(b"code"), Ok(b"dependency")]);
    bundle.install(root.path(), &reader).await?;
    assert_eq!(
        tokio::fs::read(root.path().join("actors.mjs")).await?,
        b"code"
    );
    assert_eq!(
        tokio::fs::read(root.path().join("python/dependency.so")).await?,
        b"dependency"
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_roots_and_parent_directories_are_rejected_before_reading() -> Result<()> {
    let parent = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    let root_link = parent.path().join("root");
    std::os::unix::fs::symlink(outside.path(), &root_link)?;
    let bundle = bundle();
    let reader = Reader::new(&bundle, vec![Ok(b"code")]);
    assert!(bundle.install(&root_link, &reader).await.is_err());
    assert_eq!(reader.objects.lock().unwrap().len(), 1);
    let mut bundle = bundle;
    bundle
        .files
        .push(artifact("python/dependency.so", b"dependency"));
    let reader = Reader::new(&bundle, vec![Ok(b"code"), Ok(b"dependency")]);
    std::os::unix::fs::symlink(outside.path(), parent.path().join("python"))?;
    assert!(bundle.install(parent.path(), &reader).await.is_err());
    assert_eq!(reader.objects.lock().unwrap().len(), 2);
    assert_eq!(std::fs::read_dir(outside.path())?.count(), 0);
    Ok(())
}

struct Reader {
    bucket: String,
    region: String,
    keys: Mutex<Vec<String>>,
    objects: Mutex<Vec<Result<Option<BucketObject>>>>,
}

impl Reader {
    fn new(bundle: &S3Bundle, contents: Vec<Result<&'static [u8]>>) -> Self {
        let objects = contents
            .into_iter()
            .zip(&bundle.files)
            .map(|(bytes, file)| {
                bytes.map(|bytes| {
                    Some(BucketObject {
                        revision: StorageRevision::ETag(file.etag.clone()),
                        bytes: Bytes::from_static(bytes),
                    })
                })
            })
            .collect();
        Self {
            bucket: bundle.bucket.clone(),
            region: bundle.region.clone(),
            keys: Mutex::new(
                bundle
                    .files
                    .iter()
                    .map(|file| file.object.clone())
                    .collect(),
            ),
            objects: Mutex::new(objects),
        }
    }
}

#[async_trait]
impl ArtifactReader for Reader {
    async fn download(
        &self,
        bucket: &str,
        region: &str,
        object: &str,
    ) -> Result<Option<BucketObject>> {
        assert_eq!(bucket, self.bucket);
        assert_eq!(region, self.region);
        assert_eq!(object, self.keys.lock().unwrap().remove(0));
        self.objects.lock().unwrap().remove(0)
    }
}
