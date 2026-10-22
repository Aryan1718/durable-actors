use super::{Bucket, BucketObject, StorageRevision};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_sdk_s3::{
    error::SdkError,
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError, get_object::GetObjectError,
        put_object::PutObjectError,
    },
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart},
};

const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const MAX_MULTIPART_PARTS: usize = 10_000;

pub struct S3Bucket {
    bucket: String,
    client: aws_sdk_s3::Client,
}

impl S3Bucket {
    pub fn new(bucket: &str, client: aws_sdk_s3::Client) -> Result<Self> {
        anyhow::ensure!(
            !bucket.is_empty() && !bucket.contains('/'),
            "invalid storage bucket"
        );
        Ok(Self {
            bucket: bucket.into(),
            client,
        })
    }
}

#[async_trait]
impl Bucket for S3Bucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.read(key, None).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        ensure!(length > 0, "range length must be positive");
        let end = start
            .checked_add(length - 1)
            .context("object range overflow")?;
        let object = self.read(key, Some((start, end))).await?;
        ensure!(
            object
                .as_ref()
                .is_none_or(|object| object.bytes.len() as u64 == length),
            "incomplete object range"
        );
        Ok(object)
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        revision: Option<StorageRevision>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let (if_match, if_none_match) = match revision {
            Some(StorageRevision::ETag(etag)) => (Some(etag), None),
            Some(StorageRevision::Generation(_)) => anyhow::bail!("S3 ETag required"),
            None => (None, Some("*".to_owned())),
        };
        if bytes.len() > MULTIPART_PART_SIZE {
            return self
                .multipart_compare_and_swap(key, if_match, if_none_match, bytes)
                .await;
        }
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if put_status(&error) == Some(412) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token = None;
        loop {
            let response = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .set_continuation_token(token)
                .send()
                .await?;
            keys.extend(
                response
                    .contents()
                    .iter()
                    .map(|object| object.key().context("listed S3 object missing key"))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(str::to_owned),
            );
            if !response.is_truncated().unwrap_or(false) {
                return Ok(keys);
            }
            token = Some(
                response
                    .next_continuation_token()
                    .context("truncated S3 listing missing continuation token")?
                    .to_owned(),
            );
        }
    }
}

impl S3Bucket {
    async fn multipart_compare_and_swap(
        &self,
        key: &str,
        if_match: Option<String>,
        if_none_match: Option<String>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let upload_id = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .send()
            .await?
            .upload_id()
            .context("S3 multipart upload missing upload ID")?
            .to_owned();
        let result = self.upload_parts(key, &upload_id, bytes).await;
        let parts = match result {
            Ok(parts) => parts,
            Err(error) => {
                self.abort_upload(key, &upload_id).await;
                return Err(error);
            }
        };
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        let result = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(upload)
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if complete_status(&error) == Some(412) => {
                self.abort_upload(key, &upload_id).await;
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        bytes: bytes::Bytes,
    ) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        let part_size = bytes
            .len()
            .div_ceil(MAX_MULTIPART_PARTS)
            .max(MULTIPART_PART_SIZE);
        for (index, bytes) in bytes.chunks(part_size).enumerate() {
            let response = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(i32::try_from(index + 1)?)
                .body(ByteStream::from(bytes.to_vec()))
                .checksum_algorithm(ChecksumAlgorithm::Crc32C)
                .send()
                .await?;
            parts.push(
                CompletedPart::builder()
                    .part_number(i32::try_from(index + 1)?)
                    .e_tag(response.e_tag().context("S3 multipart part missing ETag")?)
                    .checksum_crc32_c(
                        response
                            .checksum_crc32_c()
                            .context("S3 multipart part missing CRC32C checksum")?,
                    )
                    .build(),
            );
        }
        Ok(parts)
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) {
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
    }

    async fn read(&self, key: &str, range: Option<(u64, u64)>) -> Result<Option<BucketObject>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .set_range(range.map(|(start, end)| format!("bytes={start}-{end}")))
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if get_status(&error) == Some(404) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if let Some((start, end)) = range {
            validate_content_range(response.content_range(), start, end)?;
        }
        let revision = StorageRevision::ETag(
            response
                .e_tag()
                .context("S3 object missing ETag")?
                .to_owned(),
        );
        let bytes = response.body.collect().await?.into_bytes();
        Ok(Some(BucketObject { revision, bytes }))
    }
}

fn validate_content_range(value: Option<&str>, start: u64, end: u64) -> Result<()> {
    let total = value
        .and_then(|value| value.strip_prefix(&format!("bytes {start}-{end}/")))
        .context("S3 range response does not match the requested range")?;
    ensure!(
        total != "*",
        "S3 range response has an unknown object length"
    );
    let total: u64 = total.parse()?;
    ensure!(
        total > end,
        "S3 range response has an invalid object length"
    );
    Ok(())
}

fn get_status(error: &SdkError<GetObjectError>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn put_status(error: &SdkError<PutObjectError>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn complete_status(error: &SdkError<CompleteMultipartUploadError>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/s3.rs"]
mod tests;
