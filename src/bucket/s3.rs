pub(crate) mod access;
pub(crate) mod bundle;
pub(crate) mod express;
pub(crate) mod host;

pub(crate) use access::{AwsStorage, AwsStorageCredentials};
pub(crate) use bundle::S3Bundle;
pub(crate) use express::{ExpressLogReply, ExpressLogRequest, ExpressLogs, S3ExpressLogs};

use super::{Bucket, BucketObject, StorageRevision};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use aws_sdk_s3::{
    config::{retry::RetryConfig, timeout::TimeoutConfig},
    error::{ProvideErrorMetadata, SdkError},
    operation::{
        complete_multipart_upload::CompleteMultipartUploadError, get_object::GetObjectError,
        put_object::PutObjectError,
    },
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart},
};
use std::{future::Future, time::Duration};

/// Stops AWS clients in actor code from reading the node's instance credentials.
pub(crate) const EXECUTOR_ENVIRONMENT: &[(&str, &str)] = &[("AWS_EC2_METADATA_DISABLED", "true")];

pub(crate) fn is_credential_variable(name: &str) -> bool {
    name.starts_with("AWS_") || matches!(name, "BOTO_CONFIG" | "BOTO_PATH")
}

const MULTIPART_PART_SIZE: usize = 5 * 1024 * 1024;
const MAX_MULTIPART_PARTS: usize = 10_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const PART_TIMEOUT: Duration = Duration::from_secs(60);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(300);
const LIST_TIMEOUT: Duration = Duration::from_secs(30);

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
            client: configured_client(client),
        })
    }
}

#[async_trait]
impl Bucket for S3Bucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        bounded(TRANSFER_TIMEOUT, self.read(key, None)).await
    }

    async fn range(&self, key: &str, start: u64, length: u64) -> Result<Option<BucketObject>> {
        ensure!(length > 0, "range length must be positive");
        let end = start
            .checked_add(length - 1)
            .context("object range overflow")?;
        let object = bounded(REQUEST_TIMEOUT, self.read(key, Some((start, end)))).await?;
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
            return bounded(
                TRANSFER_TIMEOUT,
                self.multipart_compare_and_swap(key, if_match, if_none_match, bytes),
            )
            .await;
        }
        self.put(key, if_match, if_none_match, bytes).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        bounded(LIST_TIMEOUT, self.list_pages(prefix)).await
    }
}

impl S3Bucket {
    async fn put(
        &self,
        key: &str,
        if_match: Option<String>,
        if_none_match: Option<String>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .checksum_algorithm(ChecksumAlgorithm::Crc32C)
            .set_if_match(if_match)
            .set_if_none_match(if_none_match)
            .customize()
            .config_override(single_attempt())
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if put_is_cas_miss(&error) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_pages(&self, prefix: &str) -> Result<Vec<String>> {
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
            .customize()
            .config_override(single_attempt())
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
            .customize()
            .config_override(single_attempt().timeout_config(transfer_timeouts(TRANSFER_TIMEOUT)))
            .send()
            .await;
        match result {
            Ok(_) => Ok(true),
            Err(error) if complete_is_cas_miss(&error) => {
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
                .customize()
                .config_override(
                    aws_sdk_s3::config::Builder::new()
                        .timeout_config(transfer_timeouts(PART_TIMEOUT)),
                )
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
        match range {
            Some((start, end)) => validate_content_range(response.content_range(), start, end)?,
            None => ensure!(
                response.content_range().is_none(),
                "S3 returned a partial object for a full download"
            ),
        }
        let revision = StorageRevision::ETag(
            response
                .e_tag()
                .context("S3 object missing ETag")?
                .to_owned(),
        );
        let expected_length = response
            .content_length()
            .map(u64::try_from)
            .transpose()
            .context("S3 object has a negative content length")?;
        let bytes = download(response.body).await?;
        ensure!(
            expected_length.is_none_or(|length| length == bytes.len() as u64),
            "incomplete S3 object download"
        );
        Ok(Some(BucketObject { revision, bytes }))
    }
}

fn configured_client(client: aws_sdk_s3::Client) -> aws_sdk_s3::Client {
    let timeout = TimeoutConfig::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(Duration::from_secs(5))
        .operation_attempt_timeout(Duration::from_secs(5))
        .operation_timeout(REQUEST_TIMEOUT)
        .build();
    let retry = RetryConfig::standard()
        .with_max_attempts(3)
        .with_initial_backoff(Duration::from_millis(100))
        .with_max_backoff(Duration::from_secs(1));
    aws_sdk_s3::Client::from_conf(
        client
            .config()
            .to_builder()
            .timeout_config(timeout)
            .retry_config(retry)
            .build(),
    )
}

async fn bounded<T>(timeout: Duration, operation: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(timeout, operation)
        .await
        .context("S3 operation timed out")?
}

fn single_attempt() -> aws_sdk_s3::config::Builder {
    // Creation is not idempotent; publication retries require content reconciliation.
    aws_sdk_s3::config::Builder::new().retry_config(RetryConfig::disabled())
}

fn transfer_timeouts(attempt: Duration) -> TimeoutConfig {
    // An override replaces the client's timeouts wholesale, so every limit is restated.
    TimeoutConfig::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(attempt)
        .operation_attempt_timeout(attempt)
        .operation_timeout(TRANSFER_TIMEOUT)
        .build()
}

async fn download(mut body: ByteStream) -> Result<bytes::Bytes> {
    let mut download = crate::payload::Download::new();
    while let Some(chunk) = body.next().await {
        download = download.append(chunk?).await?;
    }
    download.finish().await
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

fn put_is_cas_miss(error: &SdkError<PutObjectError>) -> bool {
    put_status(error) == Some(412)
        || put_status(error) == Some(409)
            && error.as_service_error().and_then(|error| error.code())
                == Some("ConditionalRequestConflict")
}

fn complete_status(error: &SdkError<CompleteMultipartUploadError>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

fn complete_is_cas_miss(error: &SdkError<CompleteMultipartUploadError>) -> bool {
    complete_status(error) == Some(412)
        || complete_status(error) == Some(409)
            && error.as_service_error().and_then(|error| error.code())
                == Some("ConditionalRequestConflict")
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/s3.rs"]
mod tests;
