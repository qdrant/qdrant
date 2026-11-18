//! Native single-request append for S3-compatible stores.
//!
//! The `object_store` crate has no append support, so this module issues the
//! `PutObject` + `x-amz-write-offset-bytes` request itself, reusing the
//! store's credential chain ([`AmazonS3::credentials`]) and `object_store`'s
//! SigV4 [`AwsAuthorizer`] (which signs every header present on the request,
//! including the write-offset header).
//!
//! The write-offset append API exists on AWS S3 Express One Zone directory
//! buckets and on S3-compatible stores that adopted it (e.g. MinIO AiStor) —
//! plain S3 Standard buckets reject it.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
use object_store::ObjectStoreExt as _;
use object_store::aws::{AmazonS3, AwsAuthorizer};
use object_store::client::HttpRequestBody;

use super::crc64nvme;
use super::part_copy::PartCopyAppend;
use super::signed::SignedRequestContext;

/// Response header carrying the object size after an append.
const OBJECT_SIZE_HEADER: &str = "x-amz-object-size";

/// Request header selecting the write-offset append behavior of `PutObject`.
pub(super) const WRITE_OFFSET_HEADER: &str = "x-amz-write-offset-bytes";

/// S3 error code returned when the write offset does not match the current
/// object size.
const INVALID_WRITE_OFFSET_CODE: &str = "InvalidWriteOffset";

/// S3 error code distinguishing a missing *object* (recoverable stale view)
/// from a missing *bucket* (a config/endpoint problem) on a 404.
const NO_SUCH_KEY_CODE: &str = "NoSuchKey";

/// S3 error code returned when the object reached the store's cap on
/// appended blocks (10,000 parts); the object can only keep growing through
/// a whole-object rewrite.
const TOO_MANY_PARTS_CODE: &str = "TooManyParts";

/// Retry limit for connection errors, 5xx and 429 responses.
const MAX_ATTEMPTS: u32 = 3;

/// Backoff before retry attempt `n` is `n * RETRY_BACKOFF`.
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// The native append strategy: a signed single-request `PutObject` with
/// `x-amz-write-offset-bytes`, atomically growing the object in place.
#[derive(Debug, Clone)]
pub struct NativeAppend {
    signed: SignedRequestContext,
}

impl NativeAppend {
    pub fn new(signed: SignedRequestContext) -> Self {
        Self { signed }
    }

    /// Append `data` at `offset` (== the current object size). Returns the
    /// new total object size.
    pub(in crate::append) async fn append(
        &self,
        store: &Arc<AmazonS3>,
        key: &object_store::path::Path,
        offset: u64,
        data: Bytes,
    ) -> UioResult<u64> {
        append_request(store, &self.signed, key, offset, data).await
    }

    /// The part-copy strategy over the same store, used to compact an
    /// object that reached the appended-block cap — every native-append
    /// store is multipart-capable.
    pub(in crate::append) fn part_copy(&self) -> PartCopyAppend {
        PartCopyAppend::new(self.signed.clone())
    }
}

/// Issue a signed `PutObject` request with `x-amz-write-offset-bytes`,
/// atomically growing the object at `key` by `data`. Returns the new total
/// object size.
async fn append_request(
    store: &Arc<AmazonS3>,
    context: &SignedRequestContext,
    key: &object_store::path::Path,
    offset: u64,
    data: Bytes,
) -> UioResult<u64> {
    let credential = store
        .credentials()
        .get_credential()
        .await
        .map_err(UniversalIoError::s3)?;
    let client = context.client()?;
    let url = context.object_url(key)?;

    let data_len = data.len() as u64;

    // The request is executed with a custom HTTP client, because the
    // object_store crate does not support the append operation.
    // See: <https://github.com/apache/arrow-rs-object-store/issues/632>
    let mut attempt = 1;
    let checksum = crc64nvme(&data);
    let mut may_have_appended = false;
    loop {
        // Built and signed per attempt: the SigV4 signature embeds the
        // request date. Building can fail — `url` accepts URIs the `http`
        // crate rejects (e.g. longer than u16::MAX bytes).
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(url.as_str())
            .header(WRITE_OFFSET_HEADER, offset.to_string())
            .header("x-amz-checksum-crc64nvme", &checksum)
            .body(HttpRequestBody::from(data.clone()))
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("append request for {key}: {err}"),
            })?;

        // Signs all headers present on the request (including the
        // write-offset header) plus the payload SHA-256, and adds
        // host/date/token headers.
        AwsAuthorizer::new(&credential, context.service, &context.region)
            .try_authorize(&mut request, None)
            .map_err(UniversalIoError::s3)?;

        let response = match client.execute(request).await {
            Ok(response) => response,
            Err(_) if attempt < MAX_ATTEMPTS => {
                may_have_appended = true;
                tokio::time::sleep(RETRY_BACKOFF * attempt).await;
                attempt += 1;
                continue;
            }
            Err(err) => return Err(UniversalIoError::s3(err)),
        };
        let status = response.status();

        if (status.is_server_error() || status == http::StatusCode::TOO_MANY_REQUESTS)
            && attempt < MAX_ATTEMPTS
        {
            may_have_appended = true;
            tokio::time::sleep(RETRY_BACKOFF * attempt).await;
            attempt += 1;
            continue;
        }

        if status.is_success() {
            let object_size = response
                .headers()
                .get(OBJECT_SIZE_HEADER)
                .map(|value| {
                    value
                        .to_str()
                        .map_err(UniversalIoError::s3)?
                        .parse::<u64>()
                        .map_err(UniversalIoError::s3)
                })
                .transpose()?;

            return match object_size {
                Some(new_len) if new_len == offset + data_len => Ok(new_len),
                // The store confirmed the append but reports a final size
                // that disagrees with `offset + data`: either the write
                // offset was not honored or the single-writer contract was
                // violated — fail instead of returning a length that
                // disagrees with the object.
                Some(new_len) => Err(UniversalIoError::s3(std::io::Error::other(format!(
                    "append to {key} at offset {offset} reported object size {new_len}, \
                     expected {expected}",
                    expected = offset + data_len,
                )))),
                // At offset 0 the append is equivalent to a whole-object
                // write, so even a store without write-offset support
                // produced the right object.
                None if offset == 0 => Ok(data_len),
                // A store without write-offset support may accept the PUT
                // as a plain PutObject — REPLACING the object with just
                // `data`. The size header is the only success signal that
                // distinguishes a true append (AWS and MinIO AiStor return
                // it); treat its absence as an error instead of risking
                // silent data loss on every subsequent append.
                None => Err(UniversalIoError::s3(std::io::Error::other(format!(
                    "append to {key} was accepted without the {OBJECT_SIZE_HEADER} response \
                     header; the store likely does not support write-offset appends and may \
                     have replaced the object instead",
                )))),
            };
        }

        // Read the body for the S3 error code (best-effort).
        let body = response.into_body().bytes().await.unwrap_or_default();
        let body_text = String::from_utf8_lossy(&body);

        // The appended-block cap is a permanent property of the object —
        // report it as rewrite-required so the caller can recover, instead
        // of an opaque failure. Anything unrecognized stays a hard error
        // rather than silently triggering a full-object rewrite.
        if status == http::StatusCode::BAD_REQUEST && body_text.contains(TOO_MANY_PARTS_CODE) {
            return Err(UniversalIoError::AppendRewriteRequired {
                path: PathBuf::from(key.to_string()),
            });
        }

        // AWS reports a write-offset mismatch as 400 InvalidWriteOffset;
        // some S3-compatibles use 412 instead.
        let write_offset_conflict = match status {
            http::StatusCode::BAD_REQUEST => body_text.contains(INVALID_WRITE_OFFSET_CODE),
            http::StatusCode::PRECONDITION_FAILED => true,
            // A missing *object* while a nonzero offset was expected is a
            // stale view of the object (deleted behind our back) — the same
            // reopen-and-retry recovery as an offset mismatch, matching the
            // in-memory emulation; stores that return bodiless 404s keep
            // that recovery. A missing *bucket* is a config/endpoint
            // problem and must stay a hard error — it is also the runtime
            // guard against drift in the zonal-endpoint derivation. At
            // offset 0 a 404 is a genuine missing-target error.
            http::StatusCode::NOT_FOUND => {
                offset > 0
                    && super::extract_xml_tag(&body_text, "Code")
                        .is_none_or(|code| code == NO_SUCH_KEY_CODE)
            }
            _ => false,
        };

        if write_offset_conflict {
            // A conflict on a retried attempt may just mean the earlier,
            // lost-acknowledgement attempt landed; under the single-writer
            // contract a matching object size proves the tail is ours.
            if may_have_appended
                && let Ok(meta) = store.head(key).await
                && meta.size == offset + data_len
            {
                return Ok(meta.size);
            }

            return Err(UniversalIoError::AppendOffsetConflict {
                path: PathBuf::from(key.to_string()),
                offset,
            });
        }

        return match status {
            // Only a missing object is a plain not-found; a NoSuchBucket
            // body falls through to the loud error carrying the excerpt.
            http::StatusCode::NOT_FOUND
                if super::extract_xml_tag(&body_text, "Code")
                    .is_none_or(|code| code == NO_SUCH_KEY_CODE) =>
            {
                Err(UniversalIoError::NotFound {
                    path: PathBuf::from(key.to_string()),
                })
            }
            _ => {
                let excerpt: String = body_text.chars().take(512).collect();
                Err(UniversalIoError::s3(std::io::Error::other(format!(
                    "append to {key} failed with status {status}: {excerpt}",
                ))))
            }
        };
    }
}

#[cfg(test)]
mod tests;
