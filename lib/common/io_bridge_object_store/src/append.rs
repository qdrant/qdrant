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
//! plain S3 Standard buckets reject it. MinIO-AiStor-compatible stores are
//! the primary supported target for now; real S3 Express directory buckets
//! use zonal endpoints and session-token auth that have not been verified
//! against this implementation yet.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError};
use io_bridge::AsyncAppend;
use object_store::aws::{AmazonS3, AwsAuthorizer};
use object_store::client::{HttpClient, HttpConnector as _, HttpRequestBody, ReqwestConnector};
use object_store::{ClientOptions, ObjectStoreExt as _};
use url::Url;

use crate::source::ObjectStoreSource;

/// Response header carrying the object size after an append.
const OBJECT_SIZE_HEADER: &str = "x-amz-object-size";

/// Request header selecting the write-offset append behavior of `PutObject`.
const WRITE_OFFSET_HEADER: &str = "x-amz-write-offset-bytes";

/// S3 error code returned when the write offset does not match the current
/// object size.
const INVALID_WRITE_OFFSET_CODE: &str = "InvalidWriteOffset";

/// State for issuing native append requests: a lazily-built shared HTTP
/// client plus the resolved object-URL base and signing region.
///
/// Built once per source by [`BlobBackend::append_context`]; construction is
/// cheap — the HTTP client (TLS setup, connection pool) is only built on the
/// first append, so sources that never append pay nothing.
///
/// [`BlobBackend::append_context`]: crate::BlobBackend::append_context
#[derive(Debug, Clone)]
pub struct AppendContext {
    /// Reqwest-backed HTTP client, built on first use and shared across
    /// clones of the source (and thus across file handles opened from it).
    client: Arc<OnceLock<HttpClient>>,
    /// Whether to allow plain-http endpoints; mirrors `build_store`.
    allow_http: bool,
    /// Base URL under which object keys live: path-style
    /// `{endpoint}/{bucket}` for custom endpoints, or the virtual-hosted
    /// `https://{bucket}.s3.{region}.amazonaws.com` for real AWS.
    object_url_base: Url,
    /// SigV4 signing region.
    region: String,
}

impl AppendContext {
    pub fn new(allow_http: bool, object_url_base: Url, region: String) -> Self {
        Self {
            client: Arc::new(OnceLock::new()),
            allow_http,
            object_url_base,
            region,
        }
    }

    /// The shared HTTP client, built on first call. Concurrent first calls
    /// may build a transient extra client; exactly one is kept.
    fn client(&self) -> Result<HttpClient> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }

        let mut options = ClientOptions::new();
        if self.allow_http {
            options = options.with_allow_http(true);
        }
        let client = ReqwestConnector::default()
            .connect(&options)
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("append http client: {err}"),
            })?;

        Ok(self.client.get_or_init(|| client).clone())
    }
}

impl AsyncAppend for ObjectStoreSource<AmazonS3> {
    fn append(
        &self,
        path: &std::path::Path,
        offset: u64,
        data: Bytes,
    ) -> impl Future<Output = Result<u64>> + Send + 'static {
        let store = self.store().clone();
        let context = self.append_context().cloned();
        let key = crate::source::build_key(path);

        async move {
            let Some(context) = context else {
                return Err(UniversalIoError::S3Config {
                    description: "append is not supported for this S3 backend/config (append context missing)"
                        .to_string(),
                });
            };

            append_request(&store, &context, &key, offset, data).await
        }
    }
}

/// Total attempts for one append: transient failures (connection errors,
/// 5xx, 429) are retried with a short linear backoff, like `object_store`
/// does for its own requests. Retrying is safe: the write offset acts as a
/// compare-and-swap, and a conflict caused by a lost-acknowledgement
/// attempt is reconciled in [`append_request`].
const MAX_ATTEMPTS: u32 = 3;

/// Backoff before retry attempt `n` is `n * RETRY_BACKOFF`.
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Issue a signed `PutObject` request with `x-amz-write-offset-bytes`,
/// atomically growing the object at `key` by `data`. Returns the new total
/// object size.
async fn append_request(
    store: &Arc<AmazonS3>,
    context: &AppendContext,
    key: &object_store::path::Path,
    offset: u64,
    data: Bytes,
) -> Result<u64> {
    let credential = store
        .credentials()
        .get_credential()
        .await
        .map_err(UniversalIoError::s3)?;
    let client = context.client()?;

    let mut url = context.object_url_base.clone();
    url.path_segments_mut()
        .map_err(|()| UniversalIoError::S3Config {
            description: "append object url cannot be a base".to_string(),
        })?
        .pop_if_empty()
        .extend(key.parts().map(|part| part.as_ref().to_string()));

    let data_len = data.len() as u64;

    // The request is executed with a custom HTTP client, because the
    // object_store crate does not support the append operation.
    // See: <https://github.com/apache/arrow-rs-object-store/issues/632>
    let mut attempt = 1;
    loop {
        // Built and signed per attempt: the SigV4 signature embeds the
        // request date. Building can fail — `url` accepts URIs the `http`
        // crate rejects (e.g. longer than u16::MAX bytes).
        let mut request = http::Request::builder()
            .method(http::Method::PUT)
            .uri(url.as_str())
            .header(WRITE_OFFSET_HEADER, offset.to_string())
            .body(HttpRequestBody::from(data.clone()))
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("append request for {key}: {err}"),
            })?;

        // Signs all headers present on the request (including the
        // write-offset header) plus the payload SHA-256, and adds
        // host/date/token headers.
        AwsAuthorizer::new(&credential, "s3", &context.region)
            .try_authorize(&mut request, None)
            .map_err(UniversalIoError::s3)?;

        let response = match client.execute(request).await {
            Ok(response) => response,
            Err(_) if attempt < MAX_ATTEMPTS => {
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

        // AWS reports a write-offset mismatch as 400 InvalidWriteOffset;
        // some S3-compatibles use 412 instead.
        let write_offset_conflict = match status {
            http::StatusCode::BAD_REQUEST => body_text.contains(INVALID_WRITE_OFFSET_CODE),
            http::StatusCode::PRECONDITION_FAILED => true,
            // A missing object while a nonzero offset was expected is a
            // stale view of the object (deleted behind our back) — the same
            // reopen-and-retry recovery as an offset mismatch, matching the
            // in-memory emulation. At offset 0 a 404 is a genuine
            // missing-target error (e.g. missing bucket).
            http::StatusCode::NOT_FOUND => offset > 0,
            _ => false,
        };

        if write_offset_conflict {
            // A conflict on a retried attempt may just mean the earlier,
            // lost-acknowledgement attempt landed; under the single-writer
            // contract a matching object size proves the tail is ours.
            if attempt > 1
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
            http::StatusCode::NOT_FOUND => Err(UniversalIoError::NotFound {
                path: PathBuf::from(key.to_string()),
            }),
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
mod tests {
    use object_store::aws::AmazonS3Builder;

    use super::*;

    /// Request building failures (URIs the `http` crate rejects) surface as
    /// errors instead of panics. Fails before any network IO.
    #[test]
    fn unbuildable_request_is_an_error_not_a_panic() {
        let store = Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name("bucket")
                .with_region("us-east-1")
                .with_access_key_id("id")
                .with_secret_access_key("secret")
                .build()
                .unwrap(),
        );
        let context = AppendContext::new(
            true,
            Url::parse("http://localhost:9000/bucket").unwrap(),
            "us-east-1".to_string(),
        );
        // `url` accepts this; `http` caps URIs at u16::MAX bytes.
        let key = object_store::path::Path::from("k".repeat(70_000));

        let result = io_bridge::BridgeRuntime::global().block_on(append_request(
            &store,
            &context,
            &key,
            0,
            Bytes::from_static(b"data"),
        ));
        assert!(matches!(result, Err(UniversalIoError::S3Config { .. })));
    }

    /// The HTTP client is built on first use and then reused; building it
    /// performs no IO.
    #[test]
    fn client_is_built_lazily_and_cached() {
        let context = AppendContext::new(
            true,
            Url::parse("http://localhost:9000/bucket").unwrap(),
            "us-east-1".to_string(),
        );
        assert!(context.client.get().is_none());

        context.client().unwrap();
        assert!(context.client.get().is_some());
        context.client().unwrap();
    }
}
