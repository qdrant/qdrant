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

use bytes::Bytes;
use common::universal_io::{Result, UniversalIoError};
use io_bridge::AsyncAppend;
use object_store::ClientOptions;
use object_store::aws::{AmazonS3, AwsAuthorizer};
use object_store::client::{HttpClient, HttpConnector as _, HttpRequestBody, ReqwestConnector};
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
                    description: "append requires a source constructed from an AwsConfig \
                                  (append context missing)"
                        .to_string(),
                });
            };

            append_request(&store, &context, &key, offset, data).await
        }
    }
}

/// Issue one signed `PutObject` request with `x-amz-write-offset-bytes`,
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

    let mut url = context.object_url_base.clone();
    url.path_segments_mut()
        .expect("http(s) URLs can be a base")
        .pop_if_empty()
        .extend(key.parts().map(|part| part.as_ref().to_string()));

    let data_len = data.len() as u64;
    let mut request = http::Request::builder()
        .method(http::Method::PUT)
        .uri(url.as_str())
        .header(WRITE_OFFSET_HEADER, offset.to_string())
        .body(HttpRequestBody::from(data))
        .expect("statically valid request parts");

    // Signs all headers present on the request (including the write-offset
    // header) plus the payload SHA-256, and adds host/date/token headers.
    AwsAuthorizer::new(&credential, "s3", &context.region)
        .try_authorize(&mut request, None)
        .map_err(UniversalIoError::s3)?;

    // Append operation is currently executed with a custom HTTP client, because the object_store
    // crate does not support such operation.
    // See: <https://github.com/apache/arrow-rs-object-store/issues/632>
    let response = context
        .client()?
        .execute(request)
        .await
        .map_err(UniversalIoError::s3)?;
    let status = response.status();

    if status.is_success() {
        // Prefer the size reported by the backend; fall back to the size
        // implied by the acknowledged write.
        let new_len = response
            .headers()
            .get(OBJECT_SIZE_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(offset + data_len);
        return Ok(new_len);
    }

    // Read the body for the S3 error code (best-effort).
    let body = response.into_body().bytes().await.unwrap_or_default();
    let body_text = String::from_utf8_lossy(&body);

    let conflict = || UniversalIoError::AppendOffsetConflict {
        path: PathBuf::from(key.to_string()),
        offset,
    };

    match status {
        // AWS reports a write-offset mismatch as 400 InvalidWriteOffset;
        // some S3-compatibles use 412 instead.
        http::StatusCode::BAD_REQUEST if body_text.contains(INVALID_WRITE_OFFSET_CODE) => {
            Err(conflict())
        }
        http::StatusCode::PRECONDITION_FAILED => Err(conflict()),
        http::StatusCode::NOT_FOUND => Err(UniversalIoError::NotFound {
            path: PathBuf::from(key.to_string()),
        }),
        _ => {
            let excerpt: String = body_text.chars().take(512).collect();
            Err(UniversalIoError::s3(std::io::Error::other(format!(
                "append to {key} failed with status {status}: {excerpt}",
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
