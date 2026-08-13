//! Shared transport state for the hand-signed S3 requests both S3 append
//! strategies are built on.

use std::sync::{Arc, OnceLock};

use common::universal_io::{UioResult, UniversalIoError};
use object_store::ClientOptions;
use object_store::client::{HttpClient, HttpConnector as _, ReqwestConnector};
use url::Url;

/// State for issuing hand-signed S3 requests — the operations the
/// `object_store` crate does not expose (write-offset appends,
/// `UploadPartCopy`): a lazily-built shared HTTP client plus the resolved
/// object-URL base, bucket, and signing region.
///
/// Built once per source; construction is cheap — the HTTP client (TLS
/// setup, connection pool) is only built on the first append, so sources
/// that never append pay nothing.
#[derive(Debug, Clone)]
pub struct SignedRequestContext {
    /// Reqwest-backed HTTP client, built on first use and shared across
    /// clones of the source (and thus across file handles opened from it).
    client: Arc<OnceLock<HttpClient>>,
    /// Whether to allow plain-http endpoints; mirrors `build_store`.
    allow_http: bool,
    /// Bucket name, as needed by the `x-amz-copy-source` header of the
    /// part-copy rewrites.
    pub(super) bucket: String,
    /// Base URL under which object keys live: path-style
    /// `{endpoint}/{bucket}` for custom endpoints, or the virtual-hosted
    /// `https://{bucket}.s3.{region}.amazonaws.com` for real AWS.
    object_url_base: Url,
    /// SigV4 signing region.
    pub(super) region: String,
}

impl SignedRequestContext {
    pub fn new(allow_http: bool, bucket: String, object_url_base: Url, region: String) -> Self {
        Self {
            client: Arc::new(OnceLock::new()),
            allow_http,
            bucket,
            object_url_base,
            region,
        }
    }

    /// The shared HTTP client, built on first call. Concurrent first calls
    /// may build a transient extra client; exactly one is kept.
    pub(super) fn client(&self) -> UioResult<HttpClient> {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }
        let client = build_http_client(self.allow_http)?;
        Ok(self.client.get_or_init(|| client).clone())
    }

    /// Absolute URL of the object at `key` under the context's base.
    pub(super) fn object_url(&self, key: &object_store::path::Path) -> UioResult<Url> {
        let mut url = self.object_url_base.clone();
        url.path_segments_mut()
            .map_err(|()| UniversalIoError::S3Config {
                description: "append object url cannot be a base".to_string(),
            })?
            .pop_if_empty()
            .extend(key.parts().map(|part| part.as_ref().to_string()));
        Ok(url)
    }
}

/// Build the reqwest-backed HTTP client the hand-issued append requests
/// run on.
pub(super) fn build_http_client(allow_http: bool) -> UioResult<HttpClient> {
    let mut options = ClientOptions::new();
    if allow_http {
        options = options.with_allow_http(true);
    }
    ReqwestConnector::default()
        .connect(&options)
        .map_err(|err| UniversalIoError::S3Config {
            description: format!("append http client: {err}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The HTTP client is built on first use and then reused; building it
    /// performs no IO.
    #[test]
    fn client_is_built_lazily_and_cached() {
        let context = SignedRequestContext::new(
            true,
            "bucket".to_string(),
            Url::parse("http://localhost:9000/bucket").unwrap(),
            "us-east-1".to_string(),
        );
        assert!(context.client.get().is_none());

        context.client().unwrap();
        assert!(context.client.get().is_some());
        context.client().unwrap();
    }
}
