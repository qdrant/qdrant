//! Appends on GCS through its S3-compatible XML API, with the HMAC keys the
//! S3 backend already holds: the appended data is uploaded as a temporary
//! object, then the destination is atomically rewritten as the server-side
//! concatenation `compose([existing, temporary])`, as [`ComposeAppend`]
//! does over the JSON API.
//!
//! The part-copy rewrite cannot run here: GCS's XML API does not implement
//! `UploadPartCopy` for multipart uploads.
//!
//! The compose request is signed `GOOG4-HMAC-SHA256` (see [`goog4`]), not
//! SigV4: its generation precondition is an `x-goog-*` header, and GCS
//! rejects requests mixing `x-amz-*` with `x-goog-*` headers. The XML API
//! silently ignores `If-Match` and `x-amz-if-generation-match` on compose,
//! so this is the only form of the compare-and-swap it honors.
//!
//! [`ComposeAppend`]: super::ComposeAppend
//! [`goog4`]: super::goog4

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
use object_store::aws::AmazonS3;
use object_store::client::HttpRequestBody;
use object_store::{ObjectStoreExt as _, PutPayload};
use url::Url;

use super::goog4;
use super::signed::SignedRequestContext;

/// Header carrying the destination generation the compose is conditional on.
const IF_GENERATION_MATCH: &str = "x-goog-if-generation-match";
/// Response header with an object's generation.
const GENERATION: &str = "x-goog-generation";

/// The GCS-interop append strategy for the S3 backend: XML API `compose`,
/// conditional on the destination generation.
#[derive(Debug, Clone)]
pub struct XmlComposeAppend {
    signed: SignedRequestContext,
}

impl XmlComposeAppend {
    pub fn new(signed: SignedRequestContext) -> Self {
        Self { signed }
    }

    /// Append `data` at `offset` (== the current object size) by composing
    /// the existing object with the uploaded data. Returns the new total
    /// object size.
    ///
    /// The compose is conditional on the destination generation observed
    /// before the upload, so a concurrent rewrite surfaces as an offset
    /// conflict instead of silently losing data. That generation is the
    /// compare-and-swap token here, so no etag is taken: a composed
    /// object's etag is not a content hash anyway.
    pub(in crate::append) async fn append(
        &self,
        store: &Arc<AmazonS3>,
        key: &object_store::path::Path,
        offset: u64,
        data: Bytes,
    ) -> UioResult<u64> {
        let final_size = offset + data.len() as u64;

        // An empty prefix means a plain put produces the right object, but
        // only if there is no object yet. An existing object, even an empty
        // one, takes the compose path below, which checks its size.
        if offset == 0 && self.put_if_absent(store, key, data.clone()).await? {
            return Ok(final_size);
        }

        // Observe the destination: its size must match the caller's view of
        // the object, and its generation anchors the compose below.
        let generation = self.head_generation(store, key, offset).await?;

        // The appended bytes as a temporary neighbor object, uploaded through
        // the store so they carry its end-to-end checksum. The name is
        // deterministic: under the single-writer contract a collision is a
        // leftover of our own failed attempt, and overwriting it is right.
        // The contract relies on the caller's shard write lock: if two
        // writers overlap, one may compose the other's bytes.
        let temp_key = object_store::path::Path::from(format!("{key}.compose-append"));
        store
            .put(&temp_key, PutPayload::from(data))
            .await
            .map_err(UniversalIoError::s3)?;

        let result = self
            .compose_request(store, key, &temp_key, &generation, offset)
            .await;

        // Best effort: an orphaned temporary object only wastes its own
        // bytes and is overwritten by the next append anyway.
        let _ = store.delete(&temp_key).await;

        result?;
        Ok(final_size)
    }

    /// `PUT {key}` conditional on the object not existing yet
    /// (`x-goog-if-generation-match: 0`). Returns `false`, having written
    /// nothing, if it already exists.
    async fn put_if_absent(
        &self,
        store: &Arc<AmazonS3>,
        key: &object_store::path::Path,
        data: Bytes,
    ) -> UioResult<bool> {
        let (status, _headers, body) = self
            .request(
                store,
                http::Method::PUT,
                key,
                None,
                &[(IF_GENERATION_MATCH, "0".to_string())],
                data,
            )
            .await?;
        if status == http::StatusCode::PRECONDITION_FAILED {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(failure("create for compose append", key, status, &body));
        }
        Ok(true)
    }

    /// `HEAD` the destination for its generation, checking its size is
    /// `offset`. A missing object or another size is an offset conflict.
    async fn head_generation(
        &self,
        store: &Arc<AmazonS3>,
        key: &object_store::path::Path,
        offset: u64,
    ) -> UioResult<String> {
        let conflict = || UniversalIoError::AppendOffsetConflict {
            path: PathBuf::from(key.to_string()),
            offset,
        };

        let (status, headers, body) = self
            .request(store, http::Method::HEAD, key, None, &[], Bytes::new())
            .await?;
        if status == http::StatusCode::NOT_FOUND {
            return Err(conflict());
        }
        if !status.is_success() {
            return Err(failure("head for compose append", key, status, &body));
        }

        let size = header(&headers, http::header::CONTENT_LENGTH.as_str())
            .and_then(|value| value.parse::<u64>().ok());
        if size != Some(offset) {
            return Err(conflict());
        }
        header(&headers, GENERATION)
            .map(str::to_string)
            .ok_or_else(|| {
                UniversalIoError::s3(std::io::Error::other(format!(
                    "head for compose append of {key}: no {GENERATION} header, \
                     is the endpoint GCS?",
                )))
            })
    }

    /// `PUT {dest}?compose=` — atomically replace `dest` with the
    /// concatenation of `dest` and `temp`, conditional on `dest` still being
    /// at `generation`.
    async fn compose_request(
        &self,
        store: &Arc<AmazonS3>,
        dest: &object_store::path::Path,
        temp: &object_store::path::Path,
        generation: &str,
        offset: u64,
    ) -> UioResult<()> {
        let body = compose_body(&[dest, temp]);
        let (status, _headers, response) = self
            .request(
                store,
                http::Method::PUT,
                dest,
                Some("compose="),
                &[
                    ("content-type", "application/xml".to_string()),
                    (IF_GENERATION_MATCH, generation.to_string()),
                ],
                Bytes::from(body),
            )
            .await?;

        // The generation moved: the object was rewritten since we observed
        // it — the compose caught a lost race.
        if status == http::StatusCode::PRECONDITION_FAILED {
            return Err(UniversalIoError::AppendOffsetConflict {
                path: PathBuf::from(dest.to_string()),
                offset,
            });
        }
        if !status.is_success() {
            return Err(failure("compose", dest, status, &response));
        }
        Ok(())
    }

    /// One `GOOG4`-signed request against the object at `key`. Returns the
    /// status, response headers and body; the caller interprets the status.
    async fn request(
        &self,
        store: &Arc<AmazonS3>,
        method: http::Method,
        key: &object_store::path::Path,
        query: Option<&str>,
        headers: &[(&str, String)],
        body: Bytes,
    ) -> UioResult<(http::StatusCode, http::HeaderMap, Bytes)> {
        let credential = store
            .credentials()
            .get_credential()
            .await
            .map_err(UniversalIoError::s3)?;
        let url = self.canonical_object_url(key, query)?;

        let signed_headers = goog4::sign(
            &credential,
            &self.signed.region,
            &method,
            &url,
            headers,
            &body,
            chrono::Utc::now(),
        );

        let mut builder = http::Request::builder().method(method).uri(url.as_str());
        for (name, value) in &signed_headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        // Explicit even when zero: GCS answers 411 to a bodiless request
        // without it.
        builder = builder.header(http::header::CONTENT_LENGTH, body.len());
        let request = builder.body(HttpRequestBody::from(body)).map_err(|err| {
            UniversalIoError::S3Config {
                description: format!("compose append request for {key}: {err}"),
            }
        })?;

        let response = self
            .signed
            .client()?
            .execute(request)
            .await
            .map_err(UniversalIoError::s3)?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let body = response.into_body().bytes().await.unwrap_or_default();
        Ok((status, response_headers, body))
    }

    /// The object URL with its path percent-encoded as V4 signing expects,
    /// and `query` as the literal canonical query string. The path sent is
    /// the path signed.
    fn canonical_object_url(
        &self,
        key: &object_store::path::Path,
        query: Option<&str>,
    ) -> UioResult<Url> {
        let mut url = self
            .signed
            .object_url(&object_store::path::Path::default())?;
        let base = url.path().trim_end_matches('/').to_string();
        let object: Vec<String> = key
            .as_ref()
            .split('/')
            .map(goog4::encode_path_segment)
            .collect();
        url.set_path(&format!("{base}/{}", object.join("/")));
        url.set_query(query);
        Ok(url)
    }
}

/// The `<ComposeRequest>` naming `sources` in order.
fn compose_body(sources: &[&object_store::path::Path]) -> String {
    let mut body = String::from("<ComposeRequest>");
    for source in sources {
        body.push_str("<Component><Name>");
        body.push_str(&xml_escape(source.as_ref()));
        body.push_str("</Name></Component>");
    }
    body.push_str("</ComposeRequest>");
    body
}

fn xml_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for char in text.chars() {
        match char {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn header<'a>(headers: &'a http::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn failure(
    step: &str,
    key: &object_store::path::Path,
    status: http::StatusCode,
    body: &[u8],
) -> UniversalIoError {
    let excerpt: String = String::from_utf8_lossy(body).chars().take(512).collect();
    UniversalIoError::s3(std::io::Error::other(format!(
        "{step} for {key} failed with status {status}: {excerpt}",
    )))
}

#[cfg(test)]
mod tests;
