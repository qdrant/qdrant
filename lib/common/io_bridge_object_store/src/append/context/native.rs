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
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
use object_store::ObjectStoreExt as _;
use object_store::aws::{AmazonS3, AwsAuthorizer};
use object_store::client::HttpRequestBody;

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

/// Total attempts for one append: transient failures (connection errors,
/// 5xx, 429) are retried with a short linear backoff, like `object_store`
/// does for its own requests. Retrying is safe: the write offset acts as a
/// compare-and-swap, and a conflict caused by a lost-acknowledgement
/// attempt is reconciled in [`append_request`].
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
        AwsAuthorizer::new(&credential, context.service, &context.region)
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
mod tests {
    use std::assert_matches;

    use object_store::aws::AmazonS3Builder;
    use url::Url;

    use super::super::stub::{StubResponse, stub_server, stub_store_and_context};
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
        let context = SignedRequestContext::new(
            true,
            "bucket".to_string(),
            Url::parse("http://localhost:9000/bucket").unwrap(),
            "us-east-1".to_string(),
            "s3",
        );
        // `url` accepts this; `http` caps URIs at u16::MAX bytes.
        let key = object_store::path::Path::from("k".repeat(70_000));

        let result = io_bridge::BridgeRuntime::global().block_on(
            NativeAppend::new(context).append(&store, &key, 0, Bytes::from_static(b"data")),
        );
        assert!(matches!(result, Err(UniversalIoError::S3Config { .. })));
    }

    /// Append `b"data"` at `offset` to `dir/append.dat` via the stub, so a
    /// consistent store would report a new object size of `offset + 4`.
    fn append_data_at(endpoint: &str, offset: u64) -> UioResult<u64> {
        let (store, context) = stub_store_and_context(endpoint);
        let key = object_store::path::Path::from("dir/append.dat");

        io_bridge::BridgeRuntime::global().block_on(NativeAppend::new(context).append(
            &store,
            &key,
            offset,
            Bytes::from_static(b"data"),
        ))
    }

    fn success_with_size(size: u64) -> StubResponse {
        StubResponse::new(200).header(OBJECT_SIZE_HEADER, size)
    }

    fn write_offset_conflict() -> StubResponse {
        StubResponse::new(400).body("<Error><Code>InvalidWriteOffset</Code></Error>")
    }

    /// A `head()` response; object metadata is parsed from the headers.
    fn head_with_size(size: u64) -> StubResponse {
        StubResponse::new(200)
            .header("content-length", size)
            .header("last-modified", "Tue, 14 Jul 2026 12:00:00 GMT")
            .header("etag", "\"stub\"")
    }

    #[test]
    fn append_sends_a_signed_write_offset_put_and_returns_the_new_size() {
        let (endpoint, seen) = stub_server(vec![success_with_size(9)]);
        assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

        let seen = seen.lock().unwrap();
        let [request] = &seen[..] else {
            panic!("expected exactly one request");
        };
        assert_eq!(request.method, "PUT");
        assert_eq!(request.path, "/bucket/dir/append.dat");
        assert_eq!(request.write_offset.as_deref(), Some("5"));
        assert!(request.signed);
        assert_eq!(request.body, b"data");
    }

    /// A success whose size header disagrees with `offset + data` means the
    /// write did not land as the single append we requested; the reported
    /// size must not be trusted.
    #[test]
    fn success_with_a_mismatching_size_is_an_error() {
        let (endpoint, _seen) = stub_server(vec![success_with_size(42)]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        assert!(err.to_string().contains("expected 9"), "{err}");
    }

    #[test]
    fn success_with_an_unparseable_size_is_an_error() {
        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(200).header(OBJECT_SIZE_HEADER, "over 9000"),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
    }

    /// At offset 0 an append equals a whole-object write, so a store that
    /// accepted the PUT without the size header still produced the right
    /// object.
    #[test]
    fn success_without_the_size_header_at_offset_zero_is_a_whole_object_write() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(200)]);

        assert_eq!(append_data_at(&endpoint, 0).unwrap(), 4);
    }

    /// Past offset 0 the missing size header is the replaced-not-appended
    /// signature of a store without write-offset support.
    #[test]
    fn success_without_the_size_header_at_a_nonzero_offset_is_an_error() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(200)]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        assert!(err.to_string().contains(OBJECT_SIZE_HEADER), "{err}");
    }

    /// A first-attempt conflict is returned as-is: no reconciliation
    /// `head()` is issued, since no earlier attempt of ours can have landed.
    #[test]
    fn invalid_write_offset_is_a_conflict() {
        let (endpoint, seen) = stub_server(vec![write_offset_conflict()]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// Some S3-compatibles report the offset mismatch as 412 instead of
    /// AWS's 400 `InvalidWriteOffset`.
    #[test]
    fn precondition_failed_is_a_conflict() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(412)]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );
    }

    /// The appended-block cap surfaces as `AppendRewriteRequired`, so the
    /// caller recovers with a whole-object rewrite.
    #[test]
    fn too_many_parts_is_rewrite_required() {
        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(400).body("<Error><Code>TooManyParts</Code></Error>"),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::AppendRewriteRequired { .. });
    }

    /// Only the `InvalidWriteOffset` error code makes a 400 a conflict;
    /// other bad requests must not masquerade as recoverable.
    #[test]
    fn bad_request_without_the_conflict_code_is_not_a_conflict() {
        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(400).body("<Code>MissingHeader</Code>"),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
    }

    /// A missing object under a nonzero offset is a stale view of the
    /// object (deleted behind our back): the same recovery as an offset
    /// mismatch. Bodiless 404s (and explicit NoSuchKey) both qualify.
    #[test]
    fn not_found_at_a_nonzero_offset_is_a_conflict() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(404)]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );

        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(404).body("<Error><Code>NoSuchKey</Code></Error>"),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );
    }

    /// A 404 whose body names a missing *bucket* is a config/endpoint
    /// problem (e.g. a directory bucket addressed through the standard
    /// endpoint): it must surface loudly instead of masquerading as a
    /// recoverable offset conflict or a missing file.
    #[test]
    fn not_found_for_a_missing_bucket_is_a_hard_error() {
        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(404).body("<Error><Code>NoSuchBucket</Code></Error>"),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        assert!(err.to_string().contains("NoSuchBucket"), "{err}");
    }

    /// At offset 0 a 404 is a genuine missing target (e.g. missing bucket).
    #[test]
    fn not_found_at_offset_zero_is_not_found() {
        let (endpoint, _seen) = stub_server(vec![StubResponse::new(404)]);

        let err = append_data_at(&endpoint, 0).unwrap_err();
        let UniversalIoError::NotFound { path } = err else {
            panic!("expected NotFound, got {err:?}");
        };
        assert_eq!(path, PathBuf::from("dir/append.dat"));
    }

    /// Transient failures are retried at the same offset — the offset acts
    /// as a compare-and-swap, so the retry cannot double-append.
    #[test]
    fn transient_failures_are_retried_at_the_same_offset() {
        let (endpoint, seen) = stub_server(vec![StubResponse::new(429), success_with_size(9)]);

        assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            seen.iter()
                .all(|request| request.method == "PUT"
                    && request.write_offset.as_deref() == Some("5")),
        );
    }

    #[test]
    fn persistent_server_errors_fail_after_max_attempts() {
        let responses = (0..MAX_ATTEMPTS).map(|_| StubResponse::new(503)).collect();
        let (endpoint, seen) = stub_server(responses);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        assert!(err.to_string().contains("503"), "{err}");
        assert_eq!(seen.lock().unwrap().len(), MAX_ATTEMPTS as usize);
    }

    /// Lost acknowledgement: the first attempt landed but its response was
    /// lost, so the retry conflicts. A `head()` showing the object ends
    /// exactly at `offset + data` proves the tail is ours (single-writer
    /// contract) and the append reports success.
    #[test]
    fn retried_conflict_reconciles_via_head_when_the_tail_landed() {
        let (endpoint, seen) = stub_server(vec![
            StubResponse::new(503),
            write_offset_conflict(),
            head_with_size(9),
        ]);

        assert_eq!(append_data_at(&endpoint, 5).unwrap(), 9);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[2].method, "HEAD");
    }

    /// The reconciliation only accepts the exact expected size: any other
    /// length means the conflict is real (someone else grew the object).
    #[test]
    fn retried_conflict_with_a_foreign_head_size_stays_a_conflict() {
        let (endpoint, _seen) = stub_server(vec![
            StubResponse::new(503),
            write_offset_conflict(),
            head_with_size(7),
        ]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        );
    }

    /// Unexpected failure statuses surface the status and an excerpt of the
    /// response body for diagnosis.
    #[test]
    fn failure_status_surfaces_the_body_excerpt() {
        let (endpoint, _seen) =
            stub_server(vec![StubResponse::new(403).body("AccessDenied by stub")]);

        let err = append_data_at(&endpoint, 5).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        let message = err.to_string();
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("AccessDenied by stub"), "{message}");
    }
}
