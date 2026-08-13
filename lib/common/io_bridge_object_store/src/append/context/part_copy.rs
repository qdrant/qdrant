//! Whole-object rewrite for stores without native append: a multipart
//! upload whose prefix parts are server-side `UploadPartCopy` requests —
//! nothing but the appended data crosses the network — completed as a
//! single atomic replace.
//!
//! Like the native append, this issues the requests itself: the
//! `object_store` crate keeps provider-specific operations such as
//! `UploadPartCopy` out of its portable surface (its internal part-copy is
//! crate-private and range-less).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
use object_store::aws::{AmazonS3, AwsAuthorizer, AwsCredential};
use object_store::client::{HttpClient, HttpRequestBody};
use url::Url;

use super::extract_xml_tag;
use super::signed::SignedRequestContext;

/// Max bytes per `UploadPartCopy` part: the S3 5 GiB part-size ceiling.
const MAX_COPY_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// S3 error code returned when a non-last multipart part is below the
/// store's minimum part size (5 MiB on AWS): the rewrite is impossible
/// server-side, the caller must rebuild the object by uploading it whole.
const ENTITY_TOO_SMALL_CODE: &str = "EntityTooSmall";

/// Request header making an `UploadPartCopy` conditional on the source
/// object's entity tag; a mismatch fails the copy with 412.
const COPY_SOURCE_IF_MATCH: &str = "x-amz-copy-source-if-match";

/// Minimum existing-object size this strategy can append to: the copied
/// prefix lands as non-last multipart parts, which S3 requires to be at
/// least 5 MiB. Below this, callers must write the whole object themselves.
pub(crate) const MIN_DIRECT_OFFSET: u64 = 5 * 1024 * 1024;

/// One signed S3 request against a fixed object URL. Returns the response
/// headers and body text; non-2xx statuses become errors carrying `step`
/// and a body excerpt, with [`ENTITY_TOO_SMALL_CODE`] mapped to the typed
/// [`UniversalIoError::AppendEntityTooSmall`].
struct SignedObjectClient<'a> {
    client: HttpClient,
    credential: Arc<AwsCredential>,
    region: &'a str,
    /// SigV4 service name, from [`SignedRequestContext::service`].
    service: &'static str,
    url: Url,
    /// The object's path, for error reporting.
    path: PathBuf,
}

impl SignedObjectClient<'_> {
    async fn request(
        &self,
        method: http::Method,
        query: &str,
        headers: &[(&str, &str)],
        body: HttpRequestBody,
        step: &str,
    ) -> UioResult<(http::HeaderMap, String)> {
        let mut url = self.url.clone();
        url.set_query(Some(query));

        let mut builder = http::Request::builder().method(method).uri(url.as_str());
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let mut request = builder
            .body(body)
            .map_err(|err| UniversalIoError::S3Config {
                description: format!("{step} request: {err}"),
            })?;

        AwsAuthorizer::new(&self.credential, self.service, self.region)
            .try_authorize(&mut request, None)
            .map_err(UniversalIoError::s3)?;

        let response = self
            .client
            .execute(request)
            .await
            .map_err(UniversalIoError::s3)?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let body = response.into_body().bytes().await.unwrap_or_default();
        let body_text = String::from_utf8_lossy(&body).into_owned();

        if !status.is_success() {
            if extract_xml_tag(&body_text, "Code") == Some(ENTITY_TOO_SMALL_CODE) {
                return Err(UniversalIoError::AppendEntityTooSmall {
                    path: self.path.clone(),
                });
            }
            // The only precondition these requests carry is
            // [`COPY_SOURCE_IF_MATCH`], so a 412 is an etag mismatch.
            if status == http::StatusCode::PRECONDITION_FAILED {
                return Err(UniversalIoError::AppendEtagMismatch {
                    path: self.path.clone(),
                });
            }
            let excerpt: String = body_text.chars().take(512).collect();
            return Err(UniversalIoError::s3(std::io::Error::other(format!(
                "{step} failed with status {status}: {excerpt}",
            ))));
        }
        Ok((response_headers, body_text))
    }
}

/// The part-copy append strategy for stores without native append: the
/// object is rewritten as its existing prefix (server-side `UploadPartCopy`)
/// plus the appended data, in one atomic multipart replace.
#[derive(Debug, Clone)]
pub struct PartCopyAppend {
    signed: SignedRequestContext,
}

impl PartCopyAppend {
    pub fn new(signed: SignedRequestContext) -> Self {
        Self { signed }
    }

    /// Append `data` at `offset` (== the current object size) by rewriting
    /// the whole object. Returns the new total object size.
    ///
    /// `expected_etag` is attached to the prefix copies as
    /// `x-amz-copy-source-if-match`, so the store itself rejects the
    /// rewrite (as [`UniversalIoError::AppendEtagMismatch`]) if the object
    /// no longer carries the entity tag the caller last observed.
    pub(in crate::append) async fn append(
        &self,
        store: &Arc<AmazonS3>,
        key: &object_store::path::Path,
        offset: u64,
        data: Bytes,
        expected_etag: Option<&str>,
    ) -> UioResult<u64> {
        multipart_rewrite(store, &self.signed, key, offset, data, expected_etag).await
    }
}

/// Rewrite the object at `key` as its existing `[0, offset)` prefix plus
/// `data`: `CreateMultipartUpload`, server-side `UploadPartCopy` of the
/// prefix, `data` as the final part, `CompleteMultipartUpload` (an atomic
/// replace). Returns the new total object size.
///
/// Without `expected_etag` there is no compare-and-swap here — S3 evaluates
/// the copy source when the part copy runs — so the caller must hold the
/// single-writer contract (`CachedBlobFile` validates `offset` against its
/// mirror length first).
async fn multipart_rewrite(
    store: &Arc<AmazonS3>,
    context: &SignedRequestContext,
    key: &object_store::path::Path,
    offset: u64,
    data: Bytes,
    expected_etag: Option<&str>,
) -> UioResult<u64> {
    let credential = store
        .credentials()
        .get_credential()
        .await
        .map_err(UniversalIoError::s3)?;
    let client = SignedObjectClient {
        client: context.client()?,
        credential,
        region: &context.region,
        service: context.service,
        url: context.object_url(key)?,
        path: PathBuf::from(key.to_string()),
    };

    let (_, body) = client
        .request(
            http::Method::POST,
            "uploads",
            &[],
            HttpRequestBody::from(Bytes::new()),
            "initiate multipart rewrite",
        )
        .await?;
    let upload_id = extract_xml_tag(&body, "UploadId")
        .ok_or_else(|| {
            UniversalIoError::s3(std::io::Error::other(format!(
                "initiate multipart rewrite for {key}: no UploadId in response",
            )))
        })?
        .to_string();

    let result = rewrite_parts(
        &client,
        context,
        key,
        offset,
        data,
        expected_etag,
        &upload_id,
    )
    .await;
    if result.is_err() {
        // Best effort: an orphaned multipart upload holds storage until it
        // is aborted (or reaped by a bucket lifecycle rule).
        let _ = client
            .request(
                http::Method::DELETE,
                &format!("uploadId={upload_id}"),
                &[],
                HttpRequestBody::from(Bytes::new()),
                "abort multipart rewrite",
            )
            .await;
    }
    result
}

/// The part copies and completion of [`multipart_rewrite`], separated so a
/// failure of any step aborts the multipart upload.
#[expect(clippy::too_many_arguments, reason = "internal helper")]
async fn rewrite_parts(
    client: &SignedObjectClient<'_>,
    context: &SignedRequestContext,
    key: &object_store::path::Path,
    offset: u64,
    data: Bytes,
    expected_etag: Option<&str>,
    upload_id: &str,
) -> UioResult<u64> {
    let mut etags = Vec::new();
    let copy_source = format!("/{}/{}", context.bucket, key);
    let part_len = offset.div_ceil(offset.div_ceil(MAX_COPY_PART_SIZE).max(1));
    let mut start = 0;
    while start < offset {
        let end = (start + part_len).min(offset);
        let part_number = etags.len() + 1;
        let range = format!("bytes={start}-{}", end - 1);
        let mut headers = vec![
            ("x-amz-copy-source", copy_source.as_str()),
            ("x-amz-copy-source-range", range.as_str()),
        ];
        // The store itself verifies the copied prefix still carries the
        // caller's entity tag: a genuine compare-and-swap, unlike the
        // unconditional rewrite. Rejected as 412 by `request`.
        if let Some(expected_etag) = expected_etag {
            headers.push((COPY_SOURCE_IF_MATCH, expected_etag));
        }
        let (_, body) = client
            .request(
                http::Method::PUT,
                &format!("partNumber={part_number}&uploadId={upload_id}"),
                &headers,
                HttpRequestBody::from(Bytes::new()),
                "multipart rewrite part copy",
            )
            .await?;
        // `UploadPartCopy` reports failures after 200 OK inside the body,
        // so a successful copy is recognized by its ETag.
        let etag = extract_xml_tag(&body, "ETag").ok_or_else(|| {
            let excerpt: String = body.chars().take(512).collect();
            UniversalIoError::s3(std::io::Error::other(format!(
                "multipart rewrite part copy for {key}: no ETag in response: {excerpt}",
            )))
        })?;
        etags.push(etag.to_string());
        start = end;
    }

    // The new data as the final part (no minimum size for the last part).
    let final_size = offset + data.len() as u64;
    let part_number = etags.len() + 1;
    let (headers, _) = client
        .request(
            http::Method::PUT,
            &format!("partNumber={part_number}&uploadId={upload_id}"),
            &[],
            HttpRequestBody::from(data),
            "multipart rewrite data part",
        )
        .await?;
    let etag = headers
        .get(http::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            UniversalIoError::s3(std::io::Error::other(format!(
                "multipart rewrite data part for {key}: no ETag response header",
            )))
        })?;
    etags.push(etag.to_string());

    let mut complete = String::from("<CompleteMultipartUpload>");
    for (index, etag) in etags.iter().enumerate() {
        complete += &format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{etag}</ETag></Part>",
            index + 1,
        );
    }
    complete += "</CompleteMultipartUpload>";

    let (_, body) = client
        .request(
            http::Method::POST,
            &format!("uploadId={upload_id}"),
            &[],
            HttpRequestBody::from(Bytes::from(complete)),
            "complete multipart rewrite",
        )
        .await?;
    // `CompleteMultipartUpload` also reports failures after 200 OK inside
    // the body.
    if body.contains("<Error") {
        if extract_xml_tag(&body, "Code") == Some(ENTITY_TOO_SMALL_CODE) {
            return Err(UniversalIoError::AppendEntityTooSmall {
                path: PathBuf::from(key.to_string()),
            });
        }
        let excerpt: String = body.chars().take(512).collect();
        return Err(UniversalIoError::s3(std::io::Error::other(format!(
            "complete multipart rewrite for {key} failed: {excerpt}",
        ))));
    }

    Ok(final_size)
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::super::stub::{StubResponse, stub_server, stub_store_and_context};
    use super::*;

    /// Multipart-rewrite `dir/append.dat` as its `[0, offset)` prefix plus
    /// `b"data"` via the stub.
    fn rewrite_data_at(endpoint: &str, offset: u64) -> UioResult<u64> {
        rewrite_data_at_if_match(endpoint, offset, None)
    }

    fn rewrite_data_at_if_match(
        endpoint: &str,
        offset: u64,
        expected_etag: Option<&str>,
    ) -> UioResult<u64> {
        let (store, context) = stub_store_and_context(endpoint);
        let key = object_store::path::Path::from("dir/append.dat");

        io_bridge::BridgeRuntime::global().block_on(PartCopyAppend::new(context).append(
            &store,
            &key,
            offset,
            Bytes::from_static(b"data"),
            expected_etag,
        ))
    }

    fn initiate_ok() -> StubResponse {
        StubResponse::new(200).body(
            "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>\
             </InitiateMultipartUploadResult>",
        )
    }

    fn copy_part_ok() -> StubResponse {
        StubResponse::new(200).body("<CopyPartResult><ETag>\"etag-copy\"</ETag></CopyPartResult>")
    }

    fn data_part_ok() -> StubResponse {
        StubResponse::new(200).header("etag", "\"etag-data\"")
    }

    fn complete_ok() -> StubResponse {
        StubResponse::new(200).body("<CompleteMultipartUploadResult/>")
    }

    #[test]
    fn multipart_rewrite_copies_the_prefix_and_completes() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            copy_part_ok(),
            data_part_ok(),
            complete_ok(),
        ]);

        let offset = 10 * 1024 * 1024;
        assert_eq!(rewrite_data_at(&endpoint, offset).unwrap(), offset + 4);

        let seen = seen.lock().unwrap();
        let [initiate, copy, upload, complete] = &seen[..] else {
            panic!("expected exactly four requests");
        };

        assert_eq!(initiate.method, "POST");
        assert_eq!(initiate.path, "/bucket/dir/append.dat?uploads");
        assert!(initiate.signed);

        assert_eq!(copy.method, "PUT");
        assert_eq!(
            copy.path,
            "/bucket/dir/append.dat?partNumber=1&uploadId=upload-1"
        );
        assert_eq!(copy.copy_source.as_deref(), Some("/bucket/dir/append.dat"));
        assert_eq!(copy.copy_range.as_deref(), Some("bytes=0-10485759"));

        assert_eq!(upload.method, "PUT");
        assert_eq!(
            upload.path,
            "/bucket/dir/append.dat?partNumber=2&uploadId=upload-1"
        );
        assert_eq!(upload.body, b"data");

        assert_eq!(complete.method, "POST");
        assert_eq!(complete.path, "/bucket/dir/append.dat?uploadId=upload-1");
        assert_eq!(
            String::from_utf8_lossy(&complete.body),
            "<CompleteMultipartUpload>\
             <Part><PartNumber>1</PartNumber><ETag>\"etag-copy\"</ETag></Part>\
             <Part><PartNumber>2</PartNumber><ETag>\"etag-data\"</ETag></Part>\
             </CompleteMultipartUpload>"
        );
    }

    /// A prefix over the 5 GiB part ceiling is copied in evenly-split
    /// parts, all within the ceiling.
    #[test]
    fn multipart_rewrite_splits_the_prefix_at_the_part_ceiling() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            copy_part_ok(),
            copy_part_ok(),
            data_part_ok(),
            complete_ok(),
        ]);

        let offset = MAX_COPY_PART_SIZE + 1;
        assert_eq!(rewrite_data_at(&endpoint, offset).unwrap(), offset + 4);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[1].copy_range.as_deref(), Some("bytes=0-2684354560"));
        assert_eq!(
            seen[2].copy_range.as_deref(),
            Some("bytes=2684354561-5368709120")
        );
        assert!(seen[3].path.contains("partNumber=3"), "{}", seen[3].path);
    }

    /// The expected etag rides the part copies as
    /// `x-amz-copy-source-if-match`; unconditional rewrites send no such
    /// header.
    #[test]
    fn expected_etag_is_sent_as_copy_source_if_match() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            copy_part_ok(),
            data_part_ok(),
            complete_ok(),
        ]);

        let offset = 10 * 1024 * 1024;
        rewrite_data_at_if_match(&endpoint, offset, Some("\"expected\"")).unwrap();

        let seen = seen.lock().unwrap();
        assert_eq!(seen[1].copy_if_match.as_deref(), Some("\"expected\""));
        // The data part carries no copy source, hence no precondition.
        assert_eq!(seen[2].copy_if_match, None);
    }

    /// A 412 on the conditional part copy is the store rejecting the etag
    /// precondition: the object changed behind the caller's back. The
    /// multipart upload is aborted like any other failure.
    #[test]
    fn copy_source_if_match_rejection_is_an_etag_mismatch() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            StubResponse::new(412),
            StubResponse::new(204),
        ]);

        let err =
            rewrite_data_at_if_match(&endpoint, 10 * 1024 * 1024, Some("\"stale\"")).unwrap_err();
        assert_matches!(err, UniversalIoError::AppendEtagMismatch { .. });
        assert_eq!(seen.lock().unwrap().last().unwrap().method, "DELETE");
    }

    /// A failing step aborts the multipart upload, so it does not linger
    /// holding storage for its uploaded parts.
    #[test]
    fn multipart_rewrite_aborts_on_failure() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            StubResponse::new(500),
            StubResponse::new(204),
        ]);

        let err = rewrite_data_at(&endpoint, 10 * 1024 * 1024).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[2].method, "DELETE");
        assert_eq!(seen[2].path, "/bucket/dir/append.dat?uploadId=upload-1");
    }

    /// `CompleteMultipartUpload` can fail after 200 OK, reporting the error
    /// in the body; the upload is aborted like any other failure.
    #[test]
    fn multipart_rewrite_complete_error_in_a_200_body_is_an_error() {
        let (endpoint, seen) = stub_server(vec![
            initiate_ok(),
            copy_part_ok(),
            data_part_ok(),
            StubResponse::new(200).body("<Error><Code>InternalError</Code></Error>"),
            StubResponse::new(204),
        ]);

        let err = rewrite_data_at(&endpoint, 10 * 1024 * 1024).unwrap_err();
        assert_matches!(err, UniversalIoError::S3(_));
        assert!(err.to_string().contains("InternalError"), "{err}");
        assert_eq!(seen.lock().unwrap().last().unwrap().method, "DELETE");
    }
}
