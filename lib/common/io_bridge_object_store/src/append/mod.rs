//! Appends for object stores, powering the `AsyncAppend` impls on
//! [`ObjectStoreSource`]. The configured store's [`AppendContext`] selects
//! the strategy object: a native single-request write-offset append
//! ([`NativeAppend`]), a whole-object multipart rewrite with server-side
//! prefix copies ([`PartCopyAppend`]) for S3 stores without native append,
//! or a server-side `compose` concatenation ([`ComposeAppend`]) on GCS.

mod context;

use std::path::PathBuf;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
pub use context::{
    AppendContext, ComposeAppend, NativeAppend, PartCopyAppend, SignedRequestContext,
};
use io_bridge::{AppendSupport, AsyncAppend};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;
use object_store::{ObjectStoreExt as _, PutPayload};

use crate::source::ObjectStoreSource;

impl AsyncAppend for ObjectStoreSource<AmazonS3> {
    fn append_support(&self) -> AppendSupport {
        match self.append_context() {
            Some(AppendContext::Native(_)) => AppendSupport::Always,
            // Part-copy appends carry the copied prefix as non-last
            // multipart parts, which S3 caps from below.
            Some(AppendContext::PartCopy(_)) => AppendSupport::AboveThreshold {
                min_offset: context::part_copy::MIN_DIRECT_OFFSET,
            },
            // A compose context on an S3 store is a config error; `Never`
            // keeps the caller from ever exercising it.
            Some(AppendContext::Compose(_)) | None => AppendSupport::Never,
        }
    }

    fn append(
        &self,
        path: &std::path::Path,
        offset: u64,
        data: Bytes,
        expected_etag: Option<String>,
    ) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        let store = self.store().clone();
        let context = self.append_context().cloned();
        let key = crate::source::build_key(path);

        async move {
            let Some(context) = context else {
                return Err(UniversalIoError::S3Config {
                    description: "append is not supported for this S3 backend/config \
                                  (append context missing)"
                        .to_string(),
                });
            };

            match context {
                AppendContext::Native(native) => {
                    // The write-offset PUT has no etag precondition;
                    // `expected_etag` applies from the rewrite fallbacks on.
                    match native.append(&store, &key, offset, data.clone()).await {
                        // The object hit the store's appended-block cap;
                        // rebuilding it as a single blob resets the cap.
                        Err(err) if err.is_append_rewrite_required() => {
                            let rewrite = native
                                .part_copy()
                                .append(
                                    &store,
                                    &key,
                                    offset,
                                    data.clone(),
                                    expected_etag.as_deref(),
                                )
                                .await;
                            match rewrite {
                                // The store also rejected the server-side
                                // rebuild: the prefix is below its minimum
                                // multipart part size. Download it and
                                // rewrite the whole object.
                                Err(err) if err.is_append_entity_too_small() => {
                                    download_rewrite(
                                        &store,
                                        &key,
                                        offset,
                                        data,
                                        expected_etag.as_deref(),
                                    )
                                    .await
                                }
                                result => result,
                            }
                        }
                        result => result,
                    }
                }
                // A direct append on a part-copy store IS the rewrite;
                // the caller respects the advertised threshold.
                AppendContext::PartCopy(part_copy) => {
                    part_copy
                        .append(&store, &key, offset, data, expected_etag.as_deref())
                        .await
                }
                AppendContext::Compose(_) => Err(UniversalIoError::S3Config {
                    description: "compose append context belongs to a GCS store, \
                                  not an S3 one"
                        .to_string(),
                }),
            }
        }
    }
}

/// Last-resort appended-block cap recovery, for when the store rejects even
/// the server-side rebuild because the object is below its minimum multipart
/// part size: download the existing prefix and atomically PUT the whole
/// object back as `[0, offset) + data`. That same rejection bounds the
/// download — the object is smaller than one minimum part (5 MiB on AWS).
///
/// `expected_etag` is checked against the downloaded prefix's own etag —
/// the GET carries it, so the guard costs no extra request here.
async fn download_rewrite(
    store: &AmazonS3,
    key: &object_store::path::Path,
    offset: u64,
    data: Bytes,
    expected_etag: Option<&str>,
) -> UioResult<u64> {
    let result = store.get(key).await.map_err(UniversalIoError::s3)?;

    if let Some(expected) = expected_etag
        && result.meta.e_tag.as_deref() != Some(expected)
    {
        return Err(UniversalIoError::AppendEtagMismatch {
            path: PathBuf::from(key.to_string()),
        });
    }

    let existing = result.bytes().await.map_err(UniversalIoError::s3)?;

    // The same compare-and-swap token as a direct append: a prefix of a
    // different length means `offset` is not the current object size.
    if existing.len() as u64 != offset {
        return Err(UniversalIoError::AppendOffsetConflict {
            path: PathBuf::from(key.to_string()),
            offset,
        });
    }

    let mut whole = Vec::with_capacity(existing.len() + data.len());
    whole.extend_from_slice(&existing);
    whole.extend_from_slice(&data);
    let new_len = whole.len() as u64;

    store
        .put(key, PutPayload::from(whole))
        .await
        .map_err(UniversalIoError::s3)?;

    Ok(new_len)
}

impl AsyncAppend for ObjectStoreSource<GoogleCloudStorage> {
    fn append_support(&self) -> AppendSupport {
        // Compose appends at any size: no part-size minimums, no
        // appended-block cap — callers never need a fallback.
        AppendSupport::Always
    }

    fn append(
        &self,
        path: &std::path::Path,
        offset: u64,
        data: Bytes,
        // GCS compose preconditions are generation-based, not etag-based,
        // so the expected etag cannot be honored here.
        _expected_etag: Option<String>,
    ) -> impl Future<Output = UioResult<u64>> + Send + 'static {
        let store = self.store().clone();
        let context = self.append_context().cloned();
        let key = crate::source::build_key(path);

        async move {
            let Some(context) = context else {
                return Err(UniversalIoError::S3Config {
                    description: "append is not supported for this GCS backend/config \
                                  (append context missing)"
                        .to_string(),
                });
            };

            match context {
                AppendContext::Compose(compose) => compose.append(&store, &key, offset, data).await,
                AppendContext::Native(_) | AppendContext::PartCopy(_) => {
                    Err(UniversalIoError::S3Config {
                        description: "S3 append context on a GCS store".to_string(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use context::stub::{StubResponse, stub_server, stub_store_and_context};

    use super::*;
    use crate::source::ObjectStoreSource;

    /// An append on a native store that hits the appended-block cap falls
    /// back to the part-copy rewrite within the same `append` call.
    #[test]
    fn native_cap_hit_falls_back_to_part_copy_rewrite() {
        let (endpoint, seen) = stub_server(vec![
            // Native write-offset PUT: rejected with the appended-block cap.
            StubResponse::new(400).body("<Error><Code>TooManyParts</Code></Error>"),
            // The part-copy rewrite: initiate, prefix copy, data part, complete.
            StubResponse::new(200).body(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>\
                 </InitiateMultipartUploadResult>",
            ),
            StubResponse::new(200)
                .body("<CopyPartResult><ETag>\"etag-copy\"</ETag></CopyPartResult>"),
            StubResponse::new(200).header("etag", "\"etag-data\""),
            StubResponse::new(200).body("<CompleteMultipartUploadResult/>"),
        ]);
        let (store, context) = stub_store_and_context(&endpoint);
        let source = ObjectStoreSource::new(store)
            .with_append_context(AppendContext::Native(NativeAppend::new(context)));

        let offset: u64 = 10 * 1024 * 1024;
        let new_len = io_bridge::BridgeRuntime::global()
            .block_on(source.append(
                Path::new("dir/append.dat"),
                offset,
                Bytes::from_static(b"data"),
                None,
            ))
            .unwrap();
        assert_eq!(new_len, offset + 4);

        let seen = seen.lock().unwrap();
        let methods: Vec<&str> = seen.iter().map(|request| request.method.as_str()).collect();
        assert_eq!(methods, ["PUT", "POST", "PUT", "PUT", "POST"]);
        assert_eq!(seen[0].write_offset.as_deref(), Some(&*offset.to_string()));
        assert_eq!(
            seen[2].copy_source.as_deref(),
            Some("/bucket/dir/append.dat")
        );
    }

    /// The responses of a part-copy rewrite attempt that the store rejects
    /// at the complete step because the object is below its minimum part
    /// size — followed by the abort of the orphaned multipart upload.
    fn rejected_part_copy_attempt() -> Vec<StubResponse> {
        vec![
            // Native write-offset PUT: rejected with the appended-block cap.
            StubResponse::new(400).body("<Error><Code>TooManyParts</Code></Error>"),
            // The part-copy rewrite attempt: initiate, prefix copy, data
            // part — then the complete is rejected and the upload aborted.
            StubResponse::new(200).body(
                "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>\
                 </InitiateMultipartUploadResult>",
            ),
            StubResponse::new(200)
                .body("<CopyPartResult><ETag>\"etag-copy\"</ETag></CopyPartResult>"),
            StubResponse::new(200).header("etag", "\"etag-data\""),
            StubResponse::new(400).body("<Error><Code>EntityTooSmall</Code></Error>"),
            StubResponse::new(204),
        ]
    }

    /// A capped object the store also refuses to rewrite server-side
    /// (`EntityTooSmall`) is recovered by downloading the prefix and
    /// PUTting the whole object back.
    #[test]
    fn entity_too_small_rewrite_downloads_and_puts_the_whole_object() {
        let mut responses = rejected_part_copy_attempt();
        responses.extend([
            // Download of the existing 5-byte prefix.
            StubResponse::new(200)
                .header("last-modified", "Tue, 14 Jul 2026 12:00:00 GMT")
                .header("etag", "\"stub\"")
                .body("abcde"),
            // Whole-object PUT of prefix + appended data.
            StubResponse::new(200).header("etag", "\"stub-2\""),
        ]);
        let (endpoint, seen) = stub_server(responses);
        let (store, context) = stub_store_and_context(&endpoint);
        let source = ObjectStoreSource::new(store)
            .with_append_context(AppendContext::Native(NativeAppend::new(context)));

        let new_len = io_bridge::BridgeRuntime::global()
            .block_on(source.append(
                Path::new("dir/append.dat"),
                5,
                Bytes::from_static(b"data"),
                None,
            ))
            .unwrap();
        assert_eq!(new_len, 9);

        let seen = seen.lock().unwrap();
        let methods: Vec<&str> = seen.iter().map(|request| request.method.as_str()).collect();
        assert_eq!(
            methods,
            ["PUT", "POST", "PUT", "PUT", "POST", "DELETE", "GET", "PUT"]
        );
        assert_eq!(seen[7].body, b"abcdedata");
        assert!(seen[7].write_offset.is_none());
    }

    /// The download-rewrite must not rebuild the object from a stale view:
    /// a downloaded prefix whose length disagrees with `offset` is an
    /// offset conflict, and no PUT is issued.
    #[test]
    fn download_rewrite_with_a_stale_offset_is_a_conflict() {
        let mut responses = rejected_part_copy_attempt();
        responses.extend([
            // The object is 6 bytes, not the expected 5.
            StubResponse::new(200)
                .header("last-modified", "Tue, 14 Jul 2026 12:00:00 GMT")
                .header("etag", "\"stub\"")
                .body("abcdef"),
        ]);
        let (endpoint, seen) = stub_server(responses);
        let (store, context) = stub_store_and_context(&endpoint);
        let source = ObjectStoreSource::new(store)
            .with_append_context(AppendContext::Native(NativeAppend::new(context)));

        let err = io_bridge::BridgeRuntime::global()
            .block_on(source.append(
                Path::new("dir/append.dat"),
                5,
                Bytes::from_static(b"data"),
                None,
            ))
            .unwrap_err();
        assert!(matches!(
            err,
            UniversalIoError::AppendOffsetConflict { offset: 5, .. }
        ));
        assert_eq!(seen.lock().unwrap().len(), 7);
    }
}
