//! Appends for object stores, powering the `AsyncAppend` impls on
//! [`ObjectStoreSource`]. The configured store's [`AppendContext`] selects
//! the strategy object: a native single-request write-offset append
//! ([`NativeAppend`]), a whole-object multipart rewrite with server-side
//! prefix copies ([`PartCopyAppend`]) for S3 stores without native append,
//! or a server-side `compose` concatenation ([`ComposeAppend`]) on GCS.

mod context;

use bytes::Bytes;
use common::universal_io::{UioResult, UniversalIoError};
pub use context::{
    AppendContext, ComposeAppend, NativeAppend, PartCopyAppend, SignedRequestContext,
};
use io_bridge::{AppendSupport, AsyncAppend};
use object_store::aws::AmazonS3;
use object_store::gcp::GoogleCloudStorage;

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
                    match native.append(&store, &key, offset, data.clone()).await {
                        // The object hit the store's appended-block cap;
                        // rebuilding it as a single blob via the part-copy
                        // strategy resets the cap.
                        Err(err) if err.is_append_rewrite_required() => {
                            native.part_copy().append(&store, &key, offset, data).await
                        }
                        result => result,
                    }
                }
                // A direct append on a part-copy store IS the rewrite;
                // the caller respects the advertised threshold.
                AppendContext::PartCopy(part_copy) => {
                    part_copy.append(&store, &key, offset, data).await
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
}
