//! Appends for object stores, powering the `AsyncAppend` impls on
//! [`ObjectStoreSource`]. The configured store's [`AppendContext`] selects
//! the strategy object: a native single-request write-offset append
//! ([`NativeAppend`]), a whole-object multipart rewrite with server-side
//! prefix copies ([`PartCopyAppend`]) for S3 stores without native append,
//! or a server-side `compose` concatenation ([`ComposeAppend`]) on GCS.

mod context;

use common::universal_io::{UioResult, UniversalIoError};
pub use context::{
    AppendContext, ComposeAppend, NativeAppend, PartCopyAppend, SignedRequestContext,
};
use io_bridge::{AppendRequest, AppendSupport, AsyncAppend};
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
        request: AppendRequest,
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

            match request {
                AppendRequest::Append { offset, data } => match context {
                    AppendContext::Native(native) => {
                        native.append(&store, &key, offset, data).await
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
                },
                AppendRequest::Rewrite { offset, data } => match context {
                    AppendContext::PartCopy(part_copy) => {
                        part_copy.append(&store, &key, offset, data).await
                    }
                    // Native stores rewrite too — compacting an object that
                    // reached the appended-block cap — via the part-copy
                    // strategy over the same store.
                    AppendContext::Native(native) => {
                        native.part_copy().append(&store, &key, offset, data).await
                    }
                    AppendContext::Compose(_) => Err(UniversalIoError::S3Config {
                        description: "compose append context belongs to a GCS store, \
                                      not an S3 one"
                            .to_string(),
                    }),
                },
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
        request: AppendRequest,
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

            // Both request shapes are the same operation under compose:
            // `[0, offset)` is the existing object either way.
            let (AppendRequest::Append { offset, data } | AppendRequest::Rewrite { offset, data }) =
                request;

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
