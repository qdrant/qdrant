//! The append strategies: one object per strategy, each implementing its
//! own append logic, combined by [`AppendContext`].

mod compose;
mod native;
pub(super) mod part_copy;
mod signed;
#[cfg(test)]
mod stub;

pub use compose::ComposeAppend;
pub use native::NativeAppend;
pub use part_copy::PartCopyAppend;
pub use signed::SignedRequestContext;

/// The append strategy of the configured store: one object per strategy,
/// each implementing its own append logic. The backend picks the variant
/// from its config (see [`BlobBackend::append_context`]).
///
/// [`BlobBackend::append_context`]: crate::BlobBackend::append_context
#[derive(Debug, Clone)]
pub enum AppendContext {
    /// The store honors native single-request write-offset appends
    /// (`PutObject` + `x-amz-write-offset-bytes`): S3 Express One Zone,
    /// MinIO AiStor.
    Native(NativeAppend),
    /// Plain S3: appends land as whole-object multipart rewrites whose
    /// prefix parts are server-side `UploadPartCopy` requests.
    PartCopy(PartCopyAppend),
    /// GCS: appends land as server-side `compose` requests — the new data
    /// uploaded as a temporary object, then composed onto the existing one.
    Compose(ComposeAppend),
}
