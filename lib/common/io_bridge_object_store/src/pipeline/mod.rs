//! Read pipelines bridging the sync universal-IO pipeline traits to the async
//! object-store backends.
//!
//! - [`buffer`]: the `Send` raw-pointer primitive and the shared read-future
//!   builder that streams a backend read straight into a destination `Vec<T>`.
//! - [`inner`]: the schedule/wait engine ([`PipelineInner`](inner::PipelineInner))
//!   shared by both public pipelines.
//! - [`borrowed`] / [`owned`]: the [`BorrowedBlobPipeline`] /
//!   [`OwnedBlobPipeline`] trait impls over a [`BlobFile`](crate::BlobFile).

mod borrowed;
mod buffer;
mod inner;
mod owned;

pub use borrowed::BorrowedBlobPipeline;
pub(crate) use buffer::read_into_byte_buffer;
pub use owned::OwnedBlobPipeline;

pub(crate) const BLOB_PIPELINE_CAPACITY: usize = 256;
