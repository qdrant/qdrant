//! Backend abstraction: a concrete [`ObjectStore`] type plus a config that can
//! produce it. Implemented for AWS S3, GCS, and Azure (see [`crate::backends`])
//! so the [`AsyncRead`](crate::AsyncRead) impl on `Arc<S>` stays free of `dyn`.

use common::universal_io::{Result, UniversalKind};
use object_store::ObjectStore;

use crate::append::AppendContext;

/// A concrete object-store backend that can be built from a typed [`Config`].
///
/// Each impl provides:
/// - the [`Config`] type the user supplies,
/// - a [`build_store`] function turning that config into an owned `Self`,
/// - the [`UniversalKind`] tag used by the universal IO layer.
///
/// [`Config`]: BlobBackend::Config
/// [`build_store`]: BlobBackend::build_store
pub trait BlobBackend: ObjectStore + Send + Sync + Sized + 'static {
    type Config: Clone + Send + Sync + 'static;

    fn build_store(config: &Self::Config) -> Result<Self>;

    fn kind() -> UniversalKind;

    /// Context for the native single-request append RPC, for backends that
    /// support it (see [`crate::append`]). The default — no append support —
    /// leaves [`ObjectStoreSource`](crate::ObjectStoreSource) without its
    /// `AsyncAppend` prerequisites for this backend.
    fn append_context(_config: &Self::Config) -> Result<Option<AppendContext>> {
        Ok(None)
    }
}
