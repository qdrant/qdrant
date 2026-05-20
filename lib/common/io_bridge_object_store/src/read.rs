use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use bytes::Bytes;
use common::universal_io::{Result, UniversalKind};

use crate::file::BlobFile;
use crate::runtime::BridgeRuntime;

/// A read-capable blob backend (S3, GCS, …). One implementation per backend.
///
/// - Static methods (`open`, `list_files`, `exists`) are the backend's entry
///   points. They take an optional [`BridgeRuntime`] — `None` falls back to
///   [`BridgeRuntime::global`] — and a backend-specific [`Config`](BlobRead::Config).
/// - Per-instance methods (`read_range`, `len`, `is_empty`) describe a single
///   opened object. The returned future from `read_range` is `'static`, so it
///   can be shipped through the bridge runtime's worker channel.
///
/// A future `BlobWrite` trait will sit next to this one and a backend type
/// (e.g. `S3Source`) impls both, with `BlobFile<A>` exposing the sync `UniversalRead` view.
pub trait BlobRead: Send + Sync + Sized + 'static {
    type Config;

    fn open(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        key: &Path,
    ) -> Result<BlobFile<Self>>;

    fn list_files(
        runtime: Option<BridgeRuntime>,
        config: &Self::Config,
        prefix: &Path,
    ) -> Result<Vec<PathBuf>>;

    fn exists(runtime: Option<BridgeRuntime>, config: &Self::Config, path: &Path) -> Result<bool>;

    fn read_range(
        &self,
        range: Range<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + 'static>>;

    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn kind() -> UniversalKind;
}

pub(crate) fn resolve_runtime(runtime: Option<BridgeRuntime>) -> BridgeRuntime {
    runtime.unwrap_or_else(BridgeRuntime::global)
}
