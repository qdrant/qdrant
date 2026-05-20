use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::universal_io::{Result, UniversalKind};

use crate::file::BlobFile;
use crate::runtime::BridgeRuntime;

/// Read-capable blob backend (S3, GCS, …). One impl per backend.
///
/// - Static methods (`open`, `list_files`, `exists`) are the backend's entry
///   points. They take an optional [`BridgeRuntime`] — `None` falls back to
///   [`BridgeRuntime::global`] — and a backend-specific [`Config`](AsyncRead::Config).
/// - Per-instance methods (`read_range`, `len`, `is_empty`) describe a single
///   opened object. The future returned by `read_range` is `'static` so it can
///   be shipped through the runtime worker's MPSC channel after being boxed at
///   the channel boundary.
///
/// A future `AsyncWrite` trait will live next to this one in `write.rs`.
pub trait AsyncRead: Send + Sync + Sized + 'static {
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

    fn read_range(&self, range: Range<u64>)
    -> impl Future<Output = Result<Bytes>> + Send + 'static;

    fn len(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn kind() -> UniversalKind;
}

pub(crate) fn resolve_runtime(runtime: Option<BridgeRuntime>) -> BridgeRuntime {
    runtime.unwrap_or_else(BridgeRuntime::global)
}
