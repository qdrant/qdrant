use std::path::Path;

use bytes::Bytes;
use common::universal_io::{
    ListedFile, OpenOptions, UioResult, UniversalReadFileOps, UniversalReadFs,
    UniversalWriteFileOps,
};

use crate::{AsyncRead, AsyncWrite, BlobFile, BridgeRuntime};

/// Filesystem handle for an object-store backend: an [`AsyncRead`] handle plus
/// the [`BridgeRuntime`] used to drive its async operations. Opens per-object
/// [`BlobFile`] handles via [`UniversalReadFs::open`] and answers metadata
/// queries (`list_files`, `exists`) by blocking on the backend.
#[derive(Clone)]
pub struct BlobFs<A: AsyncRead> {
    inner: A,
    runtime: BridgeRuntime,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFs<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { runtime, inner: _ } = self;
        f.debug_struct("BlobFs")
            .field("runtime", runtime)
            .finish_non_exhaustive()
    }
}

impl<A: AsyncRead> BlobFs<A> {
    pub fn new(inner: A, runtime: BridgeRuntime) -> Self {
        Self { inner, runtime }
    }
}

impl<A: AsyncRead + Clone> UniversalReadFileOps for BlobFs<A> {
    type ContextConfig = A::Config;

    fn from_context(config: Self::ContextConfig) -> UioResult<Self> {
        // The context carries no runtime, so use the process-wide BridgeRuntime;
        // callers needing an isolated runtime construct via `BlobFs::new`.
        Ok(Self::new(A::open(&config)?, BridgeRuntime::global()))
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);
        let result = self.runtime.block_on(self.inner.list_files(prefix_path));
        if let Some(start_time) = start_time {
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "list_files({}) took {:?} and returned {} files",
                prefix_path.display(),
                start_time.elapsed(),
                result.as_ref().map_or(0, |files| files.len()),
            );
        }
        result
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);
        let result = self.runtime.block_on(self.inner.exists(path));
        if let Some(start_time) = start_time {
            log::trace!(
                target: crate::LATENCY_LOG_TARGET,
                "exists({}) took {:?}",
                path.display(),
                start_time.elapsed(),
            );
        }
        result
    }
}

impl<A: AsyncWrite + Clone> UniversalWriteFileOps for BlobFs<A> {
    fn create(&self, path: &Path, _expected_length: usize) -> UioResult<()> {
        // Object stores have no fixed-size preallocation; the expected
        // length is ignored, as the trait allows.
        self.runtime.block_on(self.inner.create(path))
    }

    fn create_dir(&self, _path: &Path) -> UioResult<()> {
        // No materialized directories.
        Ok(())
    }

    fn remove(&self, path: &Path) -> UioResult<()> {
        self.runtime.block_on(self.inner.remove(path))
    }

    fn remove_dir(&self, _path: &Path) -> UioResult<()> {
        // No materialized directories.
        Ok(())
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> UioResult<()> {
        // A whole-object put is atomic on object stores.
        self.runtime
            .block_on(self.inner.save(path, Bytes::copy_from_slice(bytes)))
    }
}

impl<A: AsyncRead + Clone> UniversalReadFs for BlobFs<A> {
    type File = BlobFile<A>;
    type OpenExtra = ();

    /// Open a per-object handle. Blob handles have no other open-time knobs:
    /// of [`OpenOptions`], only `writeable` is honored (it gates appends).
    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        _extra: (),
    ) -> UioResult<BlobFile<A>> {
        Ok(
            BlobFile::new(self.inner.clone(), self.runtime.clone(), path.as_ref())
                .with_writeable(options.writeable),
        )
    }
}
