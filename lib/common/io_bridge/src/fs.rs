use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::uio_trace::{self, Op};
use common::universal_io::{ListedFile, OpenOptions, UioResult, UniversalReadFs};

use crate::stats::RemoteIoStats;
use crate::{AsyncRead, AsyncWrite, BlobFile, BridgeRuntime};

/// Filesystem handle for an object-store backend: an [`AsyncRead`] handle plus
/// the [`BridgeRuntime`] used to drive its async operations. Opens per-object
/// [`BlobFile`] handles via [`UniversalReadFs::open`] and answers metadata
/// queries (`list_files`, `exists`) by blocking on the backend.
#[derive(Clone)]
pub struct BlobFs<A: AsyncRead> {
    inner: A,
    runtime: BridgeRuntime,
    stats: RemoteIoStats,
}

impl<A: AsyncRead> std::fmt::Debug for BlobFs<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            runtime,
            stats,
            inner: _,
        } = self;
        f.debug_struct("BlobFs")
            .field("runtime", runtime)
            .field("stats", stats)
            .finish_non_exhaustive()
    }
}

impl<A: AsyncRead> BlobFs<A> {
    pub fn new(inner: A, runtime: BridgeRuntime) -> Self {
        Self {
            inner,
            runtime,
            stats: RemoteIoStats::default(),
        }
    }

    /// Report the remote requests of this filesystem, and of the files it opens, into `stats`.
    pub fn with_stats(mut self, stats: RemoteIoStats) -> Self {
        self.stats = stats;
        self
    }

    /// Observer of the remote requests issued through this filesystem, its clones, and the
    /// files it opened.
    pub fn stats(&self) -> RemoteIoStats {
        self.stats.clone()
    }

    /// Like the async reads, the op rides the [`BridgeRuntime`] rather than
    /// the caller's executor, so the returned future needs no ambient reactor.
    /// It spawns on first poll.
    pub(crate) fn spawn<T, F>(
        &self,
        op: F,
    ) -> impl Future<Output = UioResult<T>> + Send + 'static + use<A, T, F>
    where
        T: Send + 'static,
        F: Future<Output = UioResult<T>> + Send + 'static,
    {
        let handle = self.runtime.handle().clone();
        async move { handle.spawn(uio_trace::Context::current().wrap(op)).await? }
    }
}

impl<A: AsyncRead + Clone> BlobFs<A> {
    /// Traced, latency-logged LIST, not yet bound to any executor.
    pub(crate) fn list_files_traced(
        &self,
        prefix_path: &Path,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + 'static + use<A> {
        let request = self.stats.request(Op::List, prefix_path, 0..0);
        let inner = self.inner.clone();
        let prefix_path = prefix_path.to_path_buf();
        async move {
            let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
            let start_time = enabled.then(std::time::Instant::now);
            let result = request.wrap(inner.list_files(&prefix_path)).await;
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
    }
}

impl<A: AsyncWrite + Clone> BlobFs<A> {
    pub fn save_async(
        &self,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> impl Future<Output = UioResult<()>> + Send + 'static + use<A> {
        let inner = self.inner.clone();
        let request = self.stats.request(Op::Save, &path, 0..bytes.len() as u64);
        self.spawn(async move { request.wrap(inner.save(&path, Bytes::from(bytes))).await })
    }

    pub fn remove_async(
        &self,
        path: PathBuf,
    ) -> impl Future<Output = UioResult<()>> + Send + 'static + use<A> {
        let inner = self.inner.clone();
        let request = self.stats.request(Op::Remove, &path, 0..0);
        self.spawn(async move { request.wrap(inner.remove(&path)).await })
    }
}

impl<A: AsyncRead + Clone> UniversalReadFs for BlobFs<A> {
    type File = BlobFile<A>;
    type OpenExtra = ();
    type ContextConfig = A::Config;

    fn from_context(config: Self::ContextConfig) -> UioResult<Self> {
        // The context carries no runtime, so use the process-wide BridgeRuntime;
        // callers needing an isolated runtime construct via `BlobFs::new`.
        Ok(Self::new(A::open(&config)?, BridgeRuntime::global()))
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        self.runtime.block_on(self.list_files_traced(prefix_path))
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        let enabled = log::log_enabled!(target: crate::LATENCY_LOG_TARGET, log::Level::Trace);
        let start_time = enabled.then(std::time::Instant::now);
        let result = self.runtime.block_on(
            self.stats
                .request(Op::Exists, path, 0..0)
                .wrap(self.inner.exists(path)),
        );
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
                .with_writeable(options.writeable)
                .with_stats(self.stats.clone()),
        )
    }
}

/// Deliberately no [`UniversalWriteFs`] impl: the write-capable
/// universal-IO filesystem for object stores is
/// [`CachedBlobFs`](crate::CachedBlobFs), which delegates its mutating file
/// ops to these inherent methods and hands out
/// [`CachedBlobFile`](crate::CachedBlobFile) append handles.
///
/// [`UniversalWriteFs`]: common::universal_io::UniversalWriteFs
impl<A: AsyncWrite + Clone> BlobFs<A> {
    pub fn create(&self, path: &Path) -> UioResult<()> {
        self.runtime.block_on(
            self.stats
                .request(Op::Create, path, 0..0)
                .wrap(self.inner.create(path)),
        )
    }

    pub fn remove(&self, path: &Path) -> UioResult<()> {
        self.runtime.block_on(
            self.stats
                .request(Op::Remove, path, 0..0)
                .wrap(self.inner.remove(path)),
        )
    }

    /// A whole-object put, atomic on object stores.
    pub fn atomic_save(&self, path: &Path, bytes: &[u8]) -> UioResult<()> {
        self.runtime.block_on(
            self.stats
                .request(Op::Save, path, 0..bytes.len() as u64)
                .wrap(self.inner.save(path, Bytes::copy_from_slice(bytes))),
        )
    }
}
