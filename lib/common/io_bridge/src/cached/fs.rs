//! Filesystem handle producing [`CachedBlobFile`]s.

use std::path::Path;
use std::sync::Arc;

use common::universal_io::{
    DiskCacheConfig, DiskCacheFs, ListedFile, OpenOptions, UioResult, UniversalReadFs,
    UniversalWriteFs,
};

use super::CachedBlobFile;
use super::stats::CachedBlobStats;
use crate::file::BlobFile;
use crate::fs::BlobFs;
use crate::read::AsyncRead;
use crate::runtime::BridgeRuntime;
use crate::stats::RemoteIoStats;
use crate::write::AsyncAppend;

/// Construction context for [`CachedBlobFs`]: the local-mirror layout and
/// the remote backend's own construction config.
pub struct CachedBlobFsContext<C> {
    pub disk_cache: Arc<DiskCacheConfig>,
    pub remote: C,
}

/// Filesystem handle for [`CachedBlobFile`]: a [`DiskCacheFs`] for the
/// read-through mirrors plus a [`BlobFs`] for direct remote operations
/// (metadata, create/remove/atomic_save, and the append handles' remote side).
///
/// Unlike [`DiskCacheFs`], `open` accepts `writeable: true`: the mirror itself
/// stays read-only, and the writeable half lives in the combined handle.
#[derive(Clone)]
pub struct CachedBlobFs<A: AsyncRead + Clone> {
    pub(super) cache_fs: DiskCacheFs<BlobFile<A>>,
    pub(super) blob_fs: BlobFs<A>,
}

impl<A: AsyncRead + Clone> CachedBlobFs<A> {
    /// Build both halves around one shared backend handle, reporting their
    /// remote requests into one shared observer.
    pub fn new(remote: A, runtime: BridgeRuntime, disk_cache: Arc<DiskCacheConfig>) -> Self {
        let stats = RemoteIoStats::default();
        let remote_fs = BlobFs::new(remote.clone(), runtime.clone()).with_stats(stats.clone());
        Self {
            cache_fs: DiskCacheFs::new(disk_cache, remote_fs),
            blob_fs: BlobFs::new(remote, runtime).with_stats(stats),
        }
    }

    /// Observer of the disk-cache fetches and of every remote request, shared
    /// by this filesystem, its clones, and the files it opens.
    pub fn stats(&self) -> CachedBlobStats {
        CachedBlobStats {
            cache: self.cache_fs.stats(),
            remote: self.blob_fs.stats(),
        }
    }
}

impl<A: AsyncRead + Clone> std::fmt::Debug for CachedBlobFs<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { cache_fs, blob_fs } = self;
        f.debug_struct("CachedBlobFs")
            .field("cache_fs", cache_fs)
            .field("blob_fs", blob_fs)
            .finish()
    }
}

impl<A: AsyncAppend + Clone> UniversalReadFs for CachedBlobFs<A>
where
    A::Config: Clone,
{
    type File = CachedBlobFile<A>;
    type OpenExtra = <DiskCacheFs<BlobFile<A>> as UniversalReadFs>::OpenExtra;
    type ContextConfig = CachedBlobFsContext<A::Config>;

    fn from_context(context: Self::ContextConfig) -> UioResult<Self> {
        let CachedBlobFsContext { disk_cache, remote } = context;
        Ok(Self::new(
            A::open(&remote)?,
            BridgeRuntime::global(),
            disk_cache,
        ))
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        // The remote is the source of truth; mirrors are ephemeral.
        self.blob_fs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        self.blob_fs.exists(path)
    }

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File> {
        // The mirror is always opened read-only — appends are buffered in the
        // combined handle and synced by its flusher — so `writeable` gates
        // only the remote half.
        let mut cache_options = options;
        cache_options.writeable = false;
        let cache = self.cache_fs.open(path.as_ref(), cache_options, extra)?;

        let remote = self.blob_fs.open(path.as_ref(), options, ())?;

        Ok(CachedBlobFile::new(cache, remote, options.writeable))
    }
}

impl<A: AsyncAppend + Clone> UniversalWriteFs for CachedBlobFs<A>
where
    A::Config: Clone,
{
    type AppendFile = CachedBlobFile<A>;

    // Mutating file ops go straight to the remote.

    fn create(&self, path: &Path, _expected_length: usize) -> UioResult<()> {
        // Object stores have no fixed-size preallocation; the expected
        // length is ignored, as the trait allows.
        self.blob_fs.create(path)
    }

    fn create_dir(&self, _path: &Path) -> UioResult<()> {
        // No materialized directories.
        Ok(())
    }

    fn remove(&self, path: &Path) -> UioResult<()> {
        self.blob_fs.remove(path)
    }

    fn remove_dir(&self, _path: &Path) -> UioResult<()> {
        // No materialized directories.
        Ok(())
    }

    fn atomic_save(&self, path: &Path, bytes: &[u8]) -> UioResult<()> {
        self.blob_fs.atomic_save(path, bytes)
    }

    fn open_append(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
    ) -> UioResult<Self::AppendFile> {
        self.open(path, options.for_append(), Default::default())
    }
}
