//! Filesystem handle producing [`CachedBlobFile`]s.

use std::path::Path;
use std::sync::Arc;

use common::universal_io::{
    DiskCacheConfig, DiskCacheFs, DiskCacheFsContext, ListedFile, OpenOptions, UioResult,
    UniversalReadFileOps, UniversalReadFs, UniversalWriteFileOps,
};

use super::{AppendMode, CachedBlobFile};
use crate::file::BlobFile;
use crate::fs::BlobFs;
use crate::read::AsyncRead;
use crate::write::AsyncAppend;

/// Construction context for [`CachedBlobFs`]: the local-mirror layout, the
/// remote backend's own construction config, and the append strategy.
pub struct CachedBlobFsContext<C> {
    pub disk_cache: Arc<DiskCacheConfig>,
    pub remote: C,
    pub append_mode: AppendMode,
}

/// Filesystem handle for [`CachedBlobFile`]: a [`DiskCacheFs`] for the
/// read-through mirrors plus a [`BlobFs`] for direct remote operations
/// (metadata, create/remove/atomic_save, and the append handles' remote side).
///
/// Unlike [`DiskCacheFs`], `open` accepts `writeable: true`: the mirror itself
/// stays read-only, and the writeable half lives in the combined handle.
#[derive(Clone)]
pub struct CachedBlobFs<A: AsyncRead + Clone> {
    cache_fs: DiskCacheFs<BlobFile<A>>,
    blob_fs: BlobFs<A>,
    mode: AppendMode,
}

impl<A: AsyncRead + Clone> std::fmt::Debug for CachedBlobFs<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            cache_fs,
            blob_fs,
            mode,
        } = self;
        f.debug_struct("CachedBlobFs")
            .field("cache_fs", cache_fs)
            .field("blob_fs", blob_fs)
            .field("mode", mode)
            .finish()
    }
}

impl<A: AsyncRead + Clone> UniversalReadFileOps for CachedBlobFs<A>
where
    A::Config: Clone,
{
    type ContextConfig = CachedBlobFsContext<A::Config>;

    fn from_context(context: Self::ContextConfig) -> UioResult<Self> {
        let CachedBlobFsContext {
            disk_cache,
            remote,
            append_mode,
        } = context;

        let blob_fs = BlobFs::<A>::from_context(remote.clone())?;
        let cache_fs = DiskCacheFs::from_context(DiskCacheFsContext {
            config: disk_cache,
            remote,
        })?;

        Ok(Self {
            cache_fs,
            blob_fs,
            mode: append_mode,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        // The remote is the source of truth; mirrors are ephemeral.
        self.blob_fs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        self.blob_fs.exists(path)
    }
}

impl<A: AsyncAppend + Clone> UniversalReadFs for CachedBlobFs<A>
where
    A::Config: Clone,
{
    type File = CachedBlobFile<A>;
    type OpenExtra = <DiskCacheFs<BlobFile<A>> as UniversalReadFs>::OpenExtra;

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

        Ok(CachedBlobFile::new(
            cache,
            remote,
            self.mode,
            options.writeable,
        ))
    }
}

impl<A: AsyncAppend + Clone> UniversalWriteFileOps for CachedBlobFs<A>
where
    A::Config: Clone,
{
    type AppendFile = CachedBlobFile<A>;

    // Mutating file ops go straight to the remote.
    // TODO: delegate to inherent `BlobFs` ops once `BlobFs` loses its
    // `UniversalWriteFileOps` impl to `CachedBlobFs`.

    fn create(&self, path: &Path, expected_length: usize) -> UioResult<()> {
        self.blob_fs.create(path, expected_length)
    }

    fn create_dir(&self, path: &Path) -> UioResult<()> {
        self.blob_fs.create_dir(path)
    }

    fn remove(&self, path: &Path) -> UioResult<()> {
        self.blob_fs.remove(path)
    }

    fn remove_dir(&self, path: &Path) -> UioResult<()> {
        self.blob_fs.remove_dir(path)
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
