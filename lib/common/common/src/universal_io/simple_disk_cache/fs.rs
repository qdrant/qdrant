use std::fmt::Debug;
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::config::DiskCacheConfig;
use super::file::DiskCache;
use crate::universal_io::{
    OpenOptions, Result, UniversalIoError, UniversalRead, UniversalReadFileOps, UniversalReadFs,
};

/// Filesystem handle for the simple disk cache. Carries the remote-side
/// filesystem used to fetch missing blocks.
///
/// `DiskCacheFs` is parameterized by the remote *file* type `R`; the
/// underlying remote filesystem is `R::Fs`. This indirection lets the
/// `UniversalRead::Fs = DiskCacheFs<R>` constraint on
/// [`DiskCache<R>`] line up without an extra generic parameter on the
/// file type.
pub struct DiskCacheFs<R>
where
    R: UniversalRead,
{
    remote_fs: R::Fs,
}

impl<R> Clone for DiskCacheFs<R>
where
    R: UniversalRead,
    R::Fs: Clone,
{
    fn clone(&self) -> Self {
        Self {
            remote_fs: self.remote_fs.clone(),
        }
    }
}

impl<R> Debug for DiskCacheFs<R>
where
    R: UniversalRead,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheFs")
            .field("remote_fs", &self.remote_fs)
            .finish()
    }
}

impl<R> UniversalReadFileOps for DiskCacheFs<R>
where
    R: UniversalRead,
{
    type ContextConfig = <R::Fs as UniversalReadFileOps>::ContextConfig;

    fn from_context(ctx: Self::ContextConfig) -> Result<Self> {
        Ok(Self {
            remote_fs: R::Fs::from_context(ctx)?,
        })
    }

    fn list_files(&self, prefix_path: &Path) -> Result<Vec<PathBuf>> {
        self.remote_fs.list_files(prefix_path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.remote_fs.exists(path)
    }
}

impl<R> UniversalReadFs for DiskCacheFs<R>
where
    R: UniversalRead + Clone,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::OwnedReadPipeline<u8, Range<u32>>: Send,
{
    type File = DiskCache<R>;
    type OpenExtra = <R::Fs as UniversalReadFs>::OpenExtra;

    fn open(
        &self,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> Result<DiskCache<R>> {
        let config = DiskCacheConfig::global().ok_or_else(|| {
            UniversalIoError::uninitialized(
                "DiskCacheConfig must be initialized via `DiskCacheConfig::initialize_global` \
                 before opening a DiskCache",
            )
        })?;
        let local_path = config.local_path_for(path.as_ref())?;
        Ok(DiskCache::new(
            self.remote_fs.clone(),
            extra,
            path.as_ref(),
            local_path,
            options,
        ))
    }
}
