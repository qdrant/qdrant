//! The async surface of the append-caching blob handle: both halves (the
//! disk-cache mirror and the raw remote) are opened/read through their own
//! async impls.

use std::ops::Range;
use std::path::PathBuf;

use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    OpenOptions, UioResult, UniversalReadAsync, UniversalReadFsAsync, UniversalWriteFsAsync,
};

use super::CachedBlobFile;
use super::fs::CachedBlobFs;
use crate::write::AsyncAppend;

impl<A: AsyncAppend + Clone> UniversalReadFsAsync for CachedBlobFs<A>
where
    A::Config: Clone,
{
    async fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: Self::OpenExtra,
    ) -> UioResult<Self::File> {
        let mut cache_options = options;
        cache_options.writeable = false;

        let cache = self
            .cache_fs
            .open_async(path.clone(), cache_options, extra)
            .await?;

        let remote = self.blob_fs.open_async(path, options, ()).await?;

        Ok(CachedBlobFile::new(cache, remote, options.writeable))
    }
}

impl<A: AsyncAppend + Clone> UniversalReadAsync for CachedBlobFile<A>
where
    A::Config: Clone,
{
    fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> impl Future<Output = UioResult<ACow<'_>>> {
        self.cache.read_bytes_async(range, access_pattern, align)
    }
}

impl<A: AsyncAppend + Clone> UniversalWriteFsAsync for CachedBlobFs<A>
where
    A::Config: Clone,
{
    fn create_dir_async(&self, _path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_ {
        // No materialized directories.
        std::future::ready(Ok(()))
    }

    fn atomic_save_async(
        &self,
        path: PathBuf,
        bytes: Vec<u8>,
    ) -> impl Future<Output = UioResult<()>> + Send + '_ {
        self.blob_fs.save_async(path, bytes)
    }

    fn remove_async(&self, path: PathBuf) -> impl Future<Output = UioResult<()>> + Send + '_ {
        self.blob_fs.remove_async(path)
    }

    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        self.blob_fs.runtime().block_on(fut)
    }
}
