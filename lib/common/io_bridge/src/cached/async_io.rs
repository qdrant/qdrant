//! The async surface of the append-caching blob handle: both halves (the
//! disk-cache mirror and the raw remote) are opened/read through their own
//! async impls.

use std::ops::Range;
use std::path::{Path, PathBuf};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::ext::aligned_vec::ACow;
use common::generic_consts::AccessPattern;
use common::universal_io::{
    ChunkSink, ListedFile, OpenOptions, UioResult, UniversalReadAsync, UniversalReadFsAsync,
    UniversalWriteFsAsync,
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
        hw_acc: HwMeasurementAcc,
    ) -> UioResult<Self::File> {
        let mut cache_options = options;
        cache_options.writeable = false;

        let cache = self
            .cache_fs
            .open_async(path.clone(), cache_options, extra, hw_acc.clone())
            .await?;

        let remote = self
            .blob_fs
            .open_async(path, options, (), hw_acc.clone())
            .await?;

        Ok(CachedBlobFile::new(cache, remote, options.writeable))
    }

    fn list_files_async<'a>(
        &'a self,
        prefix_path: &'a Path,
        hw_acc: HwMeasurementAcc,
    ) -> impl Future<Output = UioResult<Vec<ListedFile>>> + Send + use<'a, A> {
        // The remote is the source of truth; mirrors are ephemeral.
        self.blob_fs.list_files_async(prefix_path, hw_acc.clone())
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
        hw_acc: HwMeasurementAcc,
    ) -> impl Future<Output = UioResult<ACow<'_>>> {
        self.cache
            .read_bytes_async(range, access_pattern, align, hw_acc.clone())
    }

    fn read_from_into_async<W, I>(
        &self,
        from: u64,
        init: I,
        hw_acc: HwMeasurementAcc,
    ) -> impl Future<Output = UioResult<W>> + Send
    where
        I: FnOnce(u64) -> UioResult<W> + Send + 'static,
        W: ChunkSink + Send + 'static,
    {
        self.cache.read_from_into_async(from, init, hw_acc.clone())
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
        hw_acc: HwMeasurementAcc,
    ) -> impl Future<Output = UioResult<()>> + Send + '_ {
        self.blob_fs.save_async(path, bytes, &hw_acc)
    }

    fn remove_async(
        &self,
        path: PathBuf,
        hw_acc: HwMeasurementAcc,
    ) -> impl Future<Output = UioResult<()>> + Send + '_ {
        self.blob_fs.remove_async(path, &hw_acc)
    }
}
