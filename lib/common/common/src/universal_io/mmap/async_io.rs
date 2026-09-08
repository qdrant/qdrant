//! Trivial async impls for the local mmap backend: local opens and reads
//! complete inline, so the returned futures are already resolved. They exist
//! so mmap-backed configurations satisfy async-bounded code — [`CachedFs`]
//! requires its inner filesystem to be [`UniversalReadFsAsync`], and mmap
//! files qualify as [`DiskCacheRemote`]s in tests.
//!
//! [`CachedFs`]: crate::universal_io::CachedFs
//! [`DiskCacheRemote`]: crate::universal_io::DiskCacheRemote

use std::future::ready;
use std::ops::Range;
use std::path::PathBuf;

use super::{MmapFile, MmapFs};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{
    OpenOptions, UioResult, UniversalRead, UniversalReadAsync, UniversalReadFs,
    UniversalReadFsAsync, UniversalWriteFileOps, UniversalWriteFsAsync,
};

impl UniversalReadFsAsync for MmapFs {
    fn open_async(
        &self,
        path: PathBuf,
        options: OpenOptions,
        extra: (),
    ) -> impl Future<Output = UioResult<MmapFile>> + '_ {
        ready(self.open(&path, options, extra))
    }
}

impl UniversalReadAsync for MmapFile {
    fn read_bytes_async<P: AccessPattern>(
        &self,
        range: Range<u64>,
        access_pattern: P,
        align: usize,
    ) -> impl Future<Output = UioResult<ACow<'_>>> {
        ready(self.read_bytes(range, access_pattern, align))
    }
}

/// Unlike the reads above, the writes are deferred to first poll, as
/// [`UniversalWriteFsAsync`] requires.
impl UniversalWriteFsAsync for MmapFs {
    async fn create_dir_async(&self, path: PathBuf) -> UioResult<()> {
        self.create_dir(&path)
    }

    async fn atomic_save_async(&self, path: PathBuf, bytes: Vec<u8>) -> UioResult<()> {
        self.atomic_save(&path, &bytes)
    }

    async fn remove_async(&self, path: PathBuf) -> UioResult<()> {
        self.remove(&path)
    }
}
