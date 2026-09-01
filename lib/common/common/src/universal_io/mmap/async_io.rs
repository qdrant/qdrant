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
    UniversalReadFsAsync,
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
