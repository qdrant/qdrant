mod config;
mod file;
mod fs;

mod local_state;
pub mod pipeline;
#[cfg(test)]
mod tests;

use std::ops::Range;

pub use config::DiskCacheConfig;
pub use file::DiskCache;
pub use fs::{DiskCacheFs, DiskCacheFsContext};

use crate::universal_io::{UniversalRead, UniversalReadFs};

/// Trait bundle for remote backends that can be cached by [`DiskCache`].
pub trait DiskCacheRemote:
    UniversalRead<
        Fs: Clone + Send + Sync + UniversalReadFs<OpenExtra: Clone + Send + Sync>,
        OwnedReadPipeline<()>: Send,
    > + Clone
{
}

impl<R> DiskCacheRemote for R
where
    R: UniversalRead + Clone,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::OwnedReadPipeline<()>: Send,
{
}

/// Files are logically split into fixed-size blocks; the roaring bitmap
/// tracks population on a per-block basis.
///
/// Matches `disk_cache::BLOCK_SIZE` and is a small multiple of typical
/// filesystem block sizes (usually 4 KiB).
const BLOCK_SIZE: usize = 16 * 1024; // 16kB

fn to_block_range(byte_range: Range<u64>) -> Range<u32> {
    let start = (byte_range.start / BLOCK_SIZE as u64) as u32;
    if byte_range.start >= byte_range.end {
        // empty byte range returns empty block range
        return start..start;
    }
    let end = byte_range.end.div_ceil(BLOCK_SIZE as u64) as u32;
    start..end
}
