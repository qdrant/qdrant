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
        ReadPipeline<'static, ()>: Send,
        ReadPipeline<'static, u64>: Send,
        ReadPipeline<'static, Range<u32>>: Send,
    > + Clone
    + 'static
{
}

impl<R> DiskCacheRemote for R
where
    R: UniversalRead + Clone + 'static,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
    R::ReadPipeline<'static, ()>: Send,
    R::ReadPipeline<'static, u64>: Send,
    R::ReadPipeline<'static, Range<u32>>: Send,
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

/// Expand a requested `byte_range` to the block-aligned region that must be
/// fetched from the remote to cover it, clamped to the file's `len` (EOF).
///
/// Returns the covering block range together with its EOF-clamped byte range,
/// or `None` when there is nothing to fetch — either `byte_range` is empty, or
/// it starts at/beyond EOF so the clamped range collapses to empty.
fn block_aligned_fetch(byte_range: Range<u64>, file_len: u64) -> Option<(Range<u32>, Range<u64>)> {
    let blocks_range = to_block_range(byte_range);
    if blocks_range.is_empty() {
        return None;
    }

    // BLOCK_SIZE aligned, clamped to EOF.
    let byte_offset = u64::from(blocks_range.start) * BLOCK_SIZE as u64;
    let fetch_length = blocks_range.len() as u64 * BLOCK_SIZE as u64;
    let max_length = file_len.saturating_sub(byte_offset);
    let blocks_byte_range = byte_offset..byte_offset + max_length.min(fetch_length);

    // The first block already starts past EOF: nothing valid to fetch.
    if blocks_byte_range.is_empty() {
        return None;
    }

    Some((blocks_range, blocks_byte_range))
}
