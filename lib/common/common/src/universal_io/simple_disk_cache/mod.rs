mod config;
mod file;

mod local_state;
pub mod pipeline;
#[cfg(test)]
mod tests;

use std::ops::Range;

pub use config::DiskCacheConfig;
pub use file::DiskCache;

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
