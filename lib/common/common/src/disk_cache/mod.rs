mod cached_file;
mod cached_slice;
mod controller;
#[cfg(test)]
mod tests;

use std::ops::Range;

pub use cached_file::CachedFile;
pub use controller::CacheController;

/// We cache data in blocks of this size.
/// Should be multiple of filesystem block size (usually 4 KiB).
pub const BLOCK_SIZE: usize = 16 * 1024;

/// Internal identifier of a cold file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileId(u32);

/// Offset within a file, in blocks.
///
/// `u32` with 16 KiB blocks covers up to 70 TiB worth of data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BlockOffset(u32);

impl BlockOffset {
    /// The same offset but in bytes instead of blocks.
    fn bytes(self) -> usize {
        self.0 as usize * BLOCK_SIZE
    }
}

/// This pair uniquely identifies a block in a cold file.
/// Acts as a cache key.
#[derive(Copy, Hash, PartialEq, Eq, Clone, Debug)]
struct BlockId {
    file_id: FileId,
    offset: BlockOffset,
}

/// A request for a range of bytes inside of a block
struct BlockRequest {
    key: BlockId,
    range: Range<usize>,
}
