mod cached_file;
mod cached_slice;
mod controller;
#[cfg(test)]
mod tests;

use std::ops::Range;

pub use cached_file::CachedFile;
pub use cached_slice::{CachedSlice, unsafe_transmute_zerocopy_vec};
pub use controller::CacheController;
use trififo::array_lookup::AsIndex;

/// We cache data in blocks of this size.
/// Should be multiple of filesystem block size (usually 4 KiB).
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Internal identifier of a cold file.
///
/// This is used as the starting [`BlockKey`] of the cold file in the cache
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileId {
    starting_block: BlockKey,
}

/// Offset within a file, in blocks.
///
/// `u32` with 4 KiB blocks is enough for up to 16 TiB files.
/// TODO(xzfc): maybe `u16` would be enough?
///     (luis): It would restrict it to 268 MiB, definitely not enough
///     (xzfc): right now, our chunks are 32 MiB (`CHUNK_SIZE`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BlockOffset(u32);

impl BlockOffset {
    /// The same offset but in bytes instead of blocks.
    fn bytes(self) -> usize {
        self.0 as usize * BLOCK_SIZE
    }
}

/// This uniquely identifies a block in a cold file.
/// Acts as a cache key.
#[derive(Copy, Hash, PartialEq, Eq, Clone, Debug)]
struct BlockKey(u64);

/// A request for a range of bytes inside of a block
struct BlockRequest {
    key: BlockKey,
    file_id: FileId,
    range: Range<usize>,
}

impl AsIndex for BlockKey {
    fn as_index(&self) -> usize {
        self.0 as usize
    }
}
