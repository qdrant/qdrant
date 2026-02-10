use std::borrow::Cow;
use std::ops::Range;
use std::path::PathBuf;
// use std::sync::Arc;

// use crate::ssd_cacher::Cacher;

pub const BLOCK_SIZE: usize = 4 * 1024; // 4KB

#[derive(Hash, PartialEq, Eq, Clone)]
struct BlockId {
    file_id: u32,
    chunk_offset: u32,
}

struct BlockDescriptor {
    key: BlockId,
    range: Range<usize>,
}

/// Abstraction over a file, which provides an interface for requesting ranges of bytes.
///
/// Internally, it maps these ranges into fixed size blocks, and requests them to the cache.
/// If not in cache, it will fetch them from the original path, and insert it.
pub(crate) struct CachedData {
    _original_path: PathBuf,
    file_id: u32,
    len: usize,
    // cacher: Arc<Cacher>,
}

impl CachedData {
    /// Returns the block descriptor for the provided bytes range
    fn blocks_for(
        &self,
        bytes_range: Range<usize>,
    ) -> impl ExactSizeIterator<Item = BlockDescriptor> {
        let first_block = bytes_range.start / BLOCK_SIZE;
        let leading_offset = bytes_range.start % BLOCK_SIZE;

        let last_block = bytes_range.end / BLOCK_SIZE;
        let trailing_offset = bytes_range.end.next_multiple_of(BLOCK_SIZE) - bytes_range.end;

        (first_block..last_block + 1).map(move |block_offset| {
            let block_id = BlockId {
                file_id: self.file_id,
                chunk_offset: block_offset as u32,
            };

            let range_start = if block_offset == first_block {
                leading_offset
            } else {
                0
            };

            let range_end = if block_offset == last_block {
                BLOCK_SIZE - trailing_offset
            } else {
                BLOCK_SIZE
            };

            BlockDescriptor {
                key: block_id,
                range: range_start..range_end,
            }
        })
    }

    fn get_from_cache(&self, _key: &BlockId) -> &[u8] {
        todo!()
    }

    /// Get a Cow reference to the range of bytes within the file.
    ///
    /// If the range is contained in a single block, it will return a borrowed reference to the block.
    /// Otherwise, it will allocate a Vec with `capacity == len`.
    pub fn get_range(&self, range: Range<usize>) -> Cow<'_, [u8]> {
        let total_len = range.end - range.start;
        let mut blocks_iter = self.blocks_for(range);
        if blocks_iter.len() == 1 {
            // single value case, just return the reference
            let bd = blocks_iter.next().unwrap();
            let block = self.get_from_cache(&bd.key);
            return Cow::Borrowed(&block[bd.range]);
        }
        // multi-block case: allocate a Vec and copy the needed parts
        let mut result = Vec::with_capacity(total_len);
        for bd in blocks_iter {
            let block = self.get_from_cache(&bd.key);
            let slice = &block[bd.range];
            result.extend_from_slice(slice);
        }
        result.shrink_to_fit();
        Cow::Owned(result)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}
