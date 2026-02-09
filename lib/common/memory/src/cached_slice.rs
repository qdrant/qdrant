use std::borrow::Cow;
use std::ops::{Index, Range};
use std::path::PathBuf;
use std::sync::Arc;

use crate::ssd_cacher::Cacher;

pub const CHUNK_SIZE: usize = 4 * 1024; // 4KB

#[derive(Hash, PartialEq, Eq, Clone)]
struct BlockId {
    file_id: u32,
    chunk_offset: u32,
}

struct BlockDescriptor {
    key: BlockId,
    range: Range<usize>,
}

pub struct CachedData {
    original_path: PathBuf,
    file_id: u32,
    len: usize,
    cacher: Arc<Cacher>,
}

impl CachedData {
    /// Returns the block descriptor for the provided bytes range
    fn blocks_for(
        &self,
        bytes_range: Range<usize>,
    ) -> impl ExactSizeIterator<Item = BlockDescriptor> {
        let first_block = bytes_range.start / CHUNK_SIZE;
        let leading_offset = bytes_range.start % CHUNK_SIZE;

        let last_block = bytes_range.end / CHUNK_SIZE;
        let trailing_offset = bytes_range.end.next_multiple_of(CHUNK_SIZE) - bytes_range.end;

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
                CHUNK_SIZE - trailing_offset
            } else {
                CHUNK_SIZE
            };

            BlockDescriptor {
                key: block_id,
                range: range_start..range_end,
            }
        })
    }

    fn get_from_cache(&self, key: &BlockId) -> &[u8] {
        todo!()
    }

    fn get_range(&self, range: Range<usize>) -> Cow<[u8]> {
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
        return Cow::Owned(result);
    }
}
