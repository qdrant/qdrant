#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, CacheController, FileId};

/// Abstraction over a file, which provides an interface for requesting ranges of bytes.
///
/// Internally, it maps these ranges into fixed size blocks, and requests them to the cache.
/// If not in cache, it will fetch them from the original path, and insert it.
#[derive(Debug)]
pub struct CachedFile {
    /// The id assigned by `cacher` for this file. This is combined with a block offset
    /// to create [`BlockRequest`]s
    pub(super) file_id: FileId,

    /// Length of this file in bytes
    pub(super) len: usize,

    /// The controller backing up this structure.
    pub(super) controller: Arc<CacheController>,
}

impl CachedFile {
    /// Returns the block descriptor for the provided bytes range
    fn blocks_for(&self, bytes_range: Range<usize>) -> impl ExactSizeIterator<Item = BlockRequest> {
        // TODO: should we make this checked? i.e. return Option
        debug_assert!(bytes_range.start <= bytes_range.end);
        debug_assert!(bytes_range.end <= self.len);

        blocks_for_range_in(self.file_id, bytes_range)
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
            let req = blocks_iter.next().expect("We just checked len() == 1");
            let slice = self
                .controller
                .get_from_cache(req)
                .expect("TODO: Reading or writing error");

            return slice;
        }

        // multi-block case: allocate exactly `total_len` bytes
        let mut result = vec![0; total_len];
        let mut copied = 0;
        for req in blocks_iter {
            let slice = self
                .controller
                .get_from_cache(req)
                .expect("TODO: Reading or writing error");
            let end = copied + slice.len();
            result[copied..end].copy_from_slice(&slice);
            copied = end;
        }
        Cow::Owned(result)
    }

    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }
}

// Extracted to make testing simpler
#[inline(always)]
fn blocks_for_range_in(
    file_id: FileId,
    bytes_range: Range<usize>,
) -> impl ExactSizeIterator<Item = BlockRequest> {
    let first_block = bytes_range.start / BLOCK_SIZE;
    let leading_offset = bytes_range.start - (first_block * BLOCK_SIZE);
    let last_block = (bytes_range.end - 1) / BLOCK_SIZE;
    let trailing_offset = bytes_range.end - (last_block * BLOCK_SIZE);

    (first_block..last_block + 1).map(move |block_offset| {
        let block_id = BlockId {
            file_id,
            offset: BlockOffset(block_offset as u32),
        };

        let range_start = if block_offset == first_block {
            leading_offset
        } else {
            0
        };

        let range_end = if block_offset == last_block {
            trailing_offset
        } else {
            BLOCK_SIZE
        };

        BlockRequest {
            key: block_id,
            range: range_start..range_end,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::BLOCK_SIZE;

    #[test]
    fn test_block_request_calculation() {
        let file_id = FileId(0);

        // 10 full blocks and 100 extra bytes in last block
        let file_len = BLOCK_SIZE * 10 + 100;

        //     block 0
        // |                          ... |
        //  < range >
        let blocks: Vec<_> = blocks_for_range_in(file_id, 0..100).collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].key.offset.0, 0);
        assert_eq!(blocks[0].range, 0..100);

        //       block 0          block 1
        // |                |                 |
        //              < range >
        let blocks: Vec<_> =
            blocks_for_range_in(file_id, BLOCK_SIZE - 50..BLOCK_SIZE + 50).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 0);
        assert_eq!(blocks[0].range, (BLOCK_SIZE - 50)..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 1);
        assert_eq!(blocks[1].range, 0..50);

        //     block 2      block 3
        // |            |             |
        // <          range           >
        let blocks: Vec<_> = blocks_for_range_in(file_id, BLOCK_SIZE * 2..BLOCK_SIZE * 4).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 2);
        assert_eq!(blocks[0].range, 0..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 3);
        assert_eq!(blocks[1].range, 0..BLOCK_SIZE);

        //  block 9  (last full block)   block 10 (partial block with trailing data)
        // |                           |         000000000000000000000|
        //    <         range                   >
        let blocks: Vec<_> = blocks_for_range_in(file_id, BLOCK_SIZE * 9 + 50..file_len).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 9);
        assert_eq!(blocks[0].range, 50..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 10);
        assert_eq!(blocks[1].range, 0..100);
    }
}
