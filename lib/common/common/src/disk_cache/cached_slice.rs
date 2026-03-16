#![allow(dead_code)] // for now

use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
use std::{io, mem};

use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, CacheController, FileId};

/// Typed view over a cached file, simulating a `&[T]` backed by the block cache.
///
/// Internally maps element ranges into fixed-size blocks and fetches them
/// through the [`CacheController`]. Blocks that are already cached are returned
/// as zero-copy borrows from the mmap; multi-block reads allocate a `Vec<T>`
/// (not `Vec<u8>`) so alignment is always correct.
pub struct CachedSlice<T> {
    /// The id assigned by the controller for this file.
    file_id: FileId,

    /// Length of this file in bytes.
    len_bytes: usize,

    /// The controller backing this structure.
    controller: Arc<CacheController>,

    r#type: PhantomData<T>,
}

impl<T: bytemuck::Pod> CachedSlice<T> {
    /// Open a file through the cache controller and return a typed view over it.
    pub fn open(
        controller: &Arc<CacheController>,
        path: &std::path::Path,
    ) -> std::io::Result<Self> {
        let (file_id, len) = controller.open_file(path)?;
        Ok(Self {
            file_id,
            len_bytes: len,
            controller: Arc::clone(controller),
            r#type: PhantomData,
        })
    }

    /// Get a Cow reference to a range of elements within the file.
    ///
    /// The `range` is in **elements of T**, not bytes. For `T = u8` this is
    /// equivalent to a byte range.
    ///
    /// If the range is contained in a single block, it will return a borrowed
    /// reference into the mmap. Otherwise, it will allocate a `Vec<T>` and copy
    /// block data into it. Allocating as `Vec<T>` (rather than `Vec<u8>`)
    /// guarantees correct alignment for any `T`.
    pub fn get_range(&self, range: Range<usize>) -> io::Result<Cow<'_, [T]>> {
        let t_size = mem::size_of::<T>();
        debug_assert!(t_size != 0, "cannot use zero-sized type");

        let byte_range = range.start * t_size..range.end * t_size;
        let total_elements = range.end - range.start;
        let mut blocks_iter = self.blocks_for(byte_range);

        // TODO(perf): if blocks are consecutive in the big cache file, we can still return without allocating.
        if blocks_iter.len() == 1 {
            let req = blocks_iter.next().expect("We just checked len() == 1");
            let slice = self.controller.get_from_cache(req)?;

            let cow = match slice {
                Cow::Borrowed(bytes) => Cow::Borrowed(bytemuck::cast_slice(bytes)),
                Cow::Owned(vec_u8) => {
                    let mut vec_t = vec![T::zeroed(); vec_u8.len() / t_size];
                    bytemuck::cast_slice_mut::<T, u8>(&mut vec_t).copy_from_slice(&vec_u8);
                    Cow::Owned(vec_t)
                }
            };

            return Ok(cow);
        }

        // Multi-block: allocate Vec<T> directly for correct alignment.
        let mut result = vec![T::zeroed(); total_elements];
        let result_bytes = bytemuck::cast_slice_mut::<T, u8>(&mut result);
        let mut copied = 0;
        for req in blocks_iter {
            let slice = self.controller.get_from_cache(req)?;
            let end = copied + slice.len();
            result_bytes[copied..end].copy_from_slice(&slice);
            copied = end;
        }
        Ok(Cow::Owned(result))
    }

    #[cfg(test)]
    pub fn get(&self, idx: usize) -> io::Result<Cow<'_, T>> {
        let slice = self.get_range(idx..idx + 1)?;

        let cow = match slice {
            Cow::Borrowed(slice) => Cow::Borrowed(&slice[0]),
            Cow::Owned(mut vec) => Cow::Owned(vec.pop().unwrap()),
        };

        Ok(cow)
    }

    #[expect(clippy::len_without_is_empty)] // Doesn't make sense to cache 0-length files
    pub fn len(&self) -> usize {
        self.len_bytes / mem::size_of::<T>()
    }

    /// Returns the block descriptor for the provided bytes range.
    fn blocks_for(&self, bytes_range: Range<usize>) -> impl ExactSizeIterator<Item = BlockRequest> {
        debug_assert!(bytes_range.start <= bytes_range.end);
        debug_assert!(bytes_range.end <= self.len_bytes);

        blocks_for_range_in_file(self.file_id, bytes_range)
    }
}

// Extracted to make testing simpler
#[inline(always)]
fn blocks_for_range_in_file(
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
        let blocks: Vec<_> = blocks_for_range_in_file(file_id, 0..100).collect();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].key.offset.0, 0);
        assert_eq!(blocks[0].range, 0..100);

        //       block 0          block 1
        // |                |                 |
        //              < range >
        let blocks: Vec<_> =
            blocks_for_range_in_file(file_id, BLOCK_SIZE - 50..BLOCK_SIZE + 50).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 0);
        assert_eq!(blocks[0].range, (BLOCK_SIZE - 50)..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 1);
        assert_eq!(blocks[1].range, 0..50);

        //     block 2      block 3
        // |            |             |
        // <          range           >
        let blocks: Vec<_> =
            blocks_for_range_in_file(file_id, BLOCK_SIZE * 2..BLOCK_SIZE * 4).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 2);
        assert_eq!(blocks[0].range, 0..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 3);
        assert_eq!(blocks[1].range, 0..BLOCK_SIZE);

        //  block 9  (last full block)   block 10 (partial block with trailing data)
        // |                           |         000000000000000000000|
        //    <         range                   >
        let blocks: Vec<_> =
            blocks_for_range_in_file(file_id, BLOCK_SIZE * 9 + 50..file_len).collect();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].key.offset.0, 9);
        assert_eq!(blocks[0].range, 50..BLOCK_SIZE);
        assert_eq!(blocks[1].key.offset.0, 10);
        assert_eq!(blocks[1].range, 0..100);
    }
}
