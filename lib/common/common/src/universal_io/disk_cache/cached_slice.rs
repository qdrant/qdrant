use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use super::controller::CacheRead;
use super::{BLOCK_SIZE, BlockId, BlockOffset, BlockRequest, CacheController, FileId};
use crate::universal_io;
use crate::universal_io::io_uring::IoUringFile;

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
    controller: Arc<CacheController<IoUringFile>>,

    r#type: PhantomData<T>,
}

impl<T: bytemuck::Pod> CachedSlice<T> {
    /// Open a file through the cache controller and return a typed view over it.
    pub fn open(
        controller: &Arc<CacheController<IoUringFile>>,
        path: &Path,
    ) -> universal_io::Result<Self> {
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
    pub fn get_range(&self, range: Range<usize>) -> universal_io::Result<Cow<'_, [T]>> {
        let t_size = mem::size_of::<T>();
        debug_assert!(t_size != 0, "cannot use zero-sized type");

        let total_elements = range.end - range.start;
        if total_elements == 0 {
            return Ok(Cow::Borrowed(&[]));
        }

        let byte_range = range.start * t_size..range.end * t_size;
        let mut blocks_iter = self.blocks_for(byte_range);

        // TODO(perf): if blocks are consecutive in the big cache file, we can potentially return without allocating.
        if blocks_iter.len() == 1 {
            let req = blocks_iter.next().expect("We just checked len() == 1");
            let result = self.controller.get_from_cache(req, |bytes| {
                let mut vec_t = vec![T::zeroed(); bytes.len() / t_size];
                bytemuck::cast_slice_mut::<T, u8>(&mut vec_t).copy_from_slice(bytes);
                vec_t
            })?;

            return Ok(match result {
                CacheRead::Hit(bytes) => Cow::Borrowed(bytemuck::cast_slice(bytes)),
                CacheRead::Miss(vec_t) => Cow::Owned(vec_t),
            });
        }

        // Multi-block: delegate to the batch path which submits all
        // cold-storage reads together via io_uring.
        let mut result = None;
        self.get_range_batch(std::iter::once(range), |_idx, buf| {
            result = Some(buf.to_vec());
            Ok(())
        })?;
        Ok(Cow::Owned(result.expect("callback was called")))
    }

    /// Batch version of [`get_range`](Self::get_range).
    ///
    /// All block reads across every range are submitted together via a single
    /// `get_from_cache_batch` call, so cold-storage I/O is batched through
    /// io_uring.
    ///
    /// `callback(range_idx, &[T])` is called once per input range with the
    /// fully assembled data for that range.
    pub fn get_range_batch(
        &self,
        ranges: impl IntoIterator<Item = Range<usize>>,
        mut callback: impl FnMut(usize, &[T]) -> universal_io::Result<()>,
    ) -> universal_io::Result<()> {
        let t_size = mem::size_of::<T>();
        debug_assert!(t_size != 0, "cannot use zero-sized type");

        struct BlockMeta {
            range_idx: usize,
            /// For multi-block ranges: index into `multiblock_buffers` and byte
            /// offset within that buffer. `None` for single-block ranges
            /// (delivered inline without a buffer).
            multiblock: Option<(usize, usize)>,
        }

        let mut block_requests: Vec<BlockRequest> = Vec::new();
        let mut block_meta: Vec<BlockMeta> = Vec::new();
        // Dense list of (range_idx, buffer) for multi-block ranges only.
        // Empty (no heap allocation) when all ranges are single-block.
        let mut multiblock_buffers: Vec<(usize, Vec<T>)> = Vec::new();

        for (range_idx, range) in ranges.into_iter().enumerate() {
            let total_elements = range.end - range.start;
            if total_elements == 0 {
                continue;
            }

            let byte_range = range.start * t_size..range.end * t_size;
            let blocks = self.blocks_for(byte_range);

            let buffer_idx = (blocks.len() > 1).then(|| {
                let buffer_idx = multiblock_buffers.len();
                multiblock_buffers.push((range_idx, vec![T::zeroed(); total_elements]));
                buffer_idx
            });

            let mut dest_offset = 0;
            for block in blocks {
                block_meta.push(BlockMeta {
                    range_idx,
                    multiblock: buffer_idx.map(|idx| (idx, dest_offset)),
                });
                dest_offset += block.range.len();
                block_requests.push(block);
            }
        }

        if block_requests.is_empty() {
            return Ok(());
        }

        let mut callback_err: Option<universal_io::UniversalIoError> = None;

        self.controller
            .get_from_cache_batch(block_requests, |block_idx, slice| {
                if callback_err.is_some() {
                    return;
                }

                let meta = &block_meta[block_idx];
                if let Some((buffer_idx, dest_offset)) = meta.multiblock {
                    // Multi-block range: copy into the multiblock buffer.
                    let buf = &mut multiblock_buffers[buffer_idx].1;
                    let buf_bytes = bytemuck::cast_slice_mut::<T, u8>(buf);
                    buf_bytes[dest_offset..dest_offset + slice.len()].copy_from_slice(slice);
                } else {
                    // Single-block range: deliver directly, no buffer needed.
                    if let Err(e) = callback(meta.range_idx, bytemuck::cast_slice(slice)) {
                        callback_err = Some(e);
                    }
                }
            })?;

        if let Some(err) = callback_err {
            return Err(err);
        }

        // Deliver assembled multi-block buffers.
        for (range_idx, buf) in multiblock_buffers {
            callback(range_idx, &buf)?;
        }

        Ok(())
    }

    /// Try to make every block this file spans present in the cache.
    ///
    /// Touches one byte per block in a sequential pass. On miss, the full
    /// block is read from cold storage into the cache.
    pub fn populate(&self) -> universal_io::Result<()> {
        if self.len_bytes == 0 {
            return Ok(());
        }

        let num_blocks = self.len_bytes.div_ceil(BLOCK_SIZE);
        for block_idx in 0..num_blocks {
            let req = BlockRequest {
                key: BlockId {
                    file_id: self.file_id,
                    offset: BlockOffset(
                        u32::try_from(block_idx).expect("file too large disk cache"),
                    ),
                },
                // Request a single byte — enough to trigger caching the whole block.
                range: 0..1,
            };

            // We only care about the side-effect of populating the cache.
            // The no-op closure avoids allocating anything on miss.
            self.controller.get_from_cache(req, |_| ())?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn get(&self, idx: usize) -> universal_io::Result<Cow<'_, T>> {
        let slice = self.get_range(idx..idx + 1)?;

        let cow = match slice {
            Cow::Borrowed(slice) => Cow::Borrowed(&slice[0]),
            Cow::Owned(mut vec) => Cow::Owned(vec.pop().unwrap()),
        };

        Ok(cow)
    }

    #[expect(clippy::len_without_is_empty)] // Doesn't make sense to cache 0-length files
    pub fn len(&self) -> usize {
        self.len_bytes / size_of::<T>()
    }

    /// Returns the block descriptor for the provided bytes range.
    fn blocks_for(&self, bytes_range: Range<usize>) -> impl ExactSizeIterator<Item = BlockRequest> {
        debug_assert!(bytes_range.start <= bytes_range.end);
        debug_assert!(bytes_range.end <= self.len_bytes);
        debug_assert!(!bytes_range.is_empty(), "empty range would underflow");

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

    // Not a RangeInclusive (..=) because it doesn't implement ExactSizeIterator
    (first_block..last_block + 1).map(move |block_offset| {
        let block_id = BlockId {
            file_id,
            offset: BlockOffset(
                u32::try_from(block_offset).expect("file too large for block cache (>70 TiB)"),
            ),
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
