mod gaps;

use std::ops::Range;
use std::path::{Path, PathBuf};

use bitvec::slice::BitSlice;
use gaps::{BitmaskGaps, RegionGaps};
use itertools::Itertools;
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::{self, MmapBitSlice};

use crate::config::StorageConfig;
use crate::tracker::{BlockOffset, PageId};

const BITMASK_NAME: &str = "bitmask.dat";

type RegionId = u32;

#[derive(Debug)]
pub struct Bitmask {
    config: StorageConfig,

    /// A summary of every 1KB (8_192 bits) of contiguous zeros in the bitmask, or less if it is the last region.
    regions_gaps: BitmaskGaps,

    /// The actual bitmask. Each bit represents a block. A 1 means the block is used, a 0 means it is free.
    bitslice: MmapBitSlice,

    /// The path to the file containing the bitmask.
    path: PathBuf,
}

/// Access pattern to the bitmask is always random reads by the already calculated page id.
/// We never need to iterate over multiple bitmask file pages in a row, therefore we can use random access.
const DEFAULT_ADVICE: Advice = Advice::Random;

impl Bitmask {
    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.path.clone(), self.regions_gaps.path()]
    }

    /// Calculate the amount of trailing free blocks in the bitmask.
    pub fn trailing_free_blocks(&self) -> u32 {
        self.regions_gaps.trailing_free_blocks()
    }

    /// Calculate the amount of bytes needed for covering the blocks of a page.
    fn length_for_page(config: &StorageConfig) -> usize {
        assert_eq!(
            config.page_size_bytes % config.block_size_bytes,
            0,
            "Page size must be a multiple of block size"
        );

        // one bit per block
        let bits = config.page_size_bytes / config.block_size_bytes;

        // length in bytes
        bits / u8::BITS as usize
    }

    /// Create a bitmask for one page
    pub(crate) fn create(dir: &Path, config: StorageConfig) -> Result<Self, String> {
        debug_assert!(
            config.page_size_bytes % config.block_size_bytes * config.region_size_blocks == 0,
            "Page size must be a multiple of block size * region size"
        );

        let length = Self::length_for_page(&config);

        // create bitmask mmap
        let path = Self::bitmask_path(dir);
        create_and_ensure_length(&path, length).unwrap();
        let mmap = open_write_mmap(&path, AdviceSetting::from(DEFAULT_ADVICE), false)
            .map_err(|err| err.to_string())?;
        let mmap_bitslice = MmapBitSlice::try_from(mmap, 0).map_err(|err| err.to_string())?;

        assert_eq!(mmap_bitslice.len(), length * 8, "Bitmask length mismatch");

        // create regions gaps mmap
        let num_regions = mmap_bitslice.len() / config.region_size_blocks;
        let region_gaps = vec![RegionGaps::all_free(config.region_size_blocks as u16); num_regions];

        let mmap_region_gaps = BitmaskGaps::create(dir, region_gaps.into_iter(), config.clone());

        Ok(Self {
            config,
            regions_gaps: mmap_region_gaps,
            bitslice: mmap_bitslice,
            path,
        })
    }

    pub(crate) fn open(dir: &Path, config: StorageConfig) -> Result<Self, String> {
        debug_assert!(
            config.page_size_bytes % config.block_size_bytes == 0,
            "Page size must be a multiple of block size"
        );

        let path = Self::bitmask_path(dir);
        if !path.exists() {
            return Err(format!("Bitmask file does not exist: {}", path.display()));
        }
        let mmap = open_write_mmap(&path, AdviceSetting::from(DEFAULT_ADVICE), false)
            .map_err(|err| err.to_string())?;
        let mmap_bitslice = MmapBitSlice::from(mmap, 0);

        let bitmask_gaps = BitmaskGaps::open(dir, config.clone())?;

        Ok(Self {
            config,
            regions_gaps: bitmask_gaps,
            bitslice: mmap_bitslice,
            path,
        })
    }

    fn bitmask_path(dir: &Path) -> PathBuf {
        dir.join(BITMASK_NAME)
    }

    pub fn flush(&self) -> Result<(), mmap_type::Error> {
        self.bitslice.flusher()()?;
        self.regions_gaps.flush()?;

        Ok(())
    }

    /// Compute the size of the storage in bytes.
    /// Does not include the metadata information (e.g. the regions gaps, bitmask...).
    pub fn get_storage_size_bytes(&self) -> usize {
        let mut size = 0;
        let region_size_blocks = self.config.region_size_blocks;
        let block_size_bytes = self.config.block_size_bytes;
        let region_size_bytes = region_size_blocks * block_size_bytes;
        for (gap_id, gap) in self.regions_gaps.as_slice().iter().enumerate() {
            // skip empty regions
            if gap.is_empty(region_size_blocks as u16) {
                continue;
            }
            // fast path for full regions
            if gap.is_full() {
                size += region_size_bytes;
            } else {
                // compute the size of the occupied blocks for the region
                let gap_offset_start = gap_id * region_size_blocks;
                let gap_offset_end = gap_offset_start + region_size_blocks;
                let occupied_blocks = self.bitslice[gap_offset_start..gap_offset_end].count_ones();
                size += occupied_blocks * block_size_bytes
            }
        }
        size
    }

    pub fn infer_num_pages(&self) -> usize {
        let bits = self.bitslice.len();
        let covered_bytes = bits * self.config.block_size_bytes;
        covered_bytes.div_euclid(self.config.page_size_bytes)
    }

    /// Extend the bitslice to cover another page
    pub fn cover_new_page(&mut self) -> Result<(), String> {
        let extra_length = Self::length_for_page(&self.config);

        // flush outstanding changes
        self.bitslice.flusher()().unwrap();

        // reopen the file with a larger size
        let previous_bitslice_len = self.bitslice.len();
        let new_length = (previous_bitslice_len / u8::BITS as usize) + extra_length;
        create_and_ensure_length(&self.path, new_length).unwrap();
        let mmap = open_write_mmap(&self.path, AdviceSetting::from(DEFAULT_ADVICE), false)
            .map_err(|err| err.to_string())?;

        self.bitslice = MmapBitSlice::try_from(mmap, 0).map_err(|err| err.to_string())?;

        // extend the region gaps
        let current_total_regions = self.regions_gaps.len();
        let expected_total_full_regions = self
            .bitslice
            .len()
            .div_euclid(self.config.region_size_blocks);
        debug_assert!(
            self.bitslice.len() % self.config.region_size_blocks == 0,
            "Bitmask length must be a multiple of region size"
        );
        let new_regions = expected_total_full_regions.saturating_sub(current_total_regions);
        let new_gaps =
            vec![RegionGaps::all_free(self.config.region_size_blocks as u16); new_regions];
        self.regions_gaps.extend(new_gaps.into_iter())?;

        // update the previous last region gaps
        self.update_region_gaps(previous_bitslice_len - 1..previous_bitslice_len + 2);

        assert_eq!(
            self.regions_gaps.len() * self.config.region_size_blocks,
            self.bitslice.len(),
            "Bitmask length mismatch",
        );

        Ok(())
    }

    fn range_of_page(&self, page_id: PageId) -> Range<usize> {
        let page_blocks = self.config.page_size_bytes / self.config.block_size_bytes;
        let start = page_id as usize * page_blocks;
        let end = start + page_blocks;
        start..end
    }

    /// The amount of blocks that have never been used in the page.
    #[cfg(test)]
    pub(crate) fn free_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        self.bitslice[range_of_page].trailing_zeros()
    }

    /// The amount of blocks that are available for reuse in the page.
    #[allow(dead_code)]
    pub(crate) fn fragmented_blocks_for_page(&self, page_id: PageId) -> usize {
        let range_of_page = self.range_of_page(page_id);
        let bitslice = &self.bitslice[range_of_page];

        bitslice.count_zeros() - bitslice.trailing_zeros()
    }

    pub(crate) fn find_available_blocks(&self, num_blocks: u32) -> Option<(PageId, BlockOffset)> {
        let region_id_range = self.regions_gaps.find_fitting_gap(num_blocks)?;
        let regions_start_offset = region_id_range.start as usize * self.config.region_size_blocks;
        let regions_end_offset = region_id_range.end as usize * self.config.region_size_blocks;

        let translate_to_answer = |local_index: u32| {
            let page_size_in_blocks = self.config.page_size_bytes / self.config.block_size_bytes;

            let global_cursor_offset = local_index as usize + regions_start_offset;

            // Calculate the page id and the block offset within the page
            let page_id = global_cursor_offset.div_euclid(page_size_in_blocks);
            let page_block_offset = global_cursor_offset.rem_euclid(page_size_in_blocks);

            (page_id as PageId, page_block_offset as BlockOffset)
        };

        let regions_bitslice = &self.bitslice[regions_start_offset..regions_end_offset];

        Self::find_available_blocks_in_slice(regions_bitslice, num_blocks, translate_to_answer)
    }

    pub fn find_available_blocks_in_slice<F>(
        bitslice: &BitSlice,
        num_blocks: u32,
        translate_local_index: F,
    ) -> Option<(PageId, BlockOffset)>
    where
        F: FnOnce(u32) -> (PageId, BlockOffset),
    {
        // Get raw memory region
        let (head, raw_region, tail) = bitslice
            .domain()
            .region()
            .expect("Regions cover more than one usize");

        // We expect the regions to not use partial usizes
        debug_assert!(head.is_none());
        debug_assert!(tail.is_none());

        let mut current_size: u32 = 0;
        let mut current_start: u32 = 0;
        let mut num_shifts = 0;
        // Iterate over the integers that compose the bitvec. So that we can perform bitwise operations.
        const BITS_IN_CHUNK: u32 = usize::BITS;
        for (chunk_idx, chunk) in raw_region.iter().enumerate() {
            let mut chunk = *chunk;

            // case of all zeros
            if chunk == 0 {
                current_size += BITS_IN_CHUNK;
                continue;
            }

            if chunk == !0 {
                // case of all ones
                if current_size >= num_blocks {
                    // bingo - we found a free cell of num_blocks
                    return Some(translate_local_index(current_start));
                }
                current_size = 0;
                current_start = (chunk_idx as u32 + 1) * BITS_IN_CHUNK;
                continue;
            }

            // At least one non-zero bit
            let leading = chunk.trailing_zeros();
            let trailing = chunk.leading_zeros();

            let max_possible_middle_gap = (BITS_IN_CHUNK - leading - trailing).saturating_sub(2);

            // Skip looking for local max if it won't improve global max
            if num_blocks > max_possible_middle_gap {
                current_size += leading;
                if current_size >= num_blocks {
                    // bingo - we found a free cell of num_blocks
                    return Some(translate_local_index(current_start));
                }
                current_size = trailing;
                current_start = (chunk_idx as u32) * BITS_IN_CHUNK + BITS_IN_CHUNK - trailing;
                continue;
            }

            while chunk != 0 {
                let num_zeros = chunk.trailing_zeros();
                current_size += num_zeros;
                if current_size >= num_blocks {
                    // bingo - we found a free cell of num_blocks
                    return Some(translate_local_index(current_start));
                }

                // shift by the number of zeros
                chunk >>= num_zeros as usize;
                num_shifts += num_zeros;

                // skip consecutive ones
                let num_ones = chunk.trailing_ones();
                if num_ones < BITS_IN_CHUNK {
                    chunk >>= num_ones;
                } else {
                    // all ones
                    debug_assert!(chunk == !0);
                    chunk = 0;
                }
                num_shifts += num_ones;

                current_size = 0;
                current_start = chunk_idx as u32 * BITS_IN_CHUNK + num_shifts;
            }
            // no more ones in the chunk
            current_size += BITS_IN_CHUNK - num_shifts;
            num_shifts = 0;
        }
        if current_size >= num_blocks {
            // bingo - we found a free cell of num_blocks
            return Some(translate_local_index(current_start));
        }

        None
    }

    pub(crate) fn mark_blocks(
        &mut self,
        page_id: PageId,
        block_offset: BlockOffset,
        num_blocks: u32,
        used: bool,
    ) {
        let page_start = self.range_of_page(page_id).start;

        let offset = page_start + block_offset as usize;
        let blocks_range = offset..offset + num_blocks as usize;

        self.bitslice[blocks_range.clone()].fill(used);

        self.update_region_gaps(blocks_range);
    }

    fn update_region_gaps(&mut self, blocks_range: Range<usize>) {
        let region_start_id = blocks_range.start / self.config.region_size_blocks;
        let region_end_id = (blocks_range.end - 1) / self.config.region_size_blocks;

        for region_id in region_start_id..=region_end_id {
            let region_start = region_id * self.config.region_size_blocks;
            let region_end = region_start + self.config.region_size_blocks;

            let bitslice = &self.bitslice[region_start..region_end];

            let gaps = Self::calculate_gaps(bitslice, self.config.region_size_blocks);

            *self.regions_gaps.get_mut(region_id) = gaps;
        }
    }

    pub fn calculate_gaps(region: &BitSlice, region_size_blocks: usize) -> RegionGaps {
        debug_assert_eq!(region.len(), region_size_blocks, "Unexpected region size");
        // Get raw memory region
        let (head, raw_region, tail) = region
            .domain()
            .region()
            .expect("Region covers more than one usize");

        // We expect the region to not use partial usizes
        debug_assert!(head.is_none());
        debug_assert!(tail.is_none());

        // Iterate over the integers that compose the bitslice. So that we can perform bitwise operations.
        let mut max = 0;
        let mut current = 0;
        const BITS_IN_CHUNK: u32 = usize::BITS;
        let mut num_shifts = 0;
        // In reverse, because we expect the regions to be filled start to end.
        // So starting from the end should give us bigger `max` earlier.
        for chunk in raw_region.iter().rev() {
            // Ensure that the chunk is little-endian.
            let mut chunk = chunk.to_le();
            // case of all zeros
            if chunk == 0 {
                current += BITS_IN_CHUNK;
                continue;
            }

            if chunk == !0 {
                // case of all ones
                max = max.max(current);
                current = 0;
                continue;
            }

            // At least one non-zero bit
            let leading = chunk.leading_zeros();
            let trailing = chunk.trailing_zeros();

            let max_possible_middle_gap = (BITS_IN_CHUNK - leading - trailing).saturating_sub(2);

            // Skip looking for local max if it won't improve global max
            if max > max_possible_middle_gap {
                current += leading;
                max = max.max(current);
                current = trailing;
                continue;
            }

            // Otherwise, look for the actual maximum in the chunk
            while chunk != 0 {
                // count consecutive zeros
                let num_zeros = chunk.leading_zeros();
                current += num_zeros;
                max = max.max(current);
                current = 0;

                // shift by the number of zeros
                chunk <<= num_zeros as usize;
                num_shifts += num_zeros;

                // skip consecutive ones
                let num_ones = chunk.leading_ones();
                if num_ones < BITS_IN_CHUNK {
                    chunk <<= num_ones;
                } else {
                    // all ones
                    debug_assert!(chunk == !0);
                    chunk = 0;
                }
                num_shifts += num_ones;
            }

            // no more ones in the chunk
            current += BITS_IN_CHUNK - num_shifts;
            num_shifts = 0;
        }

        max = max.max(current);

        let leading;
        let trailing;
        if max == region_size_blocks as u32 {
            leading = max;
            trailing = max;
        } else {
            leading = raw_region
                .iter()
                .take_while_inclusive(|chunk| chunk == &&0)
                .map(|chunk| chunk.trailing_zeros())
                .sum::<u32>();
            trailing = raw_region
                .iter()
                .rev()
                .take_while_inclusive(|chunk| chunk == &&0)
                .map(|chunk| chunk.leading_zeros())
                .sum::<u32>();
        }

        #[cfg(debug_assertions)]
        {
            RegionGaps::new(
                leading as u16,
                trailing as u16,
                max as u16,
                region_size_blocks as u16,
            )
        }

        #[cfg(not(debug_assertions))]
        {
            RegionGaps::new(leading as u16, trailing as u16, max as u16)
        }
    }
}

#[cfg(test)]
mod tests {

    use bitvec::bits;
    use bitvec::vec::BitVec;
    use proptest::prelude::*;
    use rand::thread_rng;

    use crate::config::{StorageOptions, DEFAULT_BLOCK_SIZE_BYTES, DEFAULT_REGION_SIZE_BLOCKS};

    #[test]
    fn test_length_for_page() {
        let config = &StorageOptions {
            page_size_bytes: Some(8192),
            region_size_blocks: Some(1),
            ..Default::default()
        }
        .try_into()
        .unwrap();
        assert_eq!(super::Bitmask::length_for_page(config), 8);
    }

    #[test]
    fn test_find_available_blocks() {
        let page_size = DEFAULT_BLOCK_SIZE_BYTES * DEFAULT_REGION_SIZE_BLOCKS;

        let blocks_per_page = (page_size / DEFAULT_BLOCK_SIZE_BYTES) as u32;

        let dir = tempfile::tempdir().unwrap();

        let options = StorageOptions {
            page_size_bytes: Some(page_size),
            ..Default::default()
        };

        let mut bitmask = super::Bitmask::create(dir.path(), options.try_into().unwrap()).unwrap();
        bitmask.cover_new_page().unwrap();

        assert_eq!(bitmask.bitslice.len() as u32, blocks_per_page * 2);

        // 1..10
        bitmask.mark_blocks(0, 1, 9, true);

        // 15..20
        bitmask.mark_blocks(0, 15, 5, true);

        // 30..blocks_per_page
        bitmask.mark_blocks(0, 30, blocks_per_page - 30, true);

        // blocks_per_page..blocks_per_page + 1
        bitmask.mark_blocks(1, 0, 1, true);

        let (page_id, block_offset) = bitmask.find_available_blocks(1).unwrap();
        assert_eq!(block_offset, 0);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(2).unwrap();
        assert_eq!(block_offset, 10);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(5).unwrap();
        assert_eq!(block_offset, 10);
        assert_eq!(page_id, 0);

        let (page_id, block_offset) = bitmask.find_available_blocks(6).unwrap();
        assert_eq!(block_offset, 20);
        assert_eq!(page_id, 0);

        // first free block of the next page
        let (page_id, block_offset) = bitmask.find_available_blocks(30).unwrap();
        assert_eq!(block_offset, 1);
        assert_eq!(page_id, 1);

        // not fitting cell
        let found_large = bitmask.find_available_blocks(blocks_per_page);
        assert_eq!(found_large, None);
    }

    #[test]
    fn test_raw_bitvec() {
        use bitvec::prelude::Lsb0;
        let bits = bits![
            0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1
        ];

        let mut bitvec = BitVec::<usize, Lsb0>::new();
        bitvec.extend_from_bitslice(bits);

        assert_eq!(bitvec.len(), 64);

        let raw = bitvec.as_raw_slice();
        assert_eq!(raw.len() as u32, 64 / usize::BITS);

        assert_eq!(raw[0].trailing_zeros(), 4);
        assert_eq!(raw[0].leading_zeros(), 0);
        assert_eq!((raw[0] >> 1).trailing_zeros(), 3)
    }

    prop_compose! {
        /// Creates a fixture bitvec which has gaps of a specific size
        fn regions_bitvec_with_max_gap(max_gap_size: usize) (len in 0..DEFAULT_REGION_SIZE_BLOCKS*4) -> (BitVec, usize) {
            assert!(max_gap_size > 0);
            let len = len.next_multiple_of(DEFAULT_REGION_SIZE_BLOCKS);

            let mut bitvec = BitVec::new();
            bitvec.resize(len, true);

            let mut rng = thread_rng();

            let mut i = 0;
            let mut max_gap = 0;
            while i < len {
                let run = rng.gen_range(1..max_gap_size).min(len - i);
                let skip = rng.gen_range(1..max_gap_size);

                for j in 0..run {
                    bitvec.set(i + j, false);
                }

                if run > max_gap {
                    max_gap = run;
                }

                i += run + skip;
            }

            (bitvec, max_gap)
        }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn test_find_available_blocks_properties((bitvec, max_gap) in regions_bitvec_with_max_gap(120)) {
            let bitslice = bitvec.as_bitslice();

            // Helper to check if a range is all zeros
            let is_free_range = |start: usize, len: usize| {
                let range = start..(start + len);
                bitslice.get(range)
                    .map(|slice| slice.not_any())
                    .unwrap_or(false)
            };

            // For different requested block sizes
            for req_blocks in 1..=max_gap {
                if let Some((_, block_offset)) = super::Bitmask::find_available_blocks_in_slice(
                    bitslice,
                    req_blocks as u32,
                    |idx| (0, idx),
                ) {
                    // The found position should have enough free blocks
                    prop_assert!(is_free_range(block_offset as usize, req_blocks));
                } else {
                    prop_assert!(false, "Should've found a free range")
                }
            }

            // For a block size that doesn't fit
            let req_blocks = max_gap + 1;
            prop_assert!(super::Bitmask::find_available_blocks_in_slice(
                bitslice,
                req_blocks as u32,
                |idx| (0, idx),
            ).is_none());
        }
    }
}
