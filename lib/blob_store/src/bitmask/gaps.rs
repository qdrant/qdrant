use std::ops::Range;
use std::path::{Path, PathBuf};

use itertools::Itertools;
use memory::madvise::{Advice, AdviceSetting};
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use memory::mmap_type::{self, MmapSlice};

use super::{RegionId, StorageConfig};

/// Gaps of contiguous zeros in a bitmask region.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegionGaps {
    pub max: u16,
    pub leading: u16,
    pub trailing: u16,
}

impl RegionGaps {
    pub fn new(
        leading: u16,
        trailing: u16,
        max: u16,
        #[cfg(debug_assertions)] region_size_blocks: u16,
    ) -> Self {
        #[cfg(debug_assertions)]
        {
            let maximum_possible = region_size_blocks;

            assert!(max <= maximum_possible, "Unexpected max gap size");

            assert!(
                leading <= max,
                "Invalid gaps: leading is {leading}, but max is {max}",
            );

            assert!(
                trailing <= max,
                "Invalid gaps: trailing is {trailing}, but max is {max}",
            );

            if leading == maximum_possible || trailing == maximum_possible {
                assert_eq!(leading, trailing);
            }
        }

        Self {
            max,
            leading,
            trailing,
        }
    }

    pub fn all_free(blocks: u16) -> Self {
        Self {
            max: blocks,
            leading: blocks,
            trailing: blocks,
        }
    }

    /// Check if the region is completely empty.
    /// That is a single large gap
    pub fn is_empty(&self, region_size_blocks: u16) -> bool {
        self.max == region_size_blocks
    }

    /// Check if the region is completely full.
    /// That is no gaps in the region.
    pub fn is_full(&self) -> bool {
        self.max == 0
    }
}

/// An overview of contiguous free blocks covered by the bitmask.
#[derive(Debug)]
pub(super) struct BitmaskGaps {
    pub path: PathBuf,
    config: StorageConfig,
    mmap_slice: MmapSlice<RegionGaps>,
}

impl BitmaskGaps {
    fn file_path(dir: &Path) -> PathBuf {
        dir.join("gaps.dat")
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn create(
        dir: &Path,
        mut iter: impl ExactSizeIterator<Item = RegionGaps>,
        config: StorageConfig,
    ) -> Self {
        let path = Self::file_path(dir);

        let length_in_bytes = iter.len() * size_of::<RegionGaps>();
        create_and_ensure_length(&path, length_in_bytes).unwrap();

        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), true).unwrap();
        let mut mmap_slice = unsafe { MmapSlice::from(mmap) };

        debug_assert_eq!(mmap_slice.len(), iter.len());

        mmap_slice.fill_with(|| iter.next().unwrap());

        Self {
            path,
            config,
            mmap_slice,
        }
    }

    pub fn open(dir: &Path, config: StorageConfig) -> Result<Self, String> {
        let path = Self::file_path(dir);
        let mmap = open_write_mmap(&path, AdviceSetting::from(Advice::Normal), false)
            .map_err(|err| err.to_string())?;
        let mmap_slice = unsafe { MmapSlice::try_from(mmap) }.map_err(|err| err.to_string())?;

        Ok(Self {
            path,
            config,
            mmap_slice,
        })
    }

    pub fn flush(&self) -> Result<(), mmap_type::Error> {
        self.mmap_slice.flusher()()
    }

    /// Extends the mmap file to fit the new regions
    pub fn extend(
        &mut self,
        mut iter: impl ExactSizeIterator<Item = RegionGaps>,
    ) -> Result<(), String> {
        if iter.len() == 0 {
            return Ok(());
        }

        // reopen the file with a larger size
        let prev_len = self.mmap_slice.len();
        let new_slice_len = prev_len + iter.len();
        let new_length_in_bytes = new_slice_len * size_of::<RegionGaps>();

        create_and_ensure_length(&self.path, new_length_in_bytes).unwrap();

        let mmap = open_write_mmap(&self.path, AdviceSetting::from(Advice::Normal), false)
            .map_err(|err| err.to_string())?;

        self.mmap_slice = unsafe { MmapSlice::try_from(mmap) }.map_err(|err| err.to_string())?;

        debug_assert_eq!(self.mmap_slice[prev_len..].len(), iter.len());

        self.mmap_slice[prev_len..].fill_with(|| iter.next().unwrap());

        Ok(())
    }

    pub fn trailing_free_blocks(&self) -> u32 {
        self.mmap_slice
            .iter()
            .rev()
            .take_while_inclusive(|gap| gap.trailing == self.config.region_size_blocks as u16)
            .map(|gap| u32::from(gap.trailing))
            .sum()
    }

    pub fn len(&self) -> usize {
        self.mmap_slice.len()
    }

    pub fn get(&self, idx: usize) -> Option<&RegionGaps> {
        self.mmap_slice.get(idx)
    }

    pub fn get_mut(&mut self, idx: usize) -> &mut RegionGaps {
        &mut self.mmap_slice[idx]
    }

    pub fn as_slice(&self) -> &[RegionGaps] {
        &self.mmap_slice
    }

    /// Find a gap in the bitmask that is large enough to fit `num_blocks` blocks.
    /// Returns the region id of the gap.
    /// In case of boundary gaps, returns the region id of the left gap.
    pub fn find_fitting_gap(&self, num_blocks: u32) -> Option<Range<RegionId>> {
        let regions_needed = num_blocks.div_ceil(self.config.region_size_blocks as u32) as usize;

        // if the number of regions needed is larger than the number of regions available, return None
        if regions_needed > self.mmap_slice.len() {
            return None;
        }

        // if there is only one region, check if it is large enough
        if self.mmap_slice.len() == 1 {
            return if self.get(0).unwrap().max as usize >= num_blocks as usize {
                Some(0..1)
            } else {
                None
            };
        }

        // double pointer traversal to find a fitting range
        let mut start = 0;
        let mut end = 1;
        let mut accumulated_blocks = 0;

        loop {
            let start_gap = self.get(start)?;

            // if the gap is large enough, return it
            if start_gap.max as usize >= num_blocks as usize {
                return Some(start as RegionId..start as RegionId + 1);
            }

            // we need to accumulate with the next gap(s)
            accumulated_blocks += start_gap.trailing as usize;

            // explore following gaps up to `end` pointer
            loop {
                if accumulated_blocks >= num_blocks as usize {
                    return Some(start as RegionId..end as RegionId);
                }

                let Some(end_gap) = self.get(end) else {
                    break;
                };

                if end_gap.max == self.config.region_size_blocks as u16 {
                    // the block is empty - accumulate it and advance the end pointer
                    accumulated_blocks += end_gap.max as usize;
                    end += 1;
                } else {
                    // the block is not empty - check if the leading blocks are enough
                    if accumulated_blocks + end_gap.leading as usize >= num_blocks as usize {
                        return Some(start as RegionId..end as RegionId);
                    } else {
                        break;
                    }
                }
            }

            // reset and advance
            accumulated_blocks = 0;
            start += 1;
            end = start + 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tempfile::tempdir;

    use super::*;
    use crate::config::{StorageOptions, DEFAULT_REGION_SIZE_BLOCKS};

    prop_compose! {
        fn arbitrary_region_gaps(region_size_blocks: u16)(
            leading in 0..=region_size_blocks,
            trailing in 0..=region_size_blocks,
            max in 0..=region_size_blocks,
        ) -> RegionGaps {
            if leading + trailing >= region_size_blocks {
                return RegionGaps::all_free(region_size_blocks);
            }

            let in_between = region_size_blocks - leading - trailing;

            let max = max.min(in_between.saturating_sub(2)).max(leading).max(trailing);

            RegionGaps::new(leading, trailing, max, region_size_blocks)
        }
    }

    impl Arbitrary for RegionGaps {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            arbitrary_region_gaps(DEFAULT_REGION_SIZE_BLOCKS as u16).boxed()
        }
    }

    fn regions_gaps_to_bitvec(
        gaps: &[RegionGaps],
        region_size_blocks: usize,
    ) -> bitvec::vec::BitVec {
        let total_bits = gaps.len() * region_size_blocks;
        let mut bv = bitvec::vec::BitVec::repeat(true, total_bits);

        for (region_idx, gap) in gaps.iter().enumerate() {
            let region_start = region_idx * region_size_blocks;

            // Handle leading zeros
            if gap.leading > 0 {
                for i in 0..gap.leading as usize {
                    bv.set(region_start + i, false);
                }
            }

            // Handle trailing zeros
            if gap.trailing > 0 {
                let trailing_start = region_start + region_size_blocks - gap.trailing as usize;
                for i in 0..gap.trailing as usize {
                    bv.set(trailing_start + i, false);
                }
            }

            // Handle max zeros if bigger than both leading and trailing
            if gap.max > gap.leading && gap.max > gap.trailing {
                // start after leading, but leave one bit in between to create a separate gap
                let zeros_start = region_start + gap.leading as usize + 1;
                let zeros_end = zeros_start + gap.max as usize;

                // Put remaining zeros in middle
                for i in zeros_start..zeros_end {
                    bv.set(i, false);
                }
            }
        }

        bv
    }

    proptest! {
        #[test]
        fn test_find_fitting_gap(
            gaps in prop::collection::vec(any::<RegionGaps>(), 1..100),
            num_blocks in 1..=(DEFAULT_REGION_SIZE_BLOCKS as u32 * 3)
        ) {
            let temp_dir = tempdir().unwrap();
            let config = StorageOptions::default().try_into().unwrap();
            let bitmask_gaps = BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config);

            let bitvec = regions_gaps_to_bitvec(&gaps, DEFAULT_REGION_SIZE_BLOCKS);

            if let Some(range) = bitmask_gaps.find_fitting_gap(num_blocks) {
                // Range should be within bounds
                prop_assert!(range.start <= bitmask_gaps.len() as u32);
                prop_assert!(range.end <= bitmask_gaps.len() as u32);
                prop_assert!(range.start <= range.end);

                // check that range is as constrained as possible
                let total_regions = range.end - range.start;
                let max_needed_regions = num_blocks.div_ceil(DEFAULT_REGION_SIZE_BLOCKS as u32) + 1;
                prop_assert!(total_regions <= max_needed_regions);

                // Range should actually have a gap with enough blocks
                let regions_start = range.start as usize * DEFAULT_REGION_SIZE_BLOCKS;
                let regions_end = range.end as usize * DEFAULT_REGION_SIZE_BLOCKS;
                let max_gap = bitvec[regions_start..regions_end].iter().chunk_by(|b| **b).into_iter()
                    .filter(|(used, _group)| !*used)
                    .map(|(_, group)| group.count() as u32)
                    .max()
                    .unwrap_or(0);

                // Verify the gap is large enough
                prop_assert!(max_gap >= num_blocks, "max_gap: {}, num_blocks: {}", max_gap, num_blocks);
            }
        }
    }

    #[test]
    fn test_find_fitting_gap_large() {
        let large_value_blocks = DEFAULT_REGION_SIZE_BLOCKS + 20;

        let gaps = [
            RegionGaps {
                max: 0,
                leading: 0,
                trailing: 0,
            },
            RegionGaps {
                max: 500,
                leading: 0,
                trailing: 500,
            },
            RegionGaps::all_free(DEFAULT_REGION_SIZE_BLOCKS as u16),
        ];

        let temp_dir = tempdir().unwrap();
        let config = StorageOptions::default().try_into().unwrap();
        let mut bitmask_gaps =
            BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config);
        bitmask_gaps.mmap_slice[0..3].clone_from_slice(&gaps[..]);

        assert!(bitmask_gaps
            .find_fitting_gap(large_value_blocks as u32)
            .is_some());
    }

    #[test]
    fn test_region_gaps_persistence() {
        use std::fs;

        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let dir_path = dir.path();

        let region_size_blocks = DEFAULT_REGION_SIZE_BLOCKS as u16;

        let gaps = vec![
            RegionGaps::new(1, 2, 3, region_size_blocks),
            RegionGaps::new(4, 5, 6, region_size_blocks),
            RegionGaps::new(7, 8, 9, region_size_blocks),
        ];

        // Create RegionGaps and write gaps
        {
            let config = StorageOptions::default().try_into().unwrap();
            let region_gaps = BitmaskGaps::create(dir_path, gaps.clone().into_iter(), config);
            assert_eq!(region_gaps.len(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), gap);
            }
        }

        // Reopen RegionGaps and verify gaps
        {
            let config = StorageOptions::default().try_into().unwrap();
            let region_gaps = BitmaskGaps::open(dir_path, config).unwrap();
            assert_eq!(region_gaps.len(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), gap);
            }
        }

        // Extend RegionGaps with more gaps
        let more_gaps = vec![
            RegionGaps::new(10, 11, 12, region_size_blocks),
            RegionGaps::new(13, 14, 15, region_size_blocks),
        ];

        {
            let config = StorageOptions::default().try_into().unwrap();
            let mut region_gaps = BitmaskGaps::open(dir_path, config).unwrap();
            region_gaps.extend(more_gaps.clone().into_iter()).unwrap();
            assert_eq!(region_gaps.len(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), gap);
            }
        }

        // Reopen RegionGaps and verify all gaps
        {
            let config = StorageOptions::default().try_into().unwrap();
            let region_gaps = BitmaskGaps::open(dir_path, config).unwrap();
            assert_eq!(region_gaps.len(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), gap);
            }
        }

        // Clean up
        fs::remove_file(BitmaskGaps::file_path(dir_path)).unwrap();
    }
}
