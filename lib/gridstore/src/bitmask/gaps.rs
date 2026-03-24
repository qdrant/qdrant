use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use common::mmap::{Advice, AdviceSetting, create_and_ensure_length};
use common::universal_io::{Flusher, OpenOptions, UniversalWrite};
use itertools::Itertools;

use super::{RegionId, StorageConfig};
use crate::Result;

/// Gaps of contiguous zeros in a bitmask region.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(C)]
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

fn gaps_file_path(dir: &Path) -> PathBuf {
    dir.join("gaps.dat")
}

/// An overview of contiguous free blocks covered by the bitmask.
#[derive(Debug)]
pub(super) struct BitmaskGaps<S> {
    path: PathBuf,
    config: StorageConfig,
    slice_store: S,
}

impl<S: UniversalWrite<RegionGaps>> BitmaskGaps<S> {
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn create(
        dir: &Path,
        iter: impl ExactSizeIterator<Item = RegionGaps>,
        config: StorageConfig,
    ) -> Result<Self> {
        let path = gaps_file_path(dir);

        let data: Vec<RegionGaps> = iter.collect();
        let length_in_bytes = data.len() * size_of::<RegionGaps>();
        create_and_ensure_length(&path, length_in_bytes)?;

        let options = OpenOptions {
            need_sequential: false,
            disk_parallel: None,
            populate: Some(true),
            advice: None,
        };
        let mut slice_store = S::open(&path, options)?;

        debug_assert_eq!(slice_store.len()? as usize, data.len());

        slice_store.write(0, &data)?;

        Ok(Self {
            path,
            config,
            slice_store,
        })
    }

    pub fn open(dir: &Path, config: StorageConfig) -> Result<Self> {
        let path = gaps_file_path(dir);
        let options = OpenOptions {
            need_sequential: false,
            disk_parallel: None,
            populate: Some(false),
            advice: Some(AdviceSetting::Advice(Advice::Normal)),
        };
        let slice_store = S::open(&path, options)?;

        Ok(Self {
            path,
            config,
            slice_store,
        })
    }

    pub fn flusher(&self) -> Flusher {
        self.slice_store.flusher()
    }

    /// Extends the file to fit the new regions
    pub fn extend(&mut self, iter: impl ExactSizeIterator<Item = RegionGaps>) -> Result<()> {
        let data: Vec<RegionGaps> = iter.collect();
        if data.is_empty() {
            return Ok(());
        }

        // reopen the file with a larger size
        let prev_len = self.len()?;
        let new_slice_len = prev_len + data.len();
        let new_length_in_bytes = new_slice_len * size_of::<RegionGaps>();

        create_and_ensure_length(&self.path, new_length_in_bytes)?;

        let options = OpenOptions {
            need_sequential: false,
            disk_parallel: None,
            populate: Some(false),
            advice: Some(AdviceSetting::Advice(Advice::Normal)),
        };
        self.slice_store = S::open(&self.path, options)?;

        debug_assert_eq!(self.len()? - prev_len, data.len());

        let byte_offset = (prev_len * size_of::<RegionGaps>()) as u64;
        self.slice_store.write(byte_offset, &data)?;

        Ok(())
    }

    pub fn trailing_free_blocks(&self) -> Result<u32> {
        let slice = self.as_slice()?;
        Ok(slice
            .iter()
            .rev()
            .take_while_inclusive(|gap| gap.trailing == self.config.region_size_blocks as u16)
            .map(|gap| u32::from(gap.trailing))
            .sum())
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.slice_store.len()? as usize)
    }

    #[cfg(test)]
    pub fn get(&self, idx: usize) -> Result<Option<RegionGaps>> {
        let slice = self.as_slice()?;
        Ok(slice.get(idx).copied())
    }

    pub fn set(&mut self, idx: usize, value: RegionGaps) -> Result<()> {
        let byte_offset = (idx * size_of::<RegionGaps>()) as u64;
        self.slice_store.write(byte_offset, &[value])?;
        Ok(())
    }

    pub fn as_slice(&self) -> Result<Cow<'_, [RegionGaps]>> {
        Ok(self.slice_store.read_whole()?)
    }

    /// Find a gap in the bitmask that is large enough to fit `num_blocks` blocks.
    /// Returns the range of regions where the gap is.
    pub fn find_fitting_gap(&self, num_blocks: u32) -> Result<Option<Range<RegionId>>> {
        let slice = self.as_slice()?;

        if slice.len() == 1 {
            return Ok(if slice[0].max as usize >= num_blocks as usize {
                Some(0..1)
            } else {
                None
            });
        }

        // try to find gap in the minimum regions needed
        let regions_needed = num_blocks.div_ceil(self.config.region_size_blocks as u32) as usize;

        let fits_in_min_regions = match regions_needed {
            0 => unreachable!("num_blocks should be at least 1"),
            // we might not need to merge any regions, just check the `max` field
            1 => slice.iter().enumerate().find_map(|(region_id, gap)| {
                if gap.max as usize >= num_blocks as usize {
                    Some(region_id as RegionId..(region_id + 1) as RegionId)
                } else {
                    None
                }
            }),
            // we need to merge at least 2 regions
            window_size => self.find_merged_gap(&slice, window_size, num_blocks),
        };

        if fits_in_min_regions.is_some() {
            return Ok(fits_in_min_regions);
        }

        // try to find gap by merging one more region (which is the maximum regions we may need for the value)
        let window_size = regions_needed + 1;

        Ok(self.find_merged_gap(&slice, window_size, num_blocks))
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> Result<()> {
        self.slice_store.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> Result<()> {
        self.slice_store.clear_ram_cache()?;
        Ok(())
    }

    /// Find a gap in the bitmask that is large enough to fit `num_blocks` blocks, in a merged window of regions.
    fn find_merged_gap(
        &self,
        slice: &[RegionGaps],
        window_size: usize,
        num_blocks: u32,
    ) -> Option<Range<RegionId>> {
        debug_assert!(window_size >= 2, "window size must be at least 2");

        slice
            .windows(window_size)
            .enumerate()
            .find_map(|(start_region_id, gaps)| {
                // make sure the middle regions are all free
                let middle_regions = &gaps[1..window_size - 1];
                if middle_regions
                    .iter()
                    .any(|gap| gap.max as usize != self.config.region_size_blocks)
                {
                    return None;
                }
                let first_trailing = gaps[0].trailing;
                let last_leading = gaps[window_size - 1].leading;
                let merged_gap = (first_trailing + last_leading) as usize
                    + (window_size - 2) * self.config.region_size_blocks;

                if merged_gap as u32 >= num_blocks {
                    Some(
                        start_region_id as RegionId..(start_region_id + window_size) as RegionId,
                    )
                } else {
                    None
                }
            })
    }
}

#[cfg(test)]
#[cfg(debug_assertions)]
mod tests {
    use common::universal_io::MmapUniversalRw;
    use proptest::prelude::*;
    use tempfile::tempdir;

    use super::*;
    use crate::config::{DEFAULT_REGION_SIZE_BLOCKS, StorageOptions};

    pub type MmapBitmaskGaps = BitmaskGaps<MmapUniversalRw<RegionGaps>>;

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
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]
        #[test]
        fn test_find_fitting_gap(
            gaps in prop::collection::vec(any::<RegionGaps>(), 1..50),
            num_blocks in 1..=(DEFAULT_REGION_SIZE_BLOCKS as u32 * 2)
        ) {
            let temp_dir = tempdir().unwrap();
            let config = StorageOptions::default().try_into().unwrap();
            let bitmask_gaps: MmapBitmaskGaps = BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config).unwrap();

            let bitvec = regions_gaps_to_bitvec(&gaps, DEFAULT_REGION_SIZE_BLOCKS);

            if let Some(range) = bitmask_gaps.find_fitting_gap(num_blocks).unwrap() {
                // Range should be within bounds
                prop_assert!(range.start <= bitmask_gaps.len().unwrap() as u32);
                prop_assert!(range.end <= bitmask_gaps.len().unwrap() as u32);
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

    /// Tests that it is possible to find a large gap in the end of the gaps list
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
        let bitmask_gaps: MmapBitmaskGaps =
            BitmaskGaps::create(temp_dir.path(), gaps.into_iter(), config).unwrap();
        assert!(bitmask_gaps.len().unwrap() >= 3);

        assert!(
            bitmask_gaps
                .find_fitting_gap(large_value_blocks as u32)
                .unwrap()
                .is_some(),
        );
    }

    #[test]
    fn test_find_fitting_gap_windows_end() {
        const REGION_SIZE_BLOCKS: u32 = DEFAULT_REGION_SIZE_BLOCKS as u32;

        let temp_dir = tempdir().unwrap();
        let config: StorageConfig = StorageOptions::default().try_into().unwrap();

        // 3 regions, all empty
        let gaps = vec![
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
        ];
        let bitmask_gaps: MmapBitmaskGaps =
            BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config.clone()).unwrap();

        // Find space for blocks covering up to 2 regions
        assert!(bitmask_gaps.find_fitting_gap(1).unwrap().is_some());
        assert!(bitmask_gaps.find_fitting_gap(REGION_SIZE_BLOCKS).unwrap().is_some());
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 2)
                .unwrap()
                .is_some(),
        );

        // Find space for blocks covering 3 regions
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 2 + 1)
                .unwrap()
                .is_some(),
        );
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 3)
                .unwrap()
                .is_some(),
        );

        // No space for blocks covering 4 or more regions
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 4)
                .unwrap()
                .is_none(),
        );

        // 3 regions with first 0.5 regions occupied and last 2.5 regions available
        let gaps = vec![
            RegionGaps {
                max: (REGION_SIZE_BLOCKS / 2) as u16,
                leading: 0,
                trailing: (REGION_SIZE_BLOCKS / 2) as u16,
            },
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
        ];
        let bitmask_gaps: MmapBitmaskGaps =
            BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config.clone()).unwrap();

        // Find space for blocks covering up to 2 regions
        assert!(bitmask_gaps.find_fitting_gap(REGION_SIZE_BLOCKS).unwrap().is_some());
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 2)
                .unwrap()
                .is_some(),
        );

        // Find space for blocks covering more than 2 up to 2.5 regions
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS * 2 + 1)
                .unwrap()
                .is_some(),
        );
        assert!(
            bitmask_gaps
                .find_fitting_gap((REGION_SIZE_BLOCKS * 2) + (REGION_SIZE_BLOCKS / 2))
                .unwrap()
                .is_some(),
        );

        // No space for blocks covering more than 2.5 regions
        assert!(
            bitmask_gaps
                .find_fitting_gap((REGION_SIZE_BLOCKS * 2) + (REGION_SIZE_BLOCKS / 2) + 1)
                .unwrap()
                .is_none(),
        );

        // 3 regions with first 1.5 regions occupied and last 1.5 regions available
        let gaps = vec![
            RegionGaps {
                max: 0,
                leading: 0,
                trailing: 0,
            },
            RegionGaps {
                max: (REGION_SIZE_BLOCKS / 2) as u16,
                leading: 0,
                trailing: (REGION_SIZE_BLOCKS / 2) as u16,
            },
            RegionGaps::all_free(REGION_SIZE_BLOCKS as u16),
        ];
        let bitmask_gaps: MmapBitmaskGaps =
            BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config).unwrap();

        // Find space for blocks covering more than 1 to 1.5 regions
        assert!(bitmask_gaps.find_fitting_gap(REGION_SIZE_BLOCKS).unwrap().is_some());
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + 1)
                .unwrap()
                .is_some(),
        );
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + (REGION_SIZE_BLOCKS / 2))
                .unwrap()
                .is_some(),
        );

        // No space for blocks covering more than 1.5 regions
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + REGION_SIZE_BLOCKS / 2 + 1)
                .unwrap()
                .is_none(),
        );
    }

    #[test]
    fn test_find_fitting_gap_windows_middle() {
        const REGION_SIZE_BLOCKS: u32 = DEFAULT_REGION_SIZE_BLOCKS as u32;

        let temp_dir = tempdir().unwrap();
        let config = StorageOptions::default().try_into().unwrap();

        // 3 regions with 1.5 regions occupied and 1.5 regions available
        let gaps = vec![
            // First region: occupied
            RegionGaps {
                max: 0,
                leading: 0,
                trailing: 0,
            },
            // Second region: first 25% is occupied
            RegionGaps {
                max: (REGION_SIZE_BLOCKS / 4) as u16 * 3,
                leading: 0,
                trailing: (REGION_SIZE_BLOCKS / 4) as u16 * 3,
            },
            // Third region: last 25% is occupied
            RegionGaps {
                max: (REGION_SIZE_BLOCKS / 4) as u16 * 3,
                leading: (REGION_SIZE_BLOCKS / 4) as u16 * 3,
                trailing: 0,
            },
        ];
        let bitmask_gaps: MmapBitmaskGaps =
            BitmaskGaps::create(temp_dir.path(), gaps.clone().into_iter(), config).unwrap();

        // Find space for blocks covering up to 1.5 region
        assert!(bitmask_gaps.find_fitting_gap(REGION_SIZE_BLOCKS).unwrap().is_some());
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + 1)
                .unwrap()
                .is_some(),
        );
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + REGION_SIZE_BLOCKS / 2)
                .unwrap()
                .is_some(),
        );

        // No space for blocks covering more than 1.5 regions
        assert!(
            bitmask_gaps
                .find_fitting_gap(REGION_SIZE_BLOCKS + REGION_SIZE_BLOCKS / 2 + 1)
                .unwrap()
                .is_none(),
        );
    }

    #[test]
    fn test_region_gaps_persistence() {
        use fs_err as fs;
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
            let region_gaps: MmapBitmaskGaps =
                BitmaskGaps::create(dir_path, gaps.clone().into_iter(), config).unwrap();
            assert_eq!(region_gaps.len().unwrap(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), Some(*gap));
            }
        }

        // Reopen RegionGaps and verify gaps
        {
            let config = StorageOptions::default().try_into().unwrap();
            let region_gaps: MmapBitmaskGaps = BitmaskGaps::open(dir_path, config).unwrap();
            assert_eq!(region_gaps.len().unwrap(), gaps.len());
            for (i, gap) in gaps.iter().enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), Some(*gap));
            }
        }

        // Extend RegionGaps with more gaps
        let more_gaps = vec![
            RegionGaps::new(10, 11, 12, region_size_blocks),
            RegionGaps::new(13, 14, 15, region_size_blocks),
        ];

        {
            let config = StorageOptions::default().try_into().unwrap();
            let mut region_gaps: MmapBitmaskGaps = BitmaskGaps::open(dir_path, config).unwrap();
            region_gaps.extend(more_gaps.clone().into_iter()).unwrap();
            assert_eq!(region_gaps.len().unwrap(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), Some(*gap));
            }
        }

        // Reopen RegionGaps and verify all gaps
        {
            let config = StorageOptions::default().try_into().unwrap();
            let region_gaps: MmapBitmaskGaps = BitmaskGaps::open(dir_path, config).unwrap();
            assert_eq!(region_gaps.len().unwrap(), gaps.len() + more_gaps.len());
            for (i, gap) in gaps.iter().chain(more_gaps.iter()).enumerate() {
                assert_eq!(region_gaps.get(i).unwrap(), Some(*gap));
            }
        }

        // Clean up
        fs::remove_file(gaps_file_path(dir_path)).unwrap();
    }
}
