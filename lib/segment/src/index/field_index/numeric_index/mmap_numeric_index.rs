use std::fs::{create_dir_all, remove_dir};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use memmap2::MmapMut;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{self, create_and_ensure_length};
use memory::mmap_type::{MmapBitSlice, MmapSlice};
use serde::{Deserialize, Serialize};

use super::mutable_numeric_index::InMemoryNumericIndex;
use super::Encodable;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::mmap_point_to_values::{MmapPointToValues, MmapValue};

const PAIRS_PATH: &str = "data.bin";
const DELETED_PATH: &str = "deleted.bin";
const CONFIG_PATH: &str = "mmap_field_index_config.json";

pub struct MmapNumericIndex<T: Encodable + Numericable + Default + MmapValue + 'static> {
    path: PathBuf,
    deleted: MmapBitSliceBufferedUpdateWrapper,
    // sorted pairs (id + value), sorted by value (by id if values are equal)
    pairs: MmapSlice<Point<T>>,
    histogram: Histogram<T>,
    deleted_count: usize,
    max_values_per_point: usize,
    point_to_values: MmapPointToValues<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapNumericIndexConfig {
    max_values_per_point: usize,
}

pub(super) struct NumericIndexPairsIterator<'a, T: Encodable + Numericable> {
    pairs: &'a [Point<T>],
    deleted: &'a MmapBitSliceBufferedUpdateWrapper,
    start_index: usize,
    end_index: usize,
}

impl<T: Encodable + Numericable> Iterator for NumericIndexPairsIterator<'_, T> {
    type Item = Point<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.pairs[self.start_index].clone();
            let deleted = self.deleted.get(key.idx as usize).unwrap_or(true);
            self.start_index += 1;
            if deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<T: Encodable + Numericable> DoubleEndedIterator for NumericIndexPairsIterator<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.start_index < self.end_index {
            let key = self.pairs[self.end_index - 1].clone();
            let deleted = self.deleted.get(key.idx as usize).unwrap_or(true);
            self.end_index -= 1;
            if deleted {
                continue;
            }
            return Some(key);
        }
        None
    }
}

impl<T: Encodable + Numericable + Default + MmapValue> MmapNumericIndex<T> {
    pub fn build(in_memory_index: InMemoryNumericIndex<T>, path: &Path) -> OperationResult<Self> {
        create_dir_all(path)?;

        let pairs_path = path.join(PAIRS_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        atomic_save_json(
            &config_path,
            &MmapNumericIndexConfig {
                max_values_per_point: in_memory_index.max_values_per_point,
            },
        )?;

        in_memory_index.histogram.save(path)?;

        MmapPointToValues::<T>::from_iter(
            path,
            in_memory_index
                .point_to_values
                .iter()
                .enumerate()
                .map(|(idx, values)| {
                    (
                        idx as PointOffsetType,
                        values.iter().map(|v| T::as_referenced(v)),
                    )
                }),
        )?;

        {
            let pairs_file = create_and_ensure_length(
                &pairs_path,
                in_memory_index.map.len() * std::mem::size_of::<Point<T>>(),
            )?;
            let pairs_mmap = unsafe { MmapMut::map_mut(&pairs_file)? };
            let mut pairs = unsafe { MmapSlice::<Point<T>>::try_from(pairs_mmap)? };
            for (src, dst) in in_memory_index.map.iter().zip(pairs.iter_mut()) {
                *dst = src.clone();
            }
        }

        {
            const BITS_IN_BYTE: usize = 8;
            let deleted_flags_count = in_memory_index.point_to_values.len();
            let deleted_file = create_and_ensure_length(
                &deleted_path,
                BITS_IN_BYTE
                    * BITS_IN_BYTE
                    * deleted_flags_count.div_ceil(BITS_IN_BYTE * BITS_IN_BYTE),
            )?;
            let mut deleted_mmap = unsafe { MmapMut::map_mut(&deleted_file)? };
            deleted_mmap.fill(0);
            let mut deleted_bitflags = MmapBitSlice::from(deleted_mmap, 0);
            for (idx, values) in in_memory_index.point_to_values.iter().enumerate() {
                if values.is_empty() {
                    deleted_bitflags.set(idx, true);
                }
            }
        }

        Self::load(path)
    }

    pub fn load(path: &Path) -> OperationResult<Self> {
        let pairs_path = path.join(PAIRS_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        let histogram = Histogram::<T>::load(path)?;
        let config: MmapNumericIndexConfig = read_json(&config_path)?;
        let deleted = mmap_ops::open_write_mmap(&deleted_path, AdviceSetting::Global, false)?;
        let deleted = MmapBitSlice::from(deleted, 0);
        let deleted_count = deleted.count_ones();
        let map = unsafe {
            MmapSlice::try_from(mmap_ops::open_write_mmap(
                &pairs_path,
                AdviceSetting::Global,
                false,
            )?)?
        };
        let point_to_values = MmapPointToValues::open(path)?;

        Ok(Self {
            pairs: map,
            deleted: MmapBitSliceBufferedUpdateWrapper::new(deleted),
            path: path.to_path_buf(),
            histogram,
            deleted_count,
            max_values_per_point: config.max_values_per_point,
            point_to_values,
        })
    }

    pub fn clear(self) -> OperationResult<()> {
        let files = self.files();
        let Self { path, .. } = self;
        for file in files {
            std::fs::remove_file(file)?;
        }
        let _ = remove_dir(path);
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(PAIRS_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.point_to_values.files());
        files.extend(Histogram::<T>::files(&self.path));
        files
    }

    pub fn flusher(&self) -> Flusher {
        self.deleted.flusher()
    }

    pub(super) fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
    ) -> bool {
        if self.deleted.get(idx as usize) == Some(false) {
            self.point_to_values
                .check_values_any(idx, |v| check_fn(T::from_referenced(&v)))
        } else {
            false
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        if self.deleted.get(idx as usize) == Some(false) {
            Some(Box::new(
                self.point_to_values
                    .get_values(idx)?
                    .map(|v| *T::from_referenced(&v)),
            ))
        } else {
            None
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        if self.deleted.get(idx as usize) == Some(false) {
            self.point_to_values.get_values_count(idx)
        } else {
            None
        }
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    pub(super) fn total_unique_values_count(&self) -> usize {
        self.pairs.len()
    }

    pub(super) fn values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.values_range_iterator(start_bound, end_bound)
            .map(|Point { idx, .. }| idx)
    }

    pub(super) fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_ {
        self.values_range_iterator(start_bound, end_bound)
            .map(|Point { val, idx }| (val, idx))
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if idx < self.deleted.len() && !self.deleted.get(idx).unwrap_or(true) {
            self.deleted.set(idx, true);
            self.deleted_count += 1;
        }
    }

    pub(super) fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub(super) fn get_points_count(&self) -> usize {
        self.point_to_values.len() - self.deleted_count
    }

    pub(super) fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    pub(super) fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> usize {
        let iterator = self.values_range_iterator(start_bound, end_bound);
        iterator.end_index - iterator.start_index
    }

    // get iterator
    fn values_range_iterator(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> NumericIndexPairsIterator<'_, T> {
        let start_index = match start_bound {
            Bound::Included(bound) => self.pairs.binary_search(&bound).unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => match self.pairs.binary_search(&bound) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        };

        if start_index >= self.pairs.len() {
            return NumericIndexPairsIterator {
                pairs: &self.pairs,
                deleted: &self.deleted,
                start_index: self.pairs.len(),
                end_index: self.pairs.len(),
            };
        }

        let end_index = match end_bound {
            Bound::Included(bound) => match self.pairs[start_index..].binary_search(&bound) {
                Ok(idx) => idx + 1 + start_index,
                Err(idx) => idx + start_index,
            },
            Bound::Excluded(bound) => {
                let end_bound = self.pairs[start_index..].binary_search(&bound);
                end_bound.unwrap_or_else(|idx| idx) + start_index
            }
            Bound::Unbounded => self.pairs.len(),
        };

        NumericIndexPairsIterator {
            pairs: &self.pairs,
            deleted: &self.deleted,
            start_index,
            end_index,
        }
    }
}
