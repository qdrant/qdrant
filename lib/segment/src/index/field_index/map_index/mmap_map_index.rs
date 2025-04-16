use std::borrow::Borrow;
use std::fs::{create_dir_all, remove_dir};
use std::iter;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use ahash::HashMap;
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::mmap_hashmap::{Key, MmapHashMap, READ_ENTRY_OVERHEAD};
use common::types::PointOffsetType;
use io::file_operations::{atomic_save_json, read_json};
use itertools::Itertools;
use memmap2::MmapMut;
use memory::fadvise::clear_disk_cache;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{self, create_and_ensure_length};
use memory::mmap_type::MmapBitSlice;
use serde::{Deserialize, Serialize};

use super::{IdIter, MapIndexKey};
use crate::common::Flusher;
use crate::common::mmap_bitslice_buffered_update_wrapper::MmapBitSliceBufferedUpdateWrapper;
use crate::common::operation_error::OperationResult;
use crate::index::field_index::mmap_point_to_values::MmapPointToValues;

const DELETED_PATH: &str = "deleted.bin";
const HASHMAP_PATH: &str = "values_to_points.bin";
const CONFIG_PATH: &str = "mmap_field_index_config.json";

pub struct MmapMapIndex<N: MapIndexKey + Key + ?Sized> {
    path: PathBuf,
    value_to_points: MmapHashMap<N, PointOffsetType>,
    point_to_values: MmapPointToValues<N>,
    deleted: MmapBitSliceBufferedUpdateWrapper,
    deleted_count: usize,
    total_key_value_pairs: usize,
    is_on_disk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapMapIndexConfig {
    total_key_value_pairs: usize,
}

impl<N: MapIndexKey + Key + ?Sized> MmapMapIndex<N> {
    pub fn load(path: &Path, is_on_disk: bool) -> OperationResult<Self> {
        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        let config: MmapMapIndexConfig = read_json(&config_path)?;

        let do_populate = !is_on_disk;

        let hashmap = MmapHashMap::open(&hashmap_path, do_populate)?;
        let point_to_values = MmapPointToValues::open(path, do_populate)?;

        let deleted = mmap_ops::open_write_mmap(&deleted_path, AdviceSetting::Global, do_populate)?;
        let deleted = MmapBitSlice::from(deleted, 0);
        let deleted_count = deleted.count_ones();

        Ok(Self {
            path: path.to_path_buf(),
            value_to_points: hashmap,
            point_to_values,
            deleted: MmapBitSliceBufferedUpdateWrapper::new(deleted),
            deleted_count,
            total_key_value_pairs: config.total_key_value_pairs,
            is_on_disk,
        })
    }

    pub fn build(
        path: &Path,
        point_to_values: Vec<Vec<N::Owned>>,
        values_to_points: HashMap<N::Owned, Vec<PointOffsetType>>,
        is_on_disk: bool,
    ) -> OperationResult<Self> {
        create_dir_all(path)?;

        let hashmap_path = path.join(HASHMAP_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        atomic_save_json(
            &config_path,
            &MmapMapIndexConfig {
                total_key_value_pairs: point_to_values.iter().map(|v| v.len()).sum(),
            },
        )?;

        MmapHashMap::create(
            &hashmap_path,
            values_to_points
                .iter()
                .map(|(value, ids)| (value.borrow(), ids.iter().copied())),
        )?;

        MmapPointToValues::<N>::from_iter(
            path,
            point_to_values.iter().enumerate().map(|(idx, values)| {
                (
                    idx as PointOffsetType,
                    values.iter().map(|value| N::as_referenced(value.borrow())),
                )
            }),
        )?;

        {
            let deleted_flags_count = point_to_values.len();
            let deleted_file = create_and_ensure_length(
                &deleted_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<usize>()),
            )?;
            let mut deleted_mmap = unsafe { MmapMut::map_mut(&deleted_file)? };
            deleted_mmap.fill(0);
            let mut deleted_bitflags = MmapBitSlice::from(deleted_mmap, 0);
            for (idx, values) in point_to_values.iter().enumerate() {
                if values.is_empty() {
                    deleted_bitflags.set(idx, true);
                }
            }
        }

        Self::load(path, is_on_disk)
    }

    pub fn flusher(&self) -> Flusher {
        self.deleted.flusher()
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
            self.path.join(HASHMAP_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.point_to_values.files());
        files
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if let Some(deleted) = self.deleted.get(idx) {
            if !deleted {
                self.deleted.set(idx, true);
                self.deleted_count += 1;
            }
        }
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&N) -> bool,
    ) -> bool {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        // Measure self.deleted access.
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(size_of::<bool>());

        self.deleted
            .get(idx as usize)
            .filter(|b| !b)
            .is_some_and(|_| {
                self.point_to_values.check_values_any(
                    idx,
                    |v| check_fn(N::from_referenced(&v)),
                    &hw_counter,
                )
            })
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = N::Referenced<'_>> + '_>> {
        self.deleted.get(idx as usize).filter(|b| !b).and_then(|_| {
            Some(Box::new(self.point_to_values.get_values(idx)?)
                as Box<dyn Iterator<Item = N::Referenced<'_>>>)
        })
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        self.deleted
            .get(idx as usize)
            .filter(|b| !b)
            .and_then(|_| self.point_to_values.get_values_count(idx))
    }

    pub fn get_indexed_points(&self) -> usize {
        self.point_to_values
            .len()
            .saturating_sub(self.deleted_count)
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    pub fn get_values_count(&self) -> usize {
        self.total_key_value_pairs
    }

    pub fn get_unique_values_count(&self) -> usize {
        self.value_to_points.keys_count()
    }

    pub fn get_count_for_value(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> Option<usize> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        // Since `value_to_points.get` doesn't actually force read from disk for all values
        // we need to only account for the overhead of hashmap lookup
        hw_counter
            .payload_index_io_read_counter()
            .incr_delta(READ_ENTRY_OVERHEAD);

        match self.value_to_points.get(value) {
            Ok(Some(points)) => Some(points.len()),
            Ok(None) => None,
            Err(err) => {
                debug_assert!(
                    false,
                    "Error while getting count for value {value:?}: {err:?}",
                );
                log::error!("Error while getting count for value {value:?}: {err:?}");
                None
            }
        }
    }

    pub fn get_iterator(
        &self,
        value: &N,
        hw_counter: &HardwareCounterCell,
    ) -> Box<dyn Iterator<Item = &PointOffsetType> + '_> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        match self.value_to_points.get(value) {
            Ok(Some(slice)) => {
                // We're iterating over the whole (mmapped) slice
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(size_of_val(slice) + READ_ENTRY_OVERHEAD);

                Box::new(
                    slice
                        .iter()
                        .filter(|idx| !self.deleted.get(**idx as usize).unwrap_or(false)),
                )
            }
            Ok(None) => {
                hw_counter
                    .payload_index_io_read_counter()
                    .incr_delta(READ_ENTRY_OVERHEAD);

                Box::new(iter::empty())
            }
            Err(err) => {
                debug_assert!(
                    false,
                    "Error while getting iterator for value {value:?}: {err:?}",
                );
                log::error!("Error while getting iterator for value {value:?}: {err:?}");
                Box::new(iter::empty())
            }
        }
    }

    pub fn iter_values(&self) -> Box<dyn Iterator<Item = &N> + '_> {
        Box::new(self.value_to_points.keys())
    }

    pub fn iter_counts_per_value(&self) -> impl Iterator<Item = (&N, usize)> + '_ {
        self.value_to_points.iter().map(|(k, v)| {
            let count = v
                .iter()
                .filter(|idx| !self.deleted.get(**idx as usize).unwrap_or(true))
                .unique()
                .count();
            (k, count)
        })
    }

    pub fn iter_values_map<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> impl Iterator<Item = (&'a N, IdIter<'a>)> + 'a {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        self.value_to_points.iter().map(move |(k, v)| {
            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(k.write_bytes());

            (
                k,
                Box::new(
                    v.iter()
                        .copied()
                        .filter(|idx| !self.deleted.get(*idx as usize).unwrap_or(true))
                        .measure_hw_with_acc(
                            hw_counter.new_accumulator(),
                            size_of::<PointOffsetType>(),
                            |i| i.payload_index_io_read_counter(),
                        ),
                ) as IdIter,
            )
        })
    }

    fn make_conditioned_counter<'a>(
        &self,
        hw_counter: &'a HardwareCounterCell,
    ) -> ConditionedCounter<'a> {
        ConditionedCounter::new(self.is_on_disk, hw_counter)
    }

    pub fn is_on_disk(&self) -> bool {
        self.is_on_disk
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.value_to_points.populate()?;
        self.point_to_values.populate();
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let value_to_points_path = self.path.join(HASHMAP_PATH);
        let deleted_path = self.path.join(DELETED_PATH);

        clear_disk_cache(&value_to_points_path)?;
        clear_disk_cache(&deleted_path)?;

        self.point_to_values.clear_cache()?;
        Ok(())
    }
}
