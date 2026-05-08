use std::borrow::{Borrow, Cow};
use std::ops::{BitOrAssign, Bound};
use std::path::{Path, PathBuf};

use common::bitvec::{BitSlice, BitSliceExt, BitVec};
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::counter::iterator_hw_measurement::HwMeasurementIteratorExt;
use common::fs::{atomic_save_json, clear_disk_cache, read_json};
use common::generic_consts::Random;
use common::mmap::{MmapSlice, create_and_ensure_length};
use common::stored_bitslice::MmapBitSlice;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, OpenOptions, ReadRange, TypedStorage, UniversalRead};
use fs_err as fs;
use itertools::Either;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};

use super::Encodable;
use super::mutable_numeric_index::InMemoryNumericIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::histogram::Histogram;
use crate::index::field_index::numeric_point::{Numericable, Point};
use crate::index::field_index::stored_point_to_values::{StoredPointToValues, StoredValue};

const PAIRS_PATH: &str = "data.bin";
const DELETED_PATH: &str = "deleted.bin";
const CONFIG_PATH: &str = "mmap_field_index_config.json";

/// Mmap-backed immutable numeric index.
///
/// On-disk state (`data.bin`, `deleted.bin`, `point_to_values.*`, etc.) is
/// written once during [`Self::build`] and not mutated afterwards: `deleted.bin`
/// records only the points whose payload was empty at build time.
///
/// Runtime deletions live in the in-memory `Storage::deleted` bitvec. They are
/// **not persisted** — [`Self::flusher`] is a no-op and [`Self::remove_point`]
/// only updates the in-memory bitvec. Callers must re-supply the authoritative
/// deletion set (typically `id_tracker.deleted_point_bitslice()`) via the
/// `deleted_points` argument to [`Self::open`] on reload.
pub struct MmapNumericIndex<T: Encodable + Numericable + Default + StoredValue + 'static> {
    path: PathBuf,
    pub(super) storage: Storage<T, MmapFile>,
    histogram: Histogram<T>,
    deleted_count: usize,
    max_values_per_point: usize,
    is_on_disk: bool,
}

pub(super) struct Storage<
    T: Encodable + Numericable + Default + StoredValue + 'static,
    S: UniversalRead,
> {
    deleted: BitVec,
    // sorted pairs (id + value), sorted by value (by id if values are equal)
    pairs: TypedStorage<S, Point<T>>,
    pub(super) point_to_values: StoredPointToValues<T, S>,
}

impl<T: Encodable + Numericable + Default + StoredValue + 'static, S: UniversalRead> Storage<T, S> {
    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            deleted,
            pairs,
            point_to_values,
        } = self;

        deleted.capacity().div_ceil(u8::BITS as usize)
            + pairs.ram_usage_bytes()
            + point_to_values.ram_usage_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MmapNumericIndexConfig {
    max_values_per_point: usize,
}

impl<T: Encodable + Numericable + Default + StoredValue + bytemuck::Pod> MmapNumericIndex<T> {
    pub fn build(
        in_memory_index: InMemoryNumericIndex<T>,
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;

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

        StoredPointToValues::<T, MmapFile>::from_iter(
            path,
            in_memory_index
                .point_to_values
                .iter()
                .enumerate()
                .map(|(idx, values)| (idx as PointOffsetType, values.iter().map(|v| v.borrow()))),
        )?;

        {
            let pairs_file = create_and_ensure_length(
                &pairs_path,
                in_memory_index.map.len() * size_of::<Point<T>>(),
            )?;
            let pairs_mmap = unsafe { MmapMut::map_mut(&pairs_file)? };
            let mut pairs = unsafe { MmapSlice::<Point<T>>::try_from(pairs_mmap)? };
            for (src, dst) in in_memory_index.map.iter().zip(pairs.iter_mut()) {
                *dst = *src;
            }
        }

        {
            let deleted_flags_count = in_memory_index.point_to_values.len();
            let _ = create_and_ensure_length(
                &deleted_path,
                deleted_flags_count
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<u64>()),
            )?;

            let mut deleted = MmapBitSlice::open(&deleted_path, OpenOptions::default())?;
            deleted.set_ascending_bits_batch(
                in_memory_index
                    .point_to_values
                    .iter()
                    .enumerate()
                    .filter(|(_, values)| values.is_empty())
                    .map(|(idx, _)| (idx as u64, true)),
            )?;
            deleted.flusher()()?;
        }

        Self::open(path, is_on_disk, deleted_points)?.ok_or_else(|| {
            OperationError::service_error("Failed to open MmapNumericIndex after building it")
        })
    }

    /// Open and load mmap numeric index from the given path
    pub fn open(
        path: &Path,
        is_on_disk: bool,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let pairs_path = path.join(PAIRS_PATH);
        let deleted_path = path.join(DELETED_PATH);
        let config_path = path.join(CONFIG_PATH);

        // If config doesn't exist, assume the index doesn't exist on disk
        if !config_path.is_file() {
            return Ok(None);
        }

        let histogram = Histogram::<T>::load(path)?;
        let config: MmapNumericIndexConfig = read_json(&config_path)?;
        let do_populate = !is_on_disk;

        let pairs_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            disk_parallel: None,
            populate: Some(do_populate),
            advice: None,
            prevent_caching: None,
        };
        let pairs = TypedStorage::open(pairs_path, pairs_options)?;

        let point_to_values = StoredPointToValues::open(path, do_populate)?;
        let mut deleted = deleted_points.to_owned();

        let deleted_payload_mmap = MmapBitSlice::open(&deleted_path, OpenOptions::default())?;
        let deleted_payloads_bitslice = deleted_payload_mmap.read_all()?;

        // `deleted` length must match `point_to_values.len()` because it only
        // tracks the index's contents. The id-tracker's deleted mask can be
        // shorter or longer; if shorter, the missing entries default to live
        // (the id-tracker is the source of truth for deletions, and a shorter
        // mask just means it doesn't yet know about those higher offsets).
        deleted.resize(point_to_values.len(), false);
        deleted.bitor_assign(deleted_payloads_bitslice.as_ref());

        let deleted_count = deleted.count_ones();

        Ok(Some(Self {
            path: path.to_path_buf(),
            storage: Storage {
                deleted,
                pairs,
                point_to_values,
            },
            histogram,
            deleted_count,
            max_values_per_point: config.max_values_per_point,
            is_on_disk,
        }))
    }

    pub fn wipe(self) -> OperationResult<()> {
        let files = self.files();
        let path = self.path.clone();
        // drop mmap handles before deleting files
        drop(self);
        for file in files {
            fs::remove_file(file)?;
        }
        let _ = fs::remove_dir(path);
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(PAIRS_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.files());
        files.extend(Histogram::<T>::files(&self.path));
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(PAIRS_PATH),
            self.path.join(DELETED_PATH),
            self.path.join(CONFIG_PATH),
        ];
        files.extend(self.storage.point_to_values.immutable_files());
        files.extend(Histogram::<T>::immutable_files(&self.path));
        files
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`MmapNumericIndex`] for the deletion durability contract.
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<bool> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            self.storage
                .point_to_values
                .check_values_any(idx, |v| check_fn(v), &hw_counter)
        } else {
            Ok(false)
        }
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Box<dyn Iterator<Item = T> + '_>> {
        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            Some(Box::new(
                self.storage
                    .point_to_values
                    // TODO: Propagate counter upwards
                    .values_iter(idx, ConditionedCounter::never())
                    .ok()??
                    .map(|v| *v),
            ))
        } else {
            None
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> Option<usize> {
        if self.storage.deleted.get_bit(idx as usize) == Some(false) {
            self.storage.point_to_values.get_values_count(idx).ok()?
        } else {
            None
        }
    }

    /// Returns the number of key-value pairs in the index.
    /// Note that is doesn't count deleted pairs.
    pub(super) fn total_unique_values_count(&self) -> OperationResult<usize> {
        Ok(self.storage.pairs.len()? as usize)
    }

    pub(super) fn values_range<'a>(
        &'a self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
        hw_counter: &'a HardwareCounterCell,
    ) -> OperationResult<impl Iterator<Item = PointOffsetType> + 'a> {
        let hw_counter = self.make_conditioned_counter(hw_counter);

        Ok(self
            .values_range_iterator(start_bound, end_bound)?
            .map(|point| point.idx)
            .measure_hw_with_condition_cell(hw_counter, size_of::<Point<T>>(), |i| {
                i.payload_index_io_read_counter()
            }))
    }

    pub(super) fn orderable_values_range(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = (T, PointOffsetType)> + '_> {
        Ok(self
            .values_range_iterator(start_bound, end_bound)?
            .map(|Point { val, idx, .. }| (val, idx)))
    }

    /// Marks `idx` as deleted in the in-memory deletion bitvec.
    ///
    /// Not persisted: on reopen, deletions must be re-supplied via the
    /// `deleted_points` argument to [`Self::open`].
    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if idx < self.storage.deleted.len() && !self.storage.deleted.get_bit(idx).unwrap_or(true) {
            self.storage.deleted.set(idx, true);
            self.deleted_count += 1;
        }
    }

    pub(super) fn get_histogram(&self) -> &Histogram<T> {
        &self.histogram
    }

    pub(super) fn get_points_count(&self) -> usize {
        self.storage.point_to_values.len() - self.deleted_count
    }

    pub(super) fn get_max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    pub(super) fn values_range_size(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<usize> {
        let (start, end) = self.values_range_bounds(start_bound, end_bound)?;
        Ok(end - start)
    }

    /// Binary search within `[lo, hi)` range of `pairs` storage.
    ///
    /// Returns `Ok(index)` if the element is found, `Err(index)` if not
    /// (where `index` is where the element would be inserted).
    fn binary_search_pairs(
        &self,
        bound: &Point<T>,
        lo: usize,
        hi: usize,
    ) -> OperationResult<Result<usize, usize>> {
        let mut left = lo;
        let mut right = hi;
        while left < right {
            let mid = left + (right - left) / 2;
            // TODO(luis): use read_one
            let elem = self.storage.pairs.read::<Random>(ReadRange {
                byte_offset: (mid * size_of::<Point<T>>()) as u64,
                length: 1,
            })?;
            match elem[0].cmp(bound) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => return Ok(Ok(mid)),
                std::cmp::Ordering::Greater => right = mid,
            }
        }
        Ok(Err(left))
    }

    /// Find the `[start_index, end_index)` range for the given bounds.
    fn values_range_bounds(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<(usize, usize)> {
        let len = self.storage.pairs.len()? as usize;

        let start_index = match start_bound {
            Bound::Included(bound) => self
                .binary_search_pairs(&bound, 0, len)?
                .unwrap_or_else(|idx| idx),
            Bound::Excluded(bound) => match self.binary_search_pairs(&bound, 0, len)? {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Unbounded => 0,
        };

        if start_index >= len {
            return Ok((len, len));
        }

        let end_index = match end_bound {
            Bound::Included(bound) => match self.binary_search_pairs(&bound, start_index, len)? {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            },
            Bound::Excluded(bound) => self
                .binary_search_pairs(&bound, start_index, len)?
                .unwrap_or_else(|idx| idx),
            Bound::Unbounded => len,
        };

        Ok((start_index, end_index))
    }

    /// Returns an iterator over non-deleted pairs.
    ///
    /// This will read the entire range upfront if it was not already cached.
    fn values_range_iterator(
        &self,
        start_bound: Bound<Point<T>>,
        end_bound: Bound<Point<T>>,
    ) -> OperationResult<impl DoubleEndedIterator<Item = Point<T>> + '_> {
        let (start_pos, end_pos) = self.values_range_bounds(start_bound, end_bound)?;
        let count = end_pos - start_pos;

        let iter = if count > 0 {
            match self.storage.pairs.read::<Random>(ReadRange {
                byte_offset: (start_pos * size_of::<Point<T>>()) as u64,
                length: count as u64,
            })? {
                Cow::Borrowed(slice) => Either::Left(slice.iter().copied()),
                Cow::Owned(vec) => Either::Right(vec.into_iter()),
            }
        } else {
            Either::Right(Vec::new().into_iter())
        };

        let deleted = &self.storage.deleted;

        Ok(iter.filter(move |point| !deleted.get_bit(point.idx as usize).unwrap_or(true)))
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
        self.storage.pairs.populate()?;
        self.storage.point_to_values.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path,
            storage,
            histogram: _,
            deleted_count: _,
            max_values_per_point: _,
            is_on_disk: _,
        } = self;
        let Storage {
            deleted: _,
            pairs,
            point_to_values,
        } = storage;
        pairs.clear_ram_cache()?;
        clear_disk_cache(&path.join(DELETED_PATH))?;
        point_to_values.clear_cache()?;
        Ok(())
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        let Self {
            path: _,
            storage,
            histogram,
            deleted_count: _,
            max_values_per_point: _,
            is_on_disk: _,
        } = self;

        histogram.ram_usage_bytes() + storage.ram_usage_bytes()
    }
}
