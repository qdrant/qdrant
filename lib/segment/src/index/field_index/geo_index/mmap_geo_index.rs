use std::borrow::Cow;
use std::path::{Path, PathBuf};

use ahash::AHashSet;
use common::binary_search::binary_search_by;
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::{atomic_save_json, read_json};
use common::generic_consts::{Random, Sequential};
use common::iterator_ext::ordering_iterator::OrderingIterator;
use common::mmap::{MmapSlice, create_and_ensure_length};
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, OpenOptions, ReadRange, TypedStorage, UniversalRead};
use fs_err as fs;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};

use super::mutable_geo_index::InMemoryGeoMapIndex;
use crate::common::Flusher;
use crate::common::buffered_update_bitslice::BufferedUpdateBitSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::stored_bitslice::MmapBitSlice;
use crate::index::field_index::geo_hash::{GeoHash, GeoHashRaw};
use crate::index::field_index::stored_point_to_values::StoredPointToValues;
use crate::types::GeoPoint;

const DELETED_PATH: &str = "deleted.bin";
const COUNTS_PER_HASH: &str = "counts_per_hash.bin";
const POINTS_MAP: &str = "points_map.bin";
const POINTS_MAP_IDS: &str = "points_map_ids.bin";
const STATS_PATH: &str = "mmap_field_index_stats.json";

#[repr(C)]
#[derive(Copy, Clone, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub(super) struct Counts {
    pub hash: GeoHashRaw,
    pub points: u32,
    pub values: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub(super) struct PointKeyValue {
    pub hash: GeoHashRaw,
    pub ids_start: u32,
    pub ids_end: u32,
}

/// An alias to set of traits required by [`StoredGeoMapIndex`].
#[expect(private_bounds)]
pub trait StoredGeoMapIndexStorage:
    UniversalRead<u8>
    + UniversalRead<Counts>
    + UniversalRead<PointKeyValue>
    + UniversalRead<PointOffsetType>
{
}
impl<T> StoredGeoMapIndexStorage for T where
    T: UniversalRead<u8>
        + UniversalRead<Counts>
        + UniversalRead<PointKeyValue>
        + UniversalRead<PointOffsetType>
{
}

///
///   points_map
///  ┌─────────────────────────────────────────┐
///  │ (ABC, 10, 20)|(ABD, 20, 40)             │
///  └────────┬──┬──────────┬───┬──────────────┘
///           │  │          │   │
///    ┌──────┘  └────────┐ │   └───────────────────┐
///    │                  │ └───┐                   │
///    │                  │     │                   │
///  ┌─▼──────────────────▼─────▼───────────────────▼──────────┐
///  │ 1, 8, 10, 18, 129, 213, 12, 13, 14, 87, 99, 199         │
///  └─────────────────────────────────────────────────────────┘
///   points_map_ids
///
pub struct StoredGeoMapIndex<S: StoredGeoMapIndexStorage> {
    path: PathBuf,
    pub(super) storage: Storage<S>,
    pub(super) deleted_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
    is_on_disk: bool,
}

pub(super) struct Storage<S: StoredGeoMapIndexStorage> {
    /// Stores GeoHash, points count and values count.
    /// Sorted by geohash, so we binary search the region.
    pub(super) counts_per_hash: TypedStorage<S, Counts>,
    /// Stores GeoHash and associated range of offsets in the points_map_ids.
    /// Sorted by geohash, so we binary search the region.
    pub(super) points_map: TypedStorage<S, PointKeyValue>,
    /// A storage of associations between geo-hashes and point ids. (See the diagram above)
    pub(super) points_map_ids: TypedStorage<S, PointOffsetType>,
    /// One-to-many mapping of the PointOffsetType to the GeoPoint.
    pub(super) point_to_values: StoredPointToValues<GeoPoint, S>,
    /// Deleted flags for each PointOffsetType
    pub(super) deleted: BufferedUpdateBitSlice<MmapFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredGeoMapIndexStat {
    points_values_count: usize,
    max_values_per_point: usize,
}

impl<S: StoredGeoMapIndexStorage> StoredGeoMapIndex<S> {
    pub fn build(
        dynamic_index: InMemoryGeoMapIndex,
        path: &Path,
        is_on_disk: bool,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;

        let deleted_path = path.join(DELETED_PATH);
        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        // Create the point-to-value mapping and persist in the file
        StoredPointToValues::<GeoPoint, MmapFile>::from_iter(
            path,
            dynamic_index
                .point_to_values
                .iter()
                .enumerate()
                .map(|(idx, values)| (idx as PointOffsetType, values.iter())),
        )?;

        {
            let points_map_file = create_and_ensure_length(
                &points_map_path,
                dynamic_index.points_map.len() * std::mem::size_of::<PointKeyValue>(),
            )?;
            let points_map_file = unsafe { MmapMut::map_mut(&points_map_file)? };
            let mut points_map = unsafe { MmapSlice::<PointKeyValue>::try_from(points_map_file)? };

            let points_map_ids_file = create_and_ensure_length(
                &points_map_ids_path,
                dynamic_index
                    .points_map
                    .values()
                    .map(|v| v.len())
                    .sum::<usize>()
                    * std::mem::size_of::<PointOffsetType>(),
            )?;
            let points_map_ids_file = unsafe { MmapMut::map_mut(&points_map_ids_file)? };
            let mut points_map_ids =
                unsafe { MmapSlice::<PointOffsetType>::try_from(points_map_ids_file)? };

            let mut ids_offset = 0;
            for (i, (hash, ids)) in dynamic_index.points_map.iter().enumerate() {
                points_map[i].hash = GeoHashRaw::from(*hash);
                points_map[i].ids_start = ids_offset as u32;
                points_map[i].ids_end = (ids_offset + ids.len()) as u32;
                points_map_ids[ids_offset..ids_offset + ids.len()].copy_from_slice(
                    &ids.iter()
                        .map(|v| *v as PointOffsetType)
                        .collect::<Vec<_>>(),
                );
                ids_offset += ids.len();
            }
        }

        {
            let counts_per_hash_file = create_and_ensure_length(
                &counts_per_hash_path,
                std::cmp::min(
                    dynamic_index.points_per_hash.len(),
                    dynamic_index.values_per_hash.len(),
                ) * std::mem::size_of::<Counts>(),
            )?;
            let counts_per_hash_file = unsafe { MmapMut::map_mut(&counts_per_hash_file)? };
            let mut counts_per_hash =
                unsafe { MmapSlice::<Counts>::try_from(counts_per_hash_file)? };
            for ((hash, points), dst) in dynamic_index
                .points_per_hash
                .iter()
                .zip(counts_per_hash.iter_mut())
            {
                if let Some(values) = dynamic_index.values_per_hash.get(hash) {
                    dst.hash = GeoHashRaw::from(*hash);
                    dst.points = *points as u32;
                    dst.values = *values as u32;
                }
            }
        }

        {
            let _ = create_and_ensure_length(
                &deleted_path,
                dynamic_index
                    .point_to_values
                    .len()
                    .div_ceil(u8::BITS as usize)
                    .next_multiple_of(size_of::<usize>()),
            )?;
            let mut deleted = MmapBitSlice::open(&deleted_path, OpenOptions::default())?;
            deleted.set_ascending_bits_batch(
                dynamic_index
                    .point_to_values
                    .iter()
                    .enumerate()
                    .filter(|(_, values)| values.is_empty())
                    .map(|(idx, _)| (idx as u64, true)),
            )?;
            deleted.flusher()()?;
        }

        atomic_save_json(
            &stats_path,
            &StoredGeoMapIndexStat {
                points_values_count: dynamic_index.points_values_count,
                max_values_per_point: dynamic_index.max_values_per_point,
            },
        )?;

        Self::open(path, is_on_disk)?.ok_or_else(|| {
            OperationError::service_error("Failed to open StoredGeoMapIndex after building it")
        })
    }

    pub fn open(path: &Path, is_on_disk: bool) -> OperationResult<Option<Self>> {
        let deleted_path = path.join(DELETED_PATH);
        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        // If stats file doesn't exist, assume the index doesn't exist on disk
        if !stats_path.is_file() {
            return Ok(None);
        }

        let populate = !is_on_disk;
        let stats: StoredGeoMapIndexStat = read_json(&stats_path)?;

        let open_options = OpenOptions {
            writeable: false,
            need_sequential: false,
            disk_parallel: None,
            populate: Some(populate),
            advice: None,
            prevent_caching: None,
        };

        let counts_per_hash = UniversalRead::open(&counts_per_hash_path, open_options)?;
        let points_map = UniversalRead::open(&points_map_path, open_options)?;
        let points_map_ids = UniversalRead::open(&points_map_ids_path, open_options)?;
        let point_to_values = StoredPointToValues::open(path, true)?;

        let deleted = MmapBitSlice::open(
            &deleted_path,
            OpenOptions {
                populate: Some(populate),
                ..OpenOptions::default()
            },
        )?;
        let deleted_count = deleted.count_ones()?;

        Ok(Some(Self {
            path: path.to_owned(),
            storage: Storage {
                counts_per_hash,
                points_map,
                points_map_ids,
                point_to_values,
                deleted: BufferedUpdateBitSlice::new(deleted),
            },
            deleted_count,
            points_values_count: stats.points_values_count,
            max_values_per_point: stats.max_values_per_point,
            is_on_disk,
        }))
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        let hw_counter = self.make_conditioned_counter(hw_counter);
        self.storage
            .deleted
            .get(idx as usize)
            .filter(|b| !b)
            .map(|_| {
                self.storage
                    .point_to_values
                    .check_values_any(idx, |v| check_fn(v), &hw_counter)
            })
            .map(|r| r.unwrap_or(false))
            .unwrap_or(false)
    }

    pub fn get_values(&self, idx: u32) -> Option<impl Iterator<Item = GeoPoint> + '_> {
        self.storage
            .point_to_values
            // TODO: propagate counter upwards
            .values_iter(idx, ConditionedCounter::never())
            .ok()?
            .map(|iter| iter.map(Cow::into_owned))
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.storage
            .deleted
            .get(idx as usize)
            .filter(|b| !b)
            .and_then(|_| self.storage.point_to_values.get_values_count(idx).ok()?)
            .unwrap_or(0)
    }

    pub(super) fn points_per_hash(
        &self,
        filter: impl Fn(&(GeoHash, usize)) -> bool,
    ) -> OperationResult<Vec<(GeoHash, usize)>> {
        let counts = self.storage.counts_per_hash.read::<Sequential>(ReadRange {
            byte_offset: 0,
            length: self.storage.counts_per_hash.len()?,
        })?;
        let mut results = Vec::with_capacity(counts.len());
        for count in counts.iter() {
            let pair = (count.hash.normalize(), count.points as usize);
            if filter(&pair) {
                results.push(pair);
            }
        }
        Ok(results)
    }

    pub fn points_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(self
            .counts_of_hash(hash, hw_counter)?
            .map(|c| c.points as usize)
            .unwrap_or(0))
    }

    pub fn values_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<usize> {
        Ok(self
            .counts_of_hash(hash, hw_counter)?
            .map(|c| c.values as usize)
            .unwrap_or(0))
    }

    fn counts_of_hash(
        &self,
        hash: GeoHash,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Counts>> {
        let hw_counter = self.make_conditioned_counter(hw_counter);
        let len = self.storage.counts_per_hash.len()? as usize;

        hw_counter
            .payload_index_io_read_counter()
            // Simulate binary search complexity as IO read estimation
            .incr_delta((len as f32).log2().ceil() as usize * size_of::<Counts>());

        let read_one = |idx| -> OperationResult<Counts> {
            let range = ReadRange::one((idx * size_of::<Counts>()) as u64);
            let value = self.storage.counts_per_hash.read::<Random>(range)?;
            Ok(value[0])
        };

        let found = binary_search_by(0..len, |idx| {
            read_one(idx).map(|c| c.hash.normalize().cmp(&hash))
        })?;

        if let Ok(index) = found {
            read_one(index).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn wipe(self) -> OperationResult<()> {
        let files = self.files();
        let path = self.path.clone();
        // drop storage handles before deleting files
        drop(self);
        for file in files {
            fs::remove_file(file)?;
        }
        let _ = fs::remove_dir(path);
        Ok(())
    }

    pub fn files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(DELETED_PATH),
            self.path.join(COUNTS_PER_HASH),
            self.path.join(POINTS_MAP),
            self.path.join(POINTS_MAP_IDS),
            self.path.join(STATS_PATH),
        ];
        files.extend(self.storage.point_to_values.files());
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            self.path.join(COUNTS_PER_HASH),
            self.path.join(POINTS_MAP),
            self.path.join(POINTS_MAP_IDS),
            self.path.join(STATS_PATH),
        ];
        files.extend(self.storage.point_to_values.immutable_files());
        files
    }

    pub fn flusher(&self) -> Flusher {
        self.storage.deleted.flusher()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) {
        let idx = idx as usize;
        if let Some(deleted) = self.storage.deleted.get(idx)
            && !deleted
        {
            self.storage.deleted.set(idx, true);
            self.deleted_count += 1;
        }
    }

    /// Return all unique point IDs which have any of the geo-hash prefixes.
    pub(super) fn all_points(
        &self,
        mut geo_hashes: Vec<GeoHash>,
    ) -> OperationResult<AHashSet<PointOffsetType>> {
        if geo_hashes.is_empty() {
            return Ok(AHashSet::default());
        }

        let len = self.storage.points_map.len()?;

        geo_hashes.sort_unstable();
        // Drop any prefix that is already subsumed by or equal to an earlier (shorter)
        // one. After this pass the remaining prefixes are pairwise
        // incomparable — no prefix is a prefix of another — so at most one of
        // them can match any given entry.
        geo_hashes.dedup_by(|later, earlier| later.starts_with(*earlier));

        let smallest_hash = *geo_hashes.first().unwrap();

        // The `points_map` file is sorted by geohash. We want to collect every
        // entry whose hash has any of `geo_hashes` as a prefix. Since entries
        // are sorted, the matching entries live between:
        // - `start`: the first entry with geohash >= smallest_hash, and
        // - `end`:   the first entry whose geohash is strictly greater than
        //            the last prefix and is not covered by it.
        //
        // Non-matching entries may be interleaved with matching ones when
        // multiple disjoint prefixes are requested, so we must check each
        // entry individually instead of stopping at the first miss.
        //
        // 0                             start                end           EOF
        // ├───────────────────────────────┼───────────────────┼─────────────┤
        // │<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<│..m..m.mm.....m.mmm│>>>>>>>>>>>>>│
        // │ entries < smallest_hash       │ m = matching      │ past end    │
        // └───────────────────────────────┴───────────────────┴─────────────┘

        // Step 1: binary search to find the index of the `start` entry.
        let start_idx = binary_search_by(0..len, |idx| {
            let range = ReadRange::one(idx * size_of::<PointKeyValue>() as u64);
            let value = self.storage.points_map.read::<Random>(range)?;
            OperationResult::Ok(value[0].hash.normalize().cmp(&smallest_hash))
        })?
        .unwrap_or_else(|index| index);

        // Step 2: read entries in chunks starting from `start`. Chunks may
        // arrive out of order from the underlying IO, so we reorder them with
        // `OrderingIterator` before inspecting their contents; this lets us
        // stop reading as soon as we walk past the last prefix.
        let chunks = self.storage.points_map.read_iter::<Sequential, _>(
            ReadRange {
                byte_offset: start_idx * size_of::<PointKeyValue>() as u64,
                length: len - start_idx,
            }
            .iter_autochunks::<PointKeyValue>()
            .enumerate(),
        )?;

        let ordered_chunks = OrderingIterator::new(chunks);

        // 128 - Guesstimate to avoid extra allocations on first iterations
        let mut point_map_ranges: Vec<ReadRange> = Vec::with_capacity(128);

        // `prefix_cursor` tracks the largest prefix <= current entry's hash.
        // Since entries are processed in sorted order and prefixes are sorted
        // and pairwise incomparable, the cursor only ever moves forward,
        // giving amortized O(1) matching per entry instead of O(|prefixes|).
        let mut prefix_cursor = 0usize;

        'outer: for chunk_result in ordered_chunks {
            let (_chunk_idx, entries) = chunk_result?;
            for entry in entries.iter() {
                let hash = entry.hash.normalize();

                while prefix_cursor + 1 < geo_hashes.len() && geo_hashes[prefix_cursor + 1] <= hash
                {
                    prefix_cursor += 1;
                }

                let current_prefix = geo_hashes[prefix_cursor];

                if hash.starts_with(current_prefix) {
                    point_map_ranges.push(ReadRange {
                        byte_offset: u64::from(entry.ids_start)
                            * size_of::<PointOffsetType>() as u64,
                        length: u64::from(entry.ids_end.saturating_sub(entry.ids_start)),
                    });
                } else if prefix_cursor + 1 == geo_hashes.len() {
                    // Past the last prefix with no match: no further entry
                    // can start with any prefix, so we're done.
                    break 'outer;
                }
            }
        }

        // Step 3: read the collected ranges and accumulate unique,
        // non-deleted point ids.
        let mut points = AHashSet::new();
        self.storage.points_map_ids.read_batch::<Random, _>(
            point_map_ranges.into_iter().enumerate(),
            |_idx, values| {
                points.extend(
                    values
                        .iter()
                        .copied()
                        .filter(|&id| !self.storage.deleted.get(id as usize).unwrap_or(true)),
                );
                Ok(())
            },
        )?;

        Ok(points)
    }

    pub fn points_count(&self) -> usize {
        self.storage
            .point_to_values
            .len()
            .saturating_sub(self.deleted_count)
    }

    pub fn points_values_count(&self) -> usize {
        self.points_values_count
    }

    pub fn max_values_per_point(&self) -> usize {
        self.max_values_per_point
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

    /// Populate all pages in the storage.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.storage.counts_per_hash.populate()?;
        self.storage.points_map.populate()?;
        self.storage.points_map_ids.populate()?;
        self.storage.point_to_values.populate()?;
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            path: _,
            storage,
            deleted_count: _,
            points_values_count: _,
            max_values_per_point: _,
            is_on_disk: _,
        } = self;
        let Storage {
            counts_per_hash,
            points_map,
            points_map_ids,
            point_to_values,
            deleted,
        } = storage;
        deleted.clear_cache()?;
        counts_per_hash.clear_ram_cache()?;
        points_map.clear_ram_cache()?;
        points_map_ids.clear_ram_cache()?;
        point_to_values.clear_cache()?;
        Ok(())
    }
}
