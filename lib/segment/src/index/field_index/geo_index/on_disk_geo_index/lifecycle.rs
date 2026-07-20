use std::borrow::Cow;
use std::path::{Path, PathBuf};

use ahash::AHashSet;
use common::binary_search::binary_search_by;
use common::bitvec::{BitSlice, DeletedBitVec};
use common::counter::conditioned_counter::ConditionedCounter;
use common::counter::hardware_counter::HardwareCounterCell;
use common::fs::{atomic_save_json, clear_disk_cache};
use common::generic_consts::{Random, Sequential};
use common::iterator_ext::ordering_iterator::OrderingIterator;
use common::mmap::{AdviceSetting, MmapSlice, create_and_ensure_length};
use common::types::PointOffsetType;
use common::universal_io::{
    CachedReadFs, MmapFile, OkNotFound, OpenOptions, Populate, ReadRange, SortedBlockIndex,
    TypedStorage, UioResult, UniversalRead, UniversalReadFs, UserData, read_json_via,
};
use fs_err as fs;
use memmap2::MmapMut;

use super::super::mutable_geo_index::InMemoryGeoIndex;
use super::{
    COUNTS_PER_HASH, COUNTS_PER_HASH_BLOCK_INDEX, Counts, DELETED_PATH, OnDiskGeoIndex, POINTS_MAP,
    POINTS_MAP_BLOCK_INDEX, POINTS_MAP_IDS, PointKeyValue, STATS_PATH, Storage, StoredGeoIndexStat,
};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::deleted_mask::{
    bitor_deleted_mask, deleted_mask_file, preopen_deleted_mask, save_deleted_mask,
};
use crate::index::field_index::geo_hash::{GeoHash, GeoHashRaw};
use crate::index::field_index::on_disk_point_to_values::OnDiskPointToValues;
use crate::types::GeoPoint;

impl<S: UniversalRead> OnDiskGeoIndex<S> {
    pub fn build(
        fs: &S::Fs,
        dynamic_index: InMemoryGeoIndex,
        path: &Path,
        populate: Populate,
        deleted_points: &BitSlice,
    ) -> OperationResult<Self> {
        fs::create_dir_all(path)?;

        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        // Create the point-to-value mapping and persist in the file
        OnDiskPointToValues::<GeoPoint, MmapFile>::build_from_iter(
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
                dynamic_index.points_map.len() * size_of::<PointKeyValue>(),
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
                    * size_of::<PointOffsetType>(),
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

            SortedBlockIndex::write(&path.join(POINTS_MAP_BLOCK_INDEX), &points_map)?;
        }

        {
            let counts_per_hash_file = create_and_ensure_length(
                &counts_per_hash_path,
                std::cmp::min(
                    dynamic_index.points_per_hash.len(),
                    dynamic_index.values_per_hash.len(),
                ) * size_of::<Counts>(),
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

            SortedBlockIndex::write(&path.join(COUNTS_PER_HASH_BLOCK_INDEX), &counts_per_hash)?;
        }

        save_deleted_mask(
            path,
            DELETED_PATH,
            dynamic_index.point_to_values.len(),
            dynamic_index
                .point_to_values
                .iter()
                .enumerate()
                .filter(|(_, values)| values.is_empty())
                .map(|(idx, _)| idx as PointOffsetType),
        )?;

        atomic_save_json(
            &stats_path,
            &StoredGeoIndexStat {
                points_values_count: dynamic_index.points_values_count,
                max_values_per_point: dynamic_index.max_values_per_point,
            },
        )?;

        Self::open(fs, path, populate, deleted_points)?.ok_or_else(|| {
            OperationError::service_error("Failed to open OnDiskGeoIndex after building it")
        })
    }

    fn open_options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    /// Schedule background prefetch of every file [`open`](Self::open) will read.
    ///
    /// Returns `false` when the segment is not in the
    /// on-disk format.
    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        path: &Path,
        populate: Populate,
    ) -> OperationResult<bool> {
        // Stats
        let stats_path = path.join(STATS_PATH);
        if fs
            .schedule_prefetch(&stats_path, None, None)
            .ok_not_found()?
            .is_none()
        {
            // If stats file doesn't exist, assume the index doesn't exist on disk
            return Ok(false);
        }

        // Geohash counts, points map, and point-id list
        let options = Self::open_options(populate);
        fs.schedule_prefetch(&path.join(COUNTS_PER_HASH), Some(options), None)?;
        fs.schedule_prefetch(&path.join(POINTS_MAP), Some(options), None)?;
        fs.schedule_prefetch(&path.join(POINTS_MAP_IDS), Some(options), None)?;

        // Block indexes over the two sorted arrays; optional, absent on old
        // segments
        let _ = fs
            .schedule_prefetch(&path.join(COUNTS_PER_HASH_BLOCK_INDEX), None, None)
            .ok_not_found()?;
        let _ = fs
            .schedule_prefetch(&path.join(POINTS_MAP_BLOCK_INDEX), None, None)
            .ok_not_found()?;

        // Point to values
        OnDiskPointToValues::<GeoPoint, S>::preopen(fs, path, populate)?;

        // "No values" mask
        preopen_deleted_mask(
            fs,
            path,
            DELETED_PATH,
            Self::open_options(Populate::PreferBackground),
        )?;

        Ok(true)
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        path: &Path,
        populate: Populate,
        deleted_points: &BitSlice,
    ) -> OperationResult<Option<Self>> {
        let stats_path = path.join(STATS_PATH);
        let counts_per_hash_path = path.join(COUNTS_PER_HASH);
        let points_map_path = path.join(POINTS_MAP);
        let points_map_ids_path = path.join(POINTS_MAP_IDS);

        let Some(stats) = read_json_via::<_, StoredGeoIndexStat>(fs, &stats_path).ok_not_found()?
        else {
            // If stats file doesn't exist, assume the index doesn't exist on disk
            return Ok(None);
        };

        let open_options = Self::open_options(populate);

        let counts_per_hash =
            TypedStorage::new(fs.open(&counts_per_hash_path, open_options, Default::default())?);
        let counts_per_hash_block_index = SortedBlockIndex::open(
            fs,
            &path.join(COUNTS_PER_HASH_BLOCK_INDEX),
            counts_per_hash.len()? as usize,
        )?;
        let points_map =
            TypedStorage::new(fs.open(&points_map_path, open_options, Default::default())?);
        let points_map_block_index = SortedBlockIndex::open(
            fs,
            &path.join(POINTS_MAP_BLOCK_INDEX),
            points_map.len()? as usize,
        )?;
        let points_map_ids =
            TypedStorage::new(fs.open(&points_map_ids_path, open_options, Default::default())?);
        let point_to_values = OnDiskPointToValues::open(fs, path, populate)?;

        let mut deleted = deleted_points.to_owned();

        // `deleted` length must match `point_to_values.len()` because it only
        // tracks the index's contents. The id-tracker's deleted mask can be
        // shorter or longer; if shorter, the missing entries default to live
        // (the id-tracker is the source of truth for deletions, and a shorter
        // mask just means it doesn't yet know about those higher offsets).
        deleted.resize(point_to_values.len(), false);
        let compact_deleted_mask = bitor_deleted_mask(
            fs,
            path,
            DELETED_PATH,
            Self::open_options(populate),
            &mut deleted,
        )?;

        Ok(Some(Self {
            path: path.to_owned(),
            storage: Storage {
                counts_per_hash,
                counts_per_hash_block_index,
                points_map,
                points_map_block_index,
                points_map_ids,
                point_to_values,
                deleted: DeletedBitVec::new(deleted),
            },
            points_values_count: stats.points_values_count,
            max_values_per_point: stats.max_values_per_point,
            compact_deleted_mask,
        }))
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> OperationResult<bool> {
        let hw_counter = ConditionedCounter::always(hw_counter);
        if self.storage.deleted.is_active(idx) {
            self.storage
                .point_to_values
                .check_values_any(idx, |v| check_fn(v), &hw_counter)
        } else {
            Ok(false)
        }
    }

    /// Batched counterpart of [`Self::check_values_any`].
    pub fn for_each_matching_value<I, F, M, U>(
        &self,
        items: I,
        hw_counter: &HardwareCounterCell,
        check_fn: F,
        mut on_match: M,
    ) -> OperationResult<()>
    where
        U: UserData,
        I: Iterator<Item = (U, PointOffsetType)>,
        F: Fn(&GeoPoint) -> bool,
        M: FnMut(U, bool),
    {
        self.storage.point_to_values.values_iter_batch(
            items,
            &self.storage.deleted,
            ConditionedCounter::always(hw_counter),
            |tag, mut values| on_match(tag, values.any(|value| check_fn(&value))),
        )
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
        if self.storage.deleted.is_active(idx) {
            self.storage
                .point_to_values
                .get_values_count(idx)
                .ok()
                .flatten()
                .unwrap_or(0)
        } else {
            0
        }
    }

    pub(in super::super) fn points_per_hash(
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
        let hw_counter = ConditionedCounter::always(hw_counter);
        let len = self.storage.counts_per_hash.len()? as usize;

        if let Some(block_index) = &self.storage.counts_per_hash_block_index {
            let block = block_index.find_block(|counts| counts.hash.normalize().cmp(&hash));
            if block.is_empty() {
                return Ok(None);
            }

            hw_counter
                .payload_index_io_read_counter()
                .incr_delta(block.len() * size_of::<Counts>());

            let counts = self.storage.counts_per_hash.read::<Random>(ReadRange {
                byte_offset: (block.start * size_of::<Counts>()) as u64,
                length: block.len() as u64,
            })?;
            return Ok(counts
                .binary_search_by(|counts| counts.hash.normalize().cmp(&hash))
                .ok()
                .map(|idx| counts[idx]));
        }

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
            deleted_mask_file(&self.path, self.compact_deleted_mask, DELETED_PATH),
            self.path.join(COUNTS_PER_HASH),
            self.path.join(POINTS_MAP),
            self.path.join(POINTS_MAP_IDS),
            self.path.join(STATS_PATH),
        ];
        files.extend(self.block_index_files());
        files.extend(self.storage.point_to_values.files());
        files
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        let mut files = vec![
            deleted_mask_file(&self.path, self.compact_deleted_mask, DELETED_PATH),
            self.path.join(COUNTS_PER_HASH),
            self.path.join(POINTS_MAP),
            self.path.join(POINTS_MAP_IDS),
            self.path.join(STATS_PATH),
        ];
        files.extend(self.block_index_files());
        files.extend(self.storage.point_to_values.immutable_files());
        files
    }

    /// Paths of the optional block-index sidecar files which are present.
    fn block_index_files(&self) -> Vec<PathBuf> {
        let mut files = Vec::new();
        if self.storage.counts_per_hash_block_index.is_some() {
            files.push(self.path.join(COUNTS_PER_HASH_BLOCK_INDEX));
        }
        if self.storage.points_map_block_index.is_some() {
            files.push(self.path.join(POINTS_MAP_BLOCK_INDEX));
        }
        files
    }

    /// No-op flusher: the on-disk state is build-time only. See the type-level
    /// docs on [`OnDiskGeoIndex`] for the deletion durability contract.
    pub fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }

    /// Marks `idx` as deleted in the in-memory deletion bitvec.
    ///
    /// Not persisted: on reopen, deletions must be re-supplied via the
    /// `deleted_points` argument to [`Self::open`].
    pub fn remove_point(&mut self, idx: PointOffsetType) {
        self.storage.deleted.mark_deleted(idx);
    }

    /// Return all unique point IDs which have any of the geo-hash prefixes.
    pub(in super::super) fn all_points(
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

        // Step 1: binary search to find the index of the `start` entry. With a
        // block index available, this costs a single contiguous read of one
        // block instead of `O(log n)` random reads.
        let start_idx = if let Some(block_index) = &self.storage.points_map_block_index {
            let block = block_index.find_block(|entry| entry.hash.normalize().cmp(&smallest_hash));
            let entries = self.storage.points_map.read::<Random>(ReadRange {
                byte_offset: (block.start * size_of::<PointKeyValue>()) as u64,
                length: block.len() as u64,
            })?;
            let idx = entries
                .binary_search_by(|entry| entry.hash.normalize().cmp(&smallest_hash))
                .unwrap_or_else(|idx| idx);
            (block.start + idx) as u64
        } else {
            binary_search_by(0..len, |idx| {
                let range = ReadRange::one(idx * size_of::<PointKeyValue>() as u64);
                let value = self.storage.points_map.read::<Random>(range)?;
                OperationResult::Ok(value[0].hash.normalize().cmp(&smallest_hash))
            })?
            .unwrap_or_else(|index| index)
        };

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
        let deleted = &self.storage.deleted;
        self.storage.points_map_ids.read_batch(
            point_map_ranges.into_iter().enumerate(),
            Random,
            |_idx, values| {
                points.extend(values.iter().copied().filter(|&id| deleted.is_active(id)));
                UioResult::Ok(())
            },
        )?;

        Ok(points)
    }

    pub fn points_count(&self) -> usize {
        self.storage
            .point_to_values
            .len()
            .saturating_sub(self.storage.deleted.deleted_count())
    }

    pub fn points_values_count(&self) -> usize {
        self.points_values_count
    }

    pub fn max_values_per_point(&self) -> usize {
        self.max_values_per_point
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
            path,
            storage,
            points_values_count: _,
            max_values_per_point: _,
            compact_deleted_mask,
        } = self;
        let Storage {
            counts_per_hash,
            counts_per_hash_block_index,
            points_map,
            points_map_block_index,
            points_map_ids,
            point_to_values,
            deleted: _,
        } = storage;
        clear_disk_cache(&deleted_mask_file(
            path,
            *compact_deleted_mask,
            DELETED_PATH,
        ))?;
        counts_per_hash.clear_ram_cache()?;
        if counts_per_hash_block_index.is_some() {
            clear_disk_cache(&path.join(COUNTS_PER_HASH_BLOCK_INDEX))?;
        }
        points_map.clear_ram_cache()?;
        if points_map_block_index.is_some() {
            clear_disk_cache(&path.join(POINTS_MAP_BLOCK_INDEX))?;
        }
        points_map_ids.clear_ram_cache()?;
        point_to_values.clear_cache()?;
        Ok(())
    }

    pub(crate) fn ram_usage_bytes(&self) -> usize {
        self.storage.ram_usage_bytes()
    }
}
