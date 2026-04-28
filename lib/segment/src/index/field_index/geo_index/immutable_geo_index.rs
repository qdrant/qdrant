use std::borrow::Cow;
use std::path::PathBuf;

use common::bitvec::BitSliceExt;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{MmapFile, ReadRange, UniversalRead};

use super::mmap_geo_index::StoredGeoMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::{GeoHash, encode_max_precision};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

const DELETED_SENTINEL: PointOffsetType = PointOffsetType::MAX;

#[derive(Copy, Clone, Debug)]
struct Counts {
    hash: GeoHash,
    points: u32,
    values: u32,
}

impl From<super::mmap_geo_index::Counts> for Counts {
    #[inline]
    fn from(counts: super::mmap_geo_index::Counts) -> Self {
        let super::mmap_geo_index::Counts {
            hash,
            points,
            values,
        } = counts;
        Self {
            hash: hash.normalize(),
            points,
            values,
        }
    }
}

/// RAM-loaded immutable geo index using flat parallel arrays instead of per-hash
/// `AHashSet`s. This dramatically reduces memory overhead:
///
/// - `points_map_hashes[i]` is the sorted geohash for the i-th entry.
/// - `points_map_offsets[i]..points_map_offsets[i+1]` is the range in
///   `points_map_ids` holding the point IDs for that hash.
/// - Deleted entries in `points_map_ids` are marked with `DELETED_SENTINEL`.
pub struct ImmutableGeoMapIndex {
    counts_per_hash: Vec<Counts>,
    points_map_hashes: Vec<GeoHash>,
    points_map_offsets: Vec<u32>,
    points_map_ids: Vec<PointOffsetType>,
    point_to_values: ImmutablePointToValues<GeoPoint>,
    points_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
    storage: Storage,
    cached_ram_usage_bytes: usize,
}

enum Storage {
    Mmap(Box<StoredGeoMapIndex<MmapFile>>),
}

impl ImmutableGeoMapIndex {
    /// Open and load immutable geo index from mmap storage
    pub fn open_mmap(index: StoredGeoMapIndex<MmapFile>) -> OperationResult<Self> {
        let counts_per_hash = index
            .storage
            .counts_per_hash
            .read_whole()?
            .iter()
            .copied()
            .map(Counts::from)
            .collect();

        // Build flat parallel arrays from on-disk points_map + points_map_ids
        let points_map_entries = index.storage.points_map.read_whole()?;
        let num_entries = points_map_entries.len();
        let mut points_map_hashes = Vec::with_capacity(num_entries);
        let mut points_map_offsets = Vec::with_capacity(num_entries + 1);
        let mut points_map_ids = Vec::new();

        index.storage.points_map_ids.read_batch::<Random, _>(
            points_map_entries
                .iter()
                .map(|item| ReadRange {
                    byte_offset: u64::from(item.ids_start) * size_of::<PointOffsetType>() as u64,
                    length: u64::from(item.ids_end.saturating_sub(item.ids_start)),
                })
                .enumerate(),
            |i, ids| {
                points_map_hashes.push(points_map_entries[i].hash.normalize());
                points_map_offsets.push(points_map_ids.len() as u32);
                for &id in ids {
                    if !index
                        .storage
                        .deleted
                        .get_bit(id as usize)
                        .unwrap_or_default()
                    {
                        points_map_ids.push(id);
                    }
                }
                Ok(())
            },
        )?;
        points_map_offsets.push(points_map_ids.len() as u32);
        drop(points_map_entries);

        // Get point values and filter deleted points
        // Track deleted points to adjust point and value counts after loading
        let mut deleted_points: Vec<(PointOffsetType, Vec<GeoPoint>)> =
            Vec::with_capacity(index.deleted_count);
        let point_to_values = ImmutablePointToValues::new(
            index
                .storage
                .point_to_values
                .iter()
                .map(|id_values| {
                    let (id, values) = id_values?;
                    let is_deleted = index
                        .storage
                        .deleted
                        .get_bit(id as usize)
                        .unwrap_or_default();
                    let values = match (is_deleted, values) {
                        (false, Some(values)) => values.map(Cow::into_owned).collect(),
                        (false, None) => vec![],
                        (true, Some(values)) => {
                            let geo_points: Vec<GeoPoint> = values.map(Cow::into_owned).collect();
                            deleted_points.push((id, geo_points));
                            vec![]
                        }
                        (true, None) => {
                            deleted_points.push((id, vec![]));
                            vec![]
                        }
                    };
                    Ok(values)
                })
                .collect::<OperationResult<_>>()?,
        );

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap geo index: {err}");
        }
        let _ = index;

        // Construct immutable geo index
        let mut index = Self {
            counts_per_hash,
            points_map_hashes,
            points_map_offsets,
            points_map_ids,
            point_to_values,
            points_count: index.points_count(),
            points_values_count: index.points_values_count(),
            max_values_per_point: index.max_values_per_point(),
            storage: Storage::Mmap(Box::new(index)),
            cached_ram_usage_bytes: 0,
        };

        // Update point and value counts based on deleted points
        for (_idx, removed_geo_points) in deleted_points {
            index.points_values_count = index
                .points_values_count
                .saturating_sub(removed_geo_points.len());

            let removed_geo_hashes: Vec<_> = removed_geo_points
                .into_iter()
                .map(|geo_point| encode_max_precision(geo_point.lon.0, geo_point.lat.0).unwrap())
                .collect();
            for &removed_geo_hash in &removed_geo_hashes {
                index.decrement_hash_value_counts(removed_geo_hash);
            }
            index.decrement_hash_point_counts(&removed_geo_hashes);
        }

        index.cached_ram_usage_bytes = index.compute_ram_usage_bytes();
        Ok(index)
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::Mmap(ref index) => index.files(),
        }
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        match &self.storage {
            Storage::Mmap(index) => index.immutable_files(),
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            Storage::Mmap(index) => index.clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear immutable geo index gridstore cache: {err}"
                ))
            }),
        }
    }

    pub fn wipe(self) -> OperationResult<()> {
        match self.storage {
            Storage::Mmap(index) => index.wipe(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::Mmap(ref index) => index.flusher(),
        }
    }

    pub fn points_count(&self) -> usize {
        self.points_count
    }

    pub fn points_values_count(&self) -> usize {
        self.points_values_count
    }

    pub fn max_values_per_point(&self) -> usize {
        self.max_values_per_point
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        let mut counter = 0usize;

        self.point_to_values.check_values_any(idx, |v| {
            counter += 1;
            check_fn(v)
        })
    }

    pub fn get_values(&self, idx: u32) -> Option<impl Iterator<Item = &GeoPoint> + '_> {
        self.point_to_values.get_values(idx)
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.point_to_values
            .get_values_count(idx)
            .unwrap_or_default()
    }

    pub fn points_per_hash(&self) -> impl Iterator<Item = (GeoHash, usize)> + '_ {
        self.counts_per_hash
            .iter()
            .map(|counts| (counts.hash, counts.points as usize))
    }

    pub fn points_of_hash(&self, hash: GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(&hash)) {
            self.counts_per_hash[index].points as usize
        } else {
            0
        }
    }

    pub fn values_of_hash(&self, hash: GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(&hash)) {
            self.counts_per_hash[index].values as usize
        } else {
            0
        }
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        let removed_geo_points = self.point_to_values.remove_point(idx);
        if removed_geo_points.is_empty() {
            return Ok(());
        }

        self.points_count -= 1;
        self.points_values_count -= removed_geo_points.len();
        let mut removed_geo_hashes = Vec::with_capacity(removed_geo_points.len());

        for removed_geo_point in removed_geo_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_geo_point.lon.0, removed_geo_point.lat.0).unwrap();
            removed_geo_hashes.push(removed_geo_hash);

            match self.storage {
                Storage::Mmap(ref mut index) => {
                    index.remove_point(idx);
                }
            }

            if let Ok(hash_idx) = self
                .points_map_hashes
                .binary_search_by(|x| x.cmp(&removed_geo_hash))
            {
                let start = self.points_map_offsets[hash_idx] as usize;
                let end = self.points_map_offsets[hash_idx + 1] as usize;
                for slot in &mut self.points_map_ids[start..end] {
                    if *slot == idx {
                        *slot = DELETED_SENTINEL;
                        break;
                    }
                }
            } else {
                log::warn!("Geo index error: no points for hash {removed_geo_hash} were found");
            };

            self.decrement_hash_value_counts(removed_geo_hash);
        }

        self.decrement_hash_point_counts(&removed_geo_hashes);
        Ok(())
    }

    /// Returns an iterator over all point IDs which have the `geohash` prefix.
    /// Note. Point ID may be repeated multiple times in the iterator.
    pub fn stored_sub_regions(&self, geo: GeoHash) -> impl Iterator<Item = PointOffsetType> + '_ {
        let start_index = self
            .points_map_hashes
            .binary_search_by(|p| p.cmp(&geo))
            .unwrap_or_else(|index| index);
        let hashes = &self.points_map_hashes[start_index..];
        let offsets = &self.points_map_offsets[start_index..];

        hashes
            .iter()
            .zip(offsets.iter().zip(offsets[1..].iter()))
            .take_while(move |(hash, _)| hash.starts_with(geo))
            .flat_map(|(_, (&start, &end))| {
                self.points_map_ids[start as usize..end as usize]
                    .iter()
                    .copied()
                    .filter(|&id| id != DELETED_SENTINEL)
            })
    }

    fn decrement_hash_value_counts(&mut self, geo_hash: GeoHash) {
        for i in 0..=geo_hash.len() {
            let sub_geo_hash = geo_hash.truncate(i);
            if let Ok(index) = self
                .counts_per_hash
                .binary_search_by(|x| x.hash.cmp(&sub_geo_hash))
            {
                let values_count = self.counts_per_hash[index].values;
                if values_count > 0 {
                    self.counts_per_hash[index].values = values_count - 1;
                } else {
                    debug_assert!(false, "Hash value count is already empty: {sub_geo_hash}");
                }
            } else {
                debug_assert!(
                    false,
                    "Hash value count is not found for hash: {sub_geo_hash}",
                );
            }
        }
    }

    fn decrement_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: Vec<GeoHash> = Vec::new();
        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = geo_hash.truncate(i);
                if seen_hashes.contains(&sub_geo_hash) {
                    continue;
                }
                seen_hashes.push(sub_geo_hash);
                if let Ok(index) = self
                    .counts_per_hash
                    .binary_search_by(|x| x.hash.cmp(&sub_geo_hash))
                {
                    let points_count = self.counts_per_hash[index].points;
                    if points_count > 0 {
                        self.counts_per_hash[index].points = points_count - 1;
                    } else {
                        debug_assert!(false, "Hash point count is already empty: {sub_geo_hash}");
                    }
                } else {
                    debug_assert!(
                        false,
                        "Hash point count is not found for hash: {sub_geo_hash}",
                    );
                };
            }
        }
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            Storage::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }

    /// Approximate RAM usage in bytes (cached at construction).
    pub fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    fn compute_ram_usage_bytes(&self) -> usize {
        let Self {
            counts_per_hash,
            points_map_hashes,
            points_map_offsets,
            points_map_ids,
            point_to_values,
            points_count: _,
            points_values_count: _,
            max_values_per_point: _,
            storage: _,
            cached_ram_usage_bytes: _,
        } = self;

        let cph_bytes = counts_per_hash.capacity() * size_of::<Counts>();
        let pm_hashes_bytes = points_map_hashes.capacity() * size_of::<GeoHash>();
        let pm_offsets_bytes = points_map_offsets.capacity() * size_of::<u32>();
        let pm_ids_bytes = points_map_ids.capacity() * size_of::<PointOffsetType>();
        cph_bytes
            + pm_hashes_bytes
            + pm_offsets_bytes
            + pm_ids_bytes
            + point_to_values.ram_usage_bytes()
    }
}
