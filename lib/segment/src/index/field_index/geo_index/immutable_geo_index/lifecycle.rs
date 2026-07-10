use std::borrow::Cow;
use std::path::PathBuf;

use ahash::AHashSet;
use common::bitvec::BitSliceExt;
use common::generic_consts::Random;
use common::types::PointOffsetType;
use common::universal_io::{ReadRange, UniversalRead};

use super::super::on_disk_geo_index::OnDiskGeoIndex;
use super::{Counts, DELETED_SENTINEL, ImmutableGeoIndex};
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::{GeoHash, encode_max_precision};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::index::payload_config::StorageType;
use crate::types::GeoPoint;

impl<S: UniversalRead> ImmutableGeoIndex<S> {
    /// Open and load the immutable geo index from mmap storage.
    pub fn load_from_on_disk(index: OnDiskGeoIndex<S>) -> OperationResult<Self> {
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
                    if index.storage.deleted.is_active(id) {
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
            Vec::with_capacity(index.storage.deleted.deleted_count());
        // Batched reads only report non-empty points and may arrive out of
        // order, so we pre-fill with empty value lists and index by point id.
        let mut point_to_values: Vec<Vec<GeoPoint>> =
            vec![Vec::new(); index.storage.point_to_values.len()];
        index
            .storage
            .point_to_values
            .for_all_points_values(|id, values| {
                let geo_points: Vec<GeoPoint> = values.map(Cow::into_owned).collect();
                if !index.storage.deleted.is_active(id) {
                    deleted_points.push((id, geo_points));
                } else {
                    point_to_values[id as usize] = geo_points;
                }
            })?;
        let point_to_values = ImmutablePointToValues::new(point_to_values);

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
            storage: index,
            cached_ram_usage_bytes: 0,
        };

        // Update point and value counts based on deleted points
        for (_idx, removed_geo_points) in deleted_points {
            index.points_values_count = index
                .points_values_count
                .saturating_sub(removed_geo_points.len());

            let mut removed_geo_hashes = Vec::with_capacity(removed_geo_points.len());
            for geo_point in removed_geo_points {
                removed_geo_hashes.push(encode_max_precision(geo_point.lon.0, geo_point.lat.0)?);
            }
            for &removed_geo_hash in &removed_geo_hashes {
                index.decrement_hash_value_counts(removed_geo_hash);
            }
            index.decrement_hash_point_counts(&removed_geo_hashes);
        }

        index.cached_ram_usage_bytes = index.compute_ram_usage_bytes();
        Ok(index)
    }

    pub fn files(&self) -> Vec<PathBuf> {
        self.storage.files()
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        self.storage.immutable_files()
    }

    /// Clear cache
    ///
    /// Only clears cache of mmap storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        self.storage.clear_cache().map_err(|err| {
            OperationError::service_error(format!(
                "Failed to clear immutable geo index gridstore cache: {err}"
            ))
        })
    }

    pub fn wipe(self) -> OperationResult<()> {
        self.storage.wipe()
    }

    pub fn flusher(&self) -> Flusher {
        self.storage.flusher()
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

        self.points_count = self.points_count.saturating_sub(1);
        self.points_values_count = self
            .points_values_count
            .saturating_sub(removed_geo_points.len());
        let mut removed_geo_hashes = Vec::with_capacity(removed_geo_points.len());

        for removed_geo_point in removed_geo_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_geo_point.lon.0, removed_geo_point.lat.0)?;
            removed_geo_hashes.push(removed_geo_hash);

            self.storage.remove_point(idx);

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

    pub(super) fn decrement_hash_value_counts(&mut self, geo_hash: GeoHash) {
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

    pub(super) fn decrement_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: AHashSet<GeoHash> = AHashSet::default();
        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = geo_hash.truncate(i);
                if !seen_hashes.insert(sub_geo_hash) {
                    continue;
                }
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
        StorageType::Mmap { is_on_disk: false }
    }

    /// Approximate RAM usage in bytes (cached at construction).
    pub fn ram_usage_bytes(&self) -> usize {
        self.cached_ram_usage_bytes
    }

    pub(super) fn compute_ram_usage_bytes(&self) -> usize {
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
