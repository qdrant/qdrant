use std::collections::BTreeMap;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::{GeoHash, encode_max_precision};
use crate::types::GeoPoint;

/// In-memory state shared by [`super::MutableGeoMapIndex`] and
/// [`super::read_only::ReadOnlyAppendableGeoMapIndex`].
///
/// Both wrappers add a different backing storage (`Gridstore` vs
/// `GridstoreReader`); the in-memory layout that serves every
/// [`super::super::read_ops::GeoMapIndexRead`] method is the same, so it
/// lives here once.
pub struct InMemoryGeoMapIndex {
    /*
    {
        "d": 10,
        "dr": 10,
        "dr5": 4,
        "dr5r": 3,
        "dr5ru": 1,
        "dr5rr": 2,
        ...
    }
     */
    pub points_per_hash: BTreeMap<GeoHash, usize>,
    pub values_per_hash: BTreeMap<GeoHash, usize>,
    /*
    {
        "dr5ru": {1},
        "dr5rr": {2, 3},
        ...
    }
     */
    pub points_map: BTreeMap<GeoHash, AHashSet<PointOffsetType>>,
    pub point_to_values: Vec<Vec<GeoPoint>>,
    pub points_count: usize,
    pub points_values_count: usize,
    pub max_values_per_point: usize,
}

impl Default for InMemoryGeoMapIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryGeoMapIndex {
    pub fn new() -> Self {
        Self {
            points_per_hash: Default::default(),
            values_per_hash: Default::default(),
            points_map: Default::default(),
            point_to_values: vec![],
            points_count: 0,
            points_values_count: 0,
            max_values_per_point: 0,
        }
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        self.point_to_values
            .get(idx as usize)
            .map(|values| values.iter().any(check_fn))
            .unwrap_or(false)
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.point_to_values
            .get(idx as usize)
            .map(Vec::len)
            .unwrap_or_default()
    }

    pub fn points_per_hash(&self) -> impl Iterator<Item = (GeoHash, usize)> + '_ {
        self.points_per_hash
            .iter()
            .map(|(&hash, &count)| (hash, count))
    }

    pub fn points_of_hash(&self, hash: GeoHash) -> usize {
        self.points_per_hash.get(&hash).copied().unwrap_or(0)
    }

    pub fn values_of_hash(&self, hash: GeoHash) -> usize {
        self.values_per_hash.get(&hash).copied().unwrap_or(0)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(()); // Already removed or never actually existed
        }

        let removed_geo_points = std::mem::take(&mut self.point_to_values[idx as usize]);

        if removed_geo_points.is_empty() {
            return Ok(());
        }

        self.points_count -= 1;
        self.points_values_count -= removed_geo_points.len();
        let mut removed_geo_hashes = AHashSet::with_capacity(removed_geo_points.len());

        for removed_geo_point in removed_geo_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_geo_point.lon.0, removed_geo_point.lat.0).map_err(
                    |e| OperationError::service_error(format!("Malformed geo points: {e}")),
                )?;
            if !removed_geo_hashes.insert(removed_geo_hash) {
                continue;
            }

            let is_last = if let Some(hash_ids) = self.points_map.get_mut(&removed_geo_hash) {
                hash_ids.remove(&idx);
                hash_ids.is_empty()
            } else {
                debug_assert!(
                    false,
                    "Geo index error: no points for hash {removed_geo_hash} was found",
                );
                false
            };

            if is_last {
                self.points_map.remove(&removed_geo_hash);
            }

            self.decrement_hash_value_counts(removed_geo_hash);
        }

        self.decrement_hash_point_counts(removed_geo_hashes);
        Ok(())
    }

    pub fn add_many_geo_points(
        &mut self,
        idx: PointOffsetType,
        values: &[GeoPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        if self.point_to_values.len() <= idx as usize {
            // That's a smart reallocation
            self.point_to_values.resize_with(idx as usize + 1, Vec::new);
        }

        self.point_to_values[idx as usize] = values.to_vec();

        let mut geo_hashes = vec![];

        let mut hw_cell_wb = hw_counter
            .payload_index_io_write_counter()
            .write_back_counter();

        for added_point in values {
            let added_geo_hash: GeoHash =
                encode_max_precision(added_point.lon.0, added_point.lat.0).map_err(|e| {
                    OperationError::service_error(format!("Malformed geo points: {e}"))
                })?;

            hw_cell_wb.incr_delta(size_of_val(&added_geo_hash));

            geo_hashes.push(added_geo_hash);
        }

        for &geo_hash in &geo_hashes {
            self.points_map.entry(geo_hash).or_default().insert(idx);

            self.increment_hash_value_counts(geo_hash);
        }

        hw_cell_wb.incr_delta(geo_hashes.len() * size_of::<PointOffsetType>());

        self.increment_hash_point_counts(&geo_hashes);

        self.points_values_count += values.len();
        self.points_count += 1;
        self.max_values_per_point = self.max_values_per_point.max(values.len());
        Ok(())
    }

    /// Returns an iterator over all point IDs which have the `geohash` prefix.
    /// Note. Point ID may be repeated multiple times in the iterator.
    pub fn stored_sub_regions(&self, geo: GeoHash) -> impl Iterator<Item = PointOffsetType> + '_ {
        self.points_map
            .range(geo..)
            .take_while(move |(p, _h)| p.starts_with(geo))
            .flat_map(|(_, points)| points.iter().copied())
    }

    pub(super) fn increment_hash_value_counts(&mut self, geo_hash: GeoHash) {
        for i in 0..=geo_hash.len() {
            let sub_geo_hash = geo_hash.truncate(i);
            match self.values_per_hash.get_mut(&sub_geo_hash) {
                None => {
                    self.values_per_hash.insert(sub_geo_hash, 1);
                }
                Some(count) => {
                    *count += 1;
                }
            };
        }
    }

    pub(super) fn increment_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: AHashSet<GeoHash> = Default::default();

        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = geo_hash.truncate(i);
                if seen_hashes.contains(&sub_geo_hash) {
                    continue;
                }
                seen_hashes.insert(sub_geo_hash);
                match self.points_per_hash.get_mut(&sub_geo_hash) {
                    None => {
                        self.points_per_hash.insert(sub_geo_hash, 1);
                    }
                    Some(count) => {
                        *count += 1;
                    }
                };
            }
        }
    }

    fn decrement_hash_value_counts(&mut self, geo_hash: GeoHash) {
        for i in 0..=geo_hash.len() {
            let sub_geo_hash = geo_hash.truncate(i);
            match self.values_per_hash.get_mut(&sub_geo_hash) {
                None => {
                    debug_assert!(
                        false,
                        "Hash value count is not found for hash: {sub_geo_hash}",
                    );
                    self.values_per_hash.insert(sub_geo_hash, 0);
                }
                Some(count) => {
                    *count -= 1;
                }
            };
        }
    }

    fn decrement_hash_point_counts(&mut self, geo_hashes: impl IntoIterator<Item = GeoHash>) {
        let mut seen_hashes: AHashSet<GeoHash> = Default::default();
        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = geo_hash.truncate(i);
                if seen_hashes.contains(&sub_geo_hash) {
                    continue;
                }
                seen_hashes.insert(sub_geo_hash);
                match self.points_per_hash.get_mut(&sub_geo_hash) {
                    None => {
                        debug_assert!(
                            false,
                            "Hash point count is not found for hash: {sub_geo_hash}",
                        );
                        self.points_per_hash.insert(sub_geo_hash, 0);
                    }
                    Some(count) => {
                        *count -= 1;
                    }
                };
            }
        }
    }

    /// Approximate RAM usage in bytes.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            points_per_hash,
            values_per_hash,
            points_map,
            point_to_values,
            points_count: _,
            points_values_count: _,
            max_values_per_point: _,
        } = self;

        let btree_entry_overhead = std::mem::size_of::<usize>() * 3;
        let pph_bytes = points_per_hash.len()
            * (std::mem::size_of::<GeoHash>()
                + std::mem::size_of::<usize>()
                + btree_entry_overhead);
        let vph_bytes = values_per_hash.len()
            * (std::mem::size_of::<GeoHash>()
                + std::mem::size_of::<usize>()
                + btree_entry_overhead);
        // points_map: BTreeMap entries + AHashSet per entry
        let hashset_entry_overhead = std::mem::size_of::<u64>() + std::mem::size_of::<usize>();
        let pm_bytes: usize = points_map
            .values()
            .map(|set| {
                std::mem::size_of::<GeoHash>()
                    + std::mem::size_of::<AHashSet<PointOffsetType>>()
                    + btree_entry_overhead
                    + set.capacity()
                        * (std::mem::size_of::<PointOffsetType>() + hashset_entry_overhead)
            })
            .sum();
        let ptv_bytes: usize = point_to_values.capacity() * std::mem::size_of::<Vec<GeoPoint>>()
            + point_to_values
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<GeoPoint>())
                .sum::<usize>();
        pph_bytes + vph_bytes + pm_bytes + ptv_bytes
    }
}
