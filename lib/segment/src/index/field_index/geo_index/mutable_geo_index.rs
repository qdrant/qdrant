use std::cmp::max;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::GeoMapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::geo_hash::{encode_max_precision, GeoHash};
use crate::types::GeoPoint;

pub struct MutableGeoMapIndex {
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
    pub points_map: BTreeMap<GeoHash, HashSet<PointOffsetType>>,
    pub point_to_values: Vec<Vec<GeoPoint>>,
    pub points_count: usize,
    pub points_values_count: usize,
    pub max_values_per_point: usize,
    db_wrapper: DatabaseColumnWrapper,
}

impl MutableGeoMapIndex {
    pub fn new(db: Arc<RwLock<DB>>, store_cf_name: &str) -> Self {
        let db_wrapper = DatabaseColumnWrapper::new(db, store_cf_name);
        Self {
            points_per_hash: Default::default(),
            values_per_hash: Default::default(),
            points_map: Default::default(),
            point_to_values: vec![],
            points_count: 0,
            points_values_count: 0,
            max_values_per_point: 0,
            db_wrapper,
        }
    }

    pub fn db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[GeoPoint]> {
        self.point_to_values.get(idx as usize).map(Vec::as_slice)
    }

    pub fn get_points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)> {
        self.points_per_hash
            .iter()
            .map(|(hash, count)| (hash, *count))
    }

    pub fn get_points_of_hash(&self, hash: &GeoHash) -> usize {
        self.points_per_hash.get(hash).cloned().unwrap_or(0)
    }

    pub fn get_values_of_hash(&self, hash: &GeoHash) -> usize {
        self.values_per_hash.get(hash).cloned().unwrap_or(0)
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        let mut points_to_hashes: BTreeMap<PointOffsetType, Vec<GeoHash>> = Default::default();

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let key_str = std::str::from_utf8(&key).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;

            let (geo_hash, idx) = GeoMapIndex::decode_db_key(key_str)?;
            let geo_point = GeoMapIndex::decode_db_value(value)?;

            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize_with(idx as usize + 1, Vec::new);
            }

            if self.point_to_values[idx as usize].is_empty() {
                self.points_count += 1;
            }

            points_to_hashes
                .entry(idx)
                .or_default()
                .push(geo_hash.clone());

            self.point_to_values[idx as usize].push(geo_point);
            self.points_map
                .entry(geo_hash.clone())
                .or_default()
                .insert(idx);

            self.points_values_count += 1;
        }

        for (_idx, geo_hashes) in points_to_hashes.into_iter() {
            self.max_values_per_point = max(self.max_values_per_point, geo_hashes.len());
            self.increment_hash_point_counts(&geo_hashes);
            for geo_hash in geo_hashes {
                self.increment_hash_value_counts(&geo_hash);
            }
        }
        Ok(true)
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
        let mut removed_geo_hashes = Vec::with_capacity(removed_geo_points.len());

        for removed_geo_point in removed_geo_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_geo_point.lon, removed_geo_point.lat).unwrap();
            removed_geo_hashes.push(removed_geo_hash.clone());

            let key = GeoMapIndex::encode_db_key(&removed_geo_hash, idx);
            self.db_wrapper.remove(key)?;

            let is_last = if let Some(hash_ids) = self.points_map.get_mut(&removed_geo_hash) {
                hash_ids.remove(&idx);
                hash_ids.is_empty()
            } else {
                log::warn!(
                    "Geo index error: no points for hash {} was found",
                    removed_geo_hash
                );
                false
            };

            if is_last {
                self.points_map.remove(&removed_geo_hash);
            }

            self.decrement_hash_value_counts(&removed_geo_hash);
        }

        self.decrement_hash_point_counts(&removed_geo_hashes);
        Ok(())
    }

    pub fn add_many_geo_points(
        &mut self,
        idx: PointOffsetType,
        values: &[GeoPoint],
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

        for added_point in values {
            let added_geo_hash: GeoHash = encode_max_precision(added_point.lon, added_point.lat)
                .map_err(|e| OperationError::service_error(format!("Malformed geo points: {e}")))?;

            let key = GeoMapIndex::encode_db_key(&added_geo_hash, idx);
            let value = GeoMapIndex::encode_db_value(added_point);

            geo_hashes.push(added_geo_hash);

            self.db_wrapper.put(key, value)?;
        }

        for geo_hash in &geo_hashes {
            self.points_map
                .entry(geo_hash.to_owned())
                .or_default()
                .insert(idx);

            self.increment_hash_value_counts(geo_hash);
        }

        self.increment_hash_point_counts(&geo_hashes);

        self.points_values_count += values.len();
        self.points_count += 1;
        self.max_values_per_point = self.max_values_per_point.max(values.len());
        Ok(())
    }

    pub fn get_stored_sub_regions(
        &self,
        geo: &GeoHash,
    ) -> impl Iterator<Item = (&GeoHash, &HashSet<PointOffsetType>)> + '_ {
        let geo_clone = geo.clone();
        self.points_map
            .range(geo.clone()..)
            .take_while(move |(p, _h)| p.starts_with(geo_clone.as_str()))
    }

    fn increment_hash_value_counts(&mut self, geo_hash: &GeoHash) {
        for i in 0..=geo_hash.len() {
            let sub_geo_hash = &geo_hash[0..i];
            match self.values_per_hash.get_mut(sub_geo_hash) {
                None => {
                    self.values_per_hash.insert(sub_geo_hash.into(), 1);
                }
                Some(count) => {
                    *count += 1;
                }
            };
        }
    }

    fn increment_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: HashSet<&str> = Default::default();

        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = &geo_hash[0..i];
                if seen_hashes.contains(sub_geo_hash) {
                    continue;
                }
                seen_hashes.insert(sub_geo_hash);
                match self.points_per_hash.get_mut(sub_geo_hash) {
                    None => {
                        self.points_per_hash.insert(sub_geo_hash.into(), 1);
                    }
                    Some(count) => {
                        *count += 1;
                    }
                };
            }
        }
    }

    fn decrement_hash_value_counts(&mut self, geo_hash: &GeoHash) {
        for i in 0..=geo_hash.len() {
            let sub_geo_hash = &geo_hash[0..i];
            match self.values_per_hash.get_mut(sub_geo_hash) {
                None => {
                    debug_assert!(
                        false,
                        "Hash value count is not found for hash: {}",
                        sub_geo_hash
                    );
                    self.values_per_hash.insert(sub_geo_hash.into(), 0);
                }
                Some(count) => {
                    *count -= 1;
                }
            };
        }
    }

    fn decrement_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: HashSet<&str> = Default::default();
        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = &geo_hash[0..i];
                if seen_hashes.contains(sub_geo_hash) {
                    continue;
                }
                seen_hashes.insert(sub_geo_hash);
                match self.points_per_hash.get_mut(sub_geo_hash) {
                    None => {
                        debug_assert!(
                            false,
                            "Hash point count is not found for hash: {}",
                            sub_geo_hash
                        );
                        self.points_per_hash.insert(sub_geo_hash.into(), 0);
                    }
                    Some(count) => {
                        *count -= 1;
                    }
                };
            }
        }
    }
}
