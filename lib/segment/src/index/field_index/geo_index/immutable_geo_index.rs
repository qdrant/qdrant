use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;
use smol_str::SmolStr;

use super::mutable_geo_index::{InMemoryGeoMapIndex, MutableGeoMapIndex};
use super::GeoMapIndex;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::geo_hash::{encode_max_precision, GeoHash};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::types::GeoPoint;

#[derive(Clone, Debug)]
struct Counts {
    hash: GeoHash,
    points: u32,
    values: u32,
}

pub struct ImmutableGeoMapIndex {
    counts_per_hash: Vec<Counts>,
    points_map: Vec<(GeoHash, HashSet<PointOffsetType>)>,
    point_to_values: ImmutablePointToValues<GeoPoint>,
    points_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
    db_wrapper: DatabaseColumnScheduledDeleteWrapper,
}

impl ImmutableGeoMapIndex {
    pub fn new(db: Arc<RwLock<DB>>, store_cf_name: &str) -> Self {
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            store_cf_name,
        ));
        Self {
            counts_per_hash: Default::default(),
            points_map: Default::default(),
            point_to_values: Default::default(),
            points_count: 0,
            points_values_count: 0,
            max_values_per_point: 0,
            db_wrapper,
        }
    }

    pub fn db_wrapper(&self) -> &DatabaseColumnScheduledDeleteWrapper {
        &self.db_wrapper
    }

    pub fn files(&self) -> Vec<PathBuf> {
        Default::default()
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
        self.point_to_values.check_values_any(idx, check_fn)
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        self.point_to_values
            .get_values_count(idx)
            .unwrap_or_default()
    }

    pub fn points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)> {
        self.counts_per_hash
            .iter()
            .map(|counts| (&counts.hash, counts.points as usize))
    }

    pub fn points_of_hash(&self, hash: &GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(hash)) {
            self.counts_per_hash[index].points as usize
        } else {
            0
        }
    }

    pub fn values_of_hash(&self, hash: &GeoHash) -> usize {
        if let Ok(index) = self.counts_per_hash.binary_search_by(|x| x.hash.cmp(hash)) {
            self.counts_per_hash[index].values as usize
        } else {
            0
        }
    }

    pub fn load(&mut self) -> OperationResult<bool> {
        let mut mutable_geo_index = MutableGeoMapIndex::new(
            self.db_wrapper.get_database(),
            self.db_wrapper.get_column_name(),
        );
        let result = mutable_geo_index.load()?;

        let InMemoryGeoMapIndex {
            points_per_hash,
            values_per_hash,
            points_map,
            point_to_values,
            points_count,
            points_values_count,
            max_values_per_point,
            ..
        } = mutable_geo_index.into_in_memory_index();

        let mut counts_per_hash: BTreeMap<GeoHash, Counts> = Default::default();
        for (hash, points) in points_per_hash {
            counts_per_hash.insert(
                hash,
                Counts {
                    hash,
                    points: points as u32,
                    values: 0,
                },
            );
        }
        for (hash, values) in values_per_hash {
            if let Some(counts) = counts_per_hash.get_mut(&hash) {
                counts.values = values as u32;
            } else {
                counts_per_hash.insert(
                    hash,
                    Counts {
                        hash,
                        points: 0,
                        values: values as u32,
                    },
                );
            }
        }

        self.counts_per_hash = counts_per_hash.values().cloned().collect();
        self.points_map = points_map.iter().map(|(k, v)| (*k, v.clone())).collect();
        self.point_to_values = ImmutablePointToValues::new(point_to_values);
        self.points_count = points_count;
        self.points_values_count = points_values_count;
        self.max_values_per_point = max_values_per_point;

        Ok(result)
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
                encode_max_precision(removed_geo_point.lon, removed_geo_point.lat).unwrap();
            removed_geo_hashes.push(removed_geo_hash);

            let key = GeoMapIndex::encode_db_key(removed_geo_hash, idx);
            self.db_wrapper.remove(key.as_bytes())?;

            if let Ok(index) = self
                .points_map
                .binary_search_by(|x| x.0.cmp(&removed_geo_hash))
            {
                self.points_map[index].1.remove(&idx);
            } else {
                log::warn!(
                    "Geo index error: no points for hash {} was found",
                    SmolStr::from(removed_geo_hash),
                );
            };

            self.decrement_hash_value_counts(&removed_geo_hash);
        }

        self.decrement_hash_point_counts(&removed_geo_hashes);
        Ok(())
    }

    /// Returns an iterator over all point IDs which have the `geohash` prefix.
    /// Note. Point ID may be repeated multiple times in the iterator.
    pub fn stored_sub_regions(&self, geo: &GeoHash) -> impl Iterator<Item = PointOffsetType> + '_ {
        let geo_clone = *geo;
        let start_index = self
            .points_map
            .binary_search_by(|(p, _h)| p.cmp(geo))
            .unwrap_or_else(|index| index);
        self.points_map[start_index..]
            .iter()
            .take_while(move |(p, _h)| p.starts_with(geo_clone))
            .flat_map(|(_, points)| points.iter().copied())
    }

    fn decrement_hash_value_counts(&mut self, geo_hash: &GeoHash) {
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
                    debug_assert!(
                        false,
                        "Hash value count is already empty: {}",
                        SmolStr::from(sub_geo_hash),
                    );
                }
            } else {
                debug_assert!(
                    false,
                    "Hash value count is not found for hash: {}",
                    SmolStr::from(sub_geo_hash),
                );
            }
        }
    }

    fn decrement_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
        let mut seen_hashes: HashSet<GeoHash> = Default::default();
        for geo_hash in geo_hashes {
            for i in 0..=geo_hash.len() {
                let sub_geo_hash = geo_hash.truncate(i);
                if seen_hashes.contains(&sub_geo_hash) {
                    continue;
                }
                seen_hashes.insert(sub_geo_hash);
                if let Ok(index) = self
                    .counts_per_hash
                    .binary_search_by(|x| x.hash.cmp(&sub_geo_hash))
                {
                    let points_count = self.counts_per_hash[index].points;
                    if points_count > 0 {
                        self.counts_per_hash[index].points = points_count - 1;
                    } else {
                        debug_assert!(
                            false,
                            "Hash point count is already empty: {}",
                            SmolStr::from(sub_geo_hash),
                        );
                    }
                } else {
                    debug_assert!(
                        false,
                        "Hash point count is not found for hash: {}",
                        SmolStr::from(sub_geo_hash),
                    );
                };
            }
        }
    }
}
