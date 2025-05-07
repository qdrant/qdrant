use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashSet;
use common::types::PointOffsetType;
use parking_lot::RwLock;
use rocksdb::DB;

use super::GeoMapIndex;
use super::mmap_geo_index::MmapGeoMapIndex;
use super::mutable_geo_index::{InMemoryGeoMapIndex, MutableGeoMapIndex};
use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::geo_hash::{GeoHash, encode_max_precision};
use crate::index::field_index::immutable_point_to_values::ImmutablePointToValues;
use crate::types::GeoPoint;

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
            hash,
            points,
            values,
        }
    }
}

pub struct ImmutableGeoMapIndex {
    counts_per_hash: Vec<Counts>,
    points_map: Vec<(GeoHash, AHashSet<PointOffsetType>)>,
    point_to_values: ImmutablePointToValues<GeoPoint>,
    points_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
    // Backing s torage, source of state, persists deletions
    storage: Storage,
}

enum Storage {
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Mmap(Box<MmapGeoMapIndex>),
}

impl ImmutableGeoMapIndex {
    /// Open immutable geo index from RocksDB storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_rocksdb(db: Arc<RwLock<DB>>, store_cf_name: &str) -> Self {
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
            storage: Storage::RocksDb(db_wrapper),
        }
    }

    /// Open immutable geo index from mmap storage
    ///
    /// Note: after opening, the data must be loaded into memory separately using [`load`].
    pub fn open_mmap(index: MmapGeoMapIndex) -> Self {
        Self {
            counts_per_hash: Default::default(),
            points_map: Default::default(),
            point_to_values: Default::default(),
            points_count: 0,
            points_values_count: 0,
            max_values_per_point: 0,
            storage: Storage::Mmap(Box::new(index)),
        }
    }

    /// Load storage
    ///
    /// Loads in-memory index from backing RocksDB or mmap storage.
    pub fn load(&mut self) -> OperationResult<bool> {
        match self.storage {
            Storage::RocksDb(_) => self.load_rocksdb(),
            Storage::Mmap(_) => Ok(self.load_mmap()),
        }
    }

    /// Load from RocksDB storage
    ///
    /// Loads in-memory index from RocksDB storage.
    fn load_rocksdb(&mut self) -> OperationResult<bool> {
        let Storage::RocksDb(db_wrapper) = &self.storage else {
            return Ok(false);
        };

        let mut mutable_geo_index =
            MutableGeoMapIndex::new(db_wrapper.get_database(), db_wrapper.get_column_name());
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

    /// Load from mmap storage
    ///
    /// Loads in-memory index fmmap RocksDB storage.
    fn load_mmap(&mut self) -> bool {
        let Storage::Mmap(index) = &self.storage else {
            return false;
        };

        self.points_count = index.points_count();
        self.points_values_count = index.points_values_count();
        self.max_values_per_point = index.max_values_per_point();
        self.counts_per_hash = index
            .counts_per_hash
            .iter()
            .copied()
            .map(Counts::from)
            .collect();

        // Get points per geo hash and filter deleted points
        self.points_map = index
            .points_map
            .iter()
            .copied()
            .map(|item| {
                let super::mmap_geo_index::PointKeyValue {
                    hash,
                    ids_start,
                    ids_end,
                } = item;
                (
                    hash,
                    index.points_map_ids[ids_start as usize..ids_end as usize]
                        .iter()
                        .copied()
                        // Filter deleted points
                        .filter(|id| !index.deleted.get(*id as usize).unwrap_or_default())
                        .collect(),
                )
            })
            .collect();

        // Get point values and filter deleted points
        // Track deleted points to adjust point and value counts after loading
        let mut deleted_points: Vec<(PointOffsetType, Vec<GeoPoint>)> =
            Vec::with_capacity(index.deleted_count);
        self.point_to_values = ImmutablePointToValues::new(
            index
                .point_to_values
                .iter()
                .map(|(id, values)| {
                    let is_deleted = index.deleted.get(id as usize).unwrap_or_default();
                    match (is_deleted, values) {
                        (false, Some(values)) => values.into_iter().collect(),
                        (false, None) => vec![],
                        (true, Some(values)) => {
                            let geo_points: Vec<GeoPoint> = values.collect();
                            deleted_points.push((id, geo_points));
                            vec![]
                        }
                        (true, None) => {
                            deleted_points.push((id, vec![]));
                            vec![]
                        }
                    }
                })
                .collect(),
        );

        // Index is now loaded into memory, clear cache of backing mmap storage
        if let Err(err) = index.clear_cache() {
            log::warn!("Failed to clear mmap cache of ram mmap geo index: {err}");
        }
        let _ = index;

        // Update point and value counts based on deleted points
        for (_idx, removed_geo_points) in deleted_points {
            self.points_values_count -= removed_geo_points.len();

            let removed_geo_hashes: Vec<_> = removed_geo_points
                .into_iter()
                .map(|geo_point| encode_max_precision(geo_point.lon, geo_point.lat).unwrap())
                .collect();
            for removed_geo_hash in &removed_geo_hashes {
                self.decrement_hash_value_counts(removed_geo_hash);
            }
            self.decrement_hash_point_counts(&removed_geo_hashes);
        }

        true
    }

    #[cfg(test)]
    pub fn db_wrapper(&self) -> Option<&DatabaseColumnScheduledDeleteWrapper> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => Some(db_wrapper),
            Storage::Mmap(_) => None,
        }
    }

    pub fn files(&self) -> Vec<PathBuf> {
        match self.storage {
            Storage::RocksDb(_) => vec![],
            Storage::Mmap(ref index) => index.files(),
        }
    }

    pub fn clear(self) -> OperationResult<()> {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Mmap(index) => index.clear(),
        }
    }

    pub fn flusher(&self) -> Flusher {
        match self.storage {
            Storage::RocksDb(ref db_wrapper) => db_wrapper.flusher(),
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

            match self.storage {
                Storage::RocksDb(ref db_wrapper) => {
                    let key = GeoMapIndex::encode_db_key(removed_geo_hash, idx);
                    db_wrapper.remove(&key)?;
                }
                Storage::Mmap(ref mut index) => {
                    index.remove_point(idx);
                }
            }

            if let Ok(index) = self
                .points_map
                .binary_search_by(|x| x.0.cmp(&removed_geo_hash))
            {
                self.points_map[index].1.remove(&idx);
            } else {
                log::warn!("Geo index error: no points for hash {removed_geo_hash} were found");
            };

            self.decrement_hash_value_counts(&removed_geo_hash);
        }

        self.decrement_hash_point_counts(&removed_geo_hashes);
        Ok(())
    }

    /// Returns an iterator over all point IDs which have the `geohash` prefix.
    /// Note. Point ID may be repeated multiple times in the iterator.
    pub fn stored_sub_regions(&self, geo: GeoHash) -> impl Iterator<Item = PointOffsetType> {
        let start_index = self
            .points_map
            .binary_search_by(|(p, _h)| p.cmp(&geo))
            .unwrap_or_else(|index| index);
        self.points_map[start_index..]
            .iter()
            .take_while(move |(p, _h)| p.starts_with(geo))
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
        let mut seen_hashes: AHashSet<GeoHash> = Default::default();
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
}
