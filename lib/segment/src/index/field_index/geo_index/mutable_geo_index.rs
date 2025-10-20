use std::cmp::max;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::AHashSet;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use delegate::delegate;
use gridstore::Gridstore;
use gridstore::config::StorageOptions;
use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;

#[cfg(feature = "rocksdb")]
use super::GeoMapIndex;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_buffered_delete_wrapper::DatabaseColumnScheduledDeleteWrapper;
#[cfg(feature = "rocksdb")]
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::index::field_index::geo_hash::{GeoHash, encode_max_precision};
use crate::index::payload_config::StorageType;
use crate::types::{GeoPoint, RawGeoPoint};

/// Default options for Gridstore storage
const GRIDSTORE_OPTIONS: StorageOptions = StorageOptions {
    // Size of geo point values in index
    block_size_bytes: Some(size_of::<RawGeoPoint>()),
    // Compressing geo point values is unreasonable
    compression: Some(gridstore::config::Compression::None),
    // Scale page size down with block size, prevents overhead of first page when there's (almost) no values
    page_size_bytes: Some(size_of::<RawGeoPoint>() * 8192 * 32), // 4 to 8 MiB = block_size * region_blocks * regions,
    region_size_blocks: None,
};

pub struct MutableGeoMapIndex {
    in_memory_index: InMemoryGeoMapIndex,
    storage: Storage,
}

enum Storage {
    #[cfg(feature = "rocksdb")]
    RocksDb(DatabaseColumnScheduledDeleteWrapper),
    Gridstore(Arc<RwLock<Gridstore<Vec<RawGeoPoint>>>>),
}

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

impl MutableGeoMapIndex {
    /// Open and load mutable geo index from RocksDB storage
    #[cfg(feature = "rocksdb")]
    pub fn open_rocksdb(
        db: Arc<RwLock<DB>>,
        store_cf_name: &str,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let db_wrapper = DatabaseColumnScheduledDeleteWrapper::new(DatabaseColumnWrapper::new(
            db,
            store_cf_name,
        ));

        if !db_wrapper.has_column_family()? {
            if create_if_missing {
                db_wrapper.recreate_column_family()?;
            } else {
                // Column family doesn't exist, cannot load
                return Ok(None);
            }
        };

        // Load in-memory index from RocksDB
        let mut in_memory_index = InMemoryGeoMapIndex::new();
        let mut points_to_hashes: BTreeMap<PointOffsetType, Vec<GeoHash>> = Default::default();

        for (key, value) in db_wrapper.lock_db().iter()? {
            let (geo_hash, idx) = GeoMapIndex::decode_db_key(key)?;
            let geo_point = GeoMapIndex::decode_db_value(value)?;

            if in_memory_index.point_to_values.len() <= idx as usize {
                in_memory_index
                    .point_to_values
                    .resize_with(idx as usize + 1, Vec::new);
            }

            if in_memory_index.point_to_values[idx as usize].is_empty() {
                in_memory_index.points_count += 1;
            }

            points_to_hashes.entry(idx).or_default().push(geo_hash);

            in_memory_index.point_to_values[idx as usize].push(geo_point);
            in_memory_index
                .points_map
                .entry(geo_hash)
                .or_default()
                .insert(idx);

            in_memory_index.points_values_count += 1;
        }

        for (_idx, geo_hashes) in points_to_hashes {
            in_memory_index.max_values_per_point =
                max(in_memory_index.max_values_per_point, geo_hashes.len());
            in_memory_index.increment_hash_point_counts(&geo_hashes);
            for geo_hash in geo_hashes {
                in_memory_index.increment_hash_value_counts(&geo_hash);
            }
        }

        Ok(Some(Self {
            in_memory_index,
            storage: Storage::RocksDb(db_wrapper),
        }))
    }

    /// Open and load mutable geo index from Gridstore storage
    ///
    /// The `create_if_missing` parameter indicates whether to create a new Gridstore if it does
    /// not exist. If false and files don't exist, the load function will indicate nothing could be
    /// loaded.
    pub fn open_gridstore(path: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        let store = if create_if_missing {
            Gridstore::open_or_create(path, GRIDSTORE_OPTIONS).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable geo index on gridstore: {err}"
                ))
            })?
        } else if path.exists() {
            Gridstore::open(path).map_err(|err| {
                OperationError::service_error(format!(
                    "failed to open mutable geo index on gridstore: {err}"
                ))
            })?
        } else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        // Load in-memory index from Gridstore
        let mut in_memory_index = InMemoryGeoMapIndex::new();
        let hw_counter = HardwareCounterCell::disposable();
        let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
        store
            .iter::<_, OperationError>(
                |idx, values: Vec<RawGeoPoint>| {
                    let geo_points = values.into_iter().map(GeoPoint::from).collect::<Vec<_>>();
                    let geo_hashes = geo_points
                        .iter()
                        .map(|geo_point| {
                            encode_max_precision(geo_point.lon.0, geo_point.lat.0).map_err(|e| {
                                OperationError::service_error(format!("Malformed geo points: {e}"))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    for geo_point in geo_points {
                        if in_memory_index.point_to_values.len() <= idx as usize {
                            in_memory_index
                                .point_to_values
                                .resize_with(idx as usize + 1, Vec::new);
                        }

                        if in_memory_index.point_to_values[idx as usize].is_empty() {
                            in_memory_index.points_count += 1;
                        }

                        in_memory_index.point_to_values[idx as usize].push(geo_point);
                        in_memory_index.points_values_count += 1;
                    }

                    in_memory_index.max_values_per_point =
                        max(in_memory_index.max_values_per_point, geo_hashes.len());
                    in_memory_index.increment_hash_point_counts(&geo_hashes);
                    for geo_hash in geo_hashes {
                        in_memory_index.increment_hash_value_counts(&geo_hash);
                        in_memory_index
                            .points_map
                            .entry(geo_hash)
                            .or_default()
                            .insert(idx);
                    }

                    Ok(true)
                },
                hw_counter_ref,
            )
            .map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to load mutable geo index from gridstore: {err}"
                ))
            })?;

        Ok(Some(Self {
            in_memory_index,
            storage: Storage::Gridstore(Arc::new(RwLock::new(store))),
        }))
    }

    #[cfg_attr(not(feature = "rocksdb"), expect(dead_code))]
    #[inline]
    pub(super) fn clear(&self) -> OperationResult<()> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.recreate_column_family(),
            Storage::Gridstore(store) => store.write().clear().map_err(|err| {
                OperationError::service_error(format!("Failed to clear mutable geo index: {err}",))
            }),
        }
    }

    #[inline]
    pub(super) fn wipe(self) -> OperationResult<()> {
        match self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.remove_column_family(),
            Storage::Gridstore(store) => {
                let store =
                    Arc::into_inner(store).expect("exclusive strong reference to Gridstore");

                store.into_inner().wipe().map_err(|err| {
                    OperationError::service_error(format!(
                        "Failed to wipe mutable geo index: {err}",
                    ))
                })
            }
        }
    }

    /// Clear cache
    ///
    /// Only clears cache of Gridstore storage if used. Does not clear in-memory representation of
    /// index.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => Ok(()),
            Storage::Gridstore(index) => index.read().clear_cache().map_err(|err| {
                OperationError::service_error(format!(
                    "Failed to clear mutable geo index gridstore cache: {err}"
                ))
            }),
        }
    }

    #[inline]
    pub(super) fn files(&self) -> Vec<PathBuf> {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => vec![],
            Storage::Gridstore(store) => store.read().files(),
        }
    }

    #[inline]
    pub(super) fn flusher(&self) -> Flusher {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => db_wrapper.flusher(),
            Storage::Gridstore(store) => {
                let store = Arc::downgrade(store);
                Box::new(move || {
                    store
                        .upgrade()
                        .ok_or_else(|| {
                            OperationError::service_error(
                                "Failed to flush mutable numeric index, backing Gridstore storage is already dropped",
                            )
                        })?
                        .read()
                        .flush()
                        .map_err(|err| {
                            OperationError::service_error(format!(
                                "Failed to flush mutable geo index gridstore: {err}"
                            ))
                        })
                })
            }
        }
    }

    pub fn add_many_geo_points(
        &mut self,
        idx: PointOffsetType,
        values: &[GeoPoint],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        // Update persisted storage
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                for added_point in values {
                    let added_geo_hash: GeoHash =
                        encode_max_precision(added_point.lon.0, added_point.lat.0).map_err(
                            |e| OperationError::service_error(format!("Malformed geo points: {e}")),
                        )?;

                    let key = GeoMapIndex::encode_db_key(added_geo_hash, idx);
                    let value = GeoMapIndex::encode_db_value(added_point);

                    db_wrapper.put(&key, value)?;
                }
            }
            // We cannot store empty value, then delete instead
            Storage::Gridstore(store) if values.is_empty() => {
                store.write().delete_value(idx);
            }
            Storage::Gridstore(store) => {
                let hw_counter_ref = hw_counter.ref_payload_index_io_write_counter();
                let values = values
                    .iter()
                    .cloned()
                    .map(RawGeoPoint::from)
                    .collect::<Vec<_>>();
                store
                    .write()
                    .put_value(idx, &values, hw_counter_ref)
                    .map_err(|err| {
                        OperationError::service_error(format!(
                            "failed to put value in mutable geo index gridstore: {err}"
                        ))
                    })?;
            }
        }

        self.in_memory_index
            .add_many_geo_points(idx, values, hw_counter)
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        // Update persisted storage
        match &mut self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(db_wrapper) => {
                let Some(geo_points_to_remove) =
                    self.in_memory_index.point_to_values.get(idx as usize)
                else {
                    return Ok(());
                };

                for removed_geo_point in geo_points_to_remove {
                    let geo_hash_to_remove: GeoHash =
                        encode_max_precision(removed_geo_point.lon.0, removed_geo_point.lat.0)
                            .map_err(|e| {
                                OperationError::service_error(format!("Malformed geo points: {e}"))
                            })?;
                    let key = GeoMapIndex::encode_db_key(geo_hash_to_remove, idx);
                    db_wrapper.remove(&key)?;
                }
            }
            Storage::Gridstore(store) => {
                store.write().delete_value(idx);
            }
        }

        self.in_memory_index.remove_point(idx)
    }

    pub fn points_count(&self) -> usize {
        self.in_memory_index.points_count
    }

    pub fn points_values_count(&self) -> usize {
        self.in_memory_index.points_values_count
    }

    pub fn max_values_per_point(&self) -> usize {
        self.in_memory_index.max_values_per_point
    }

    pub fn into_in_memory_index(self) -> InMemoryGeoMapIndex {
        self.in_memory_index
    }

    pub fn get_values(&self, idx: u32) -> Option<impl Iterator<Item = &GeoPoint> + '_> {
        self.in_memory_index
            .point_to_values
            .get(idx as usize)
            .map(|v| v.iter())
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self.storage {
            Storage::RocksDb(_) => true,
            Storage::Gridstore(_) => false,
        }
    }

    delegate! {
        to self.in_memory_index {
            pub fn check_values_any(&self, idx: PointOffsetType, check_fn: impl Fn(&GeoPoint) -> bool) -> bool;
            pub fn values_count(&self, idx: PointOffsetType) -> usize;
            pub fn points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)>;
            pub fn points_of_hash(&self, hash: &GeoHash) -> usize;
            pub fn values_of_hash(&self, hash: &GeoHash) -> usize;
            pub fn stored_sub_regions(
                &self,
                geo: GeoHash,
            ) -> impl Iterator<Item = PointOffsetType>;
        }
    }

    pub fn storage_type(&self) -> StorageType {
        match &self.storage {
            #[cfg(feature = "rocksdb")]
            Storage::RocksDb(_) => StorageType::RocksDb,
            Storage::Gridstore(_) => StorageType::Gridstore,
        }
    }
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

    pub fn points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)> {
        self.points_per_hash
            .iter()
            .map(|(hash, count)| (hash, *count))
    }

    pub fn points_of_hash(&self, hash: &GeoHash) -> usize {
        self.points_per_hash.get(hash).copied().unwrap_or(0)
    }

    pub fn values_of_hash(&self, hash: &GeoHash) -> usize {
        self.values_per_hash.get(hash).copied().unwrap_or(0)
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
                encode_max_precision(removed_geo_point.lon.0, removed_geo_point.lat.0).map_err(
                    |e| OperationError::service_error(format!("Malformed geo points: {e}")),
                )?;
            removed_geo_hashes.push(removed_geo_hash);

            let is_last = if let Some(hash_ids) = self.points_map.get_mut(&removed_geo_hash) {
                hash_ids.remove(&idx);
                hash_ids.is_empty()
            } else {
                log::warn!("Geo index error: no points for hash {removed_geo_hash} was found");
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

        for geo_hash in &geo_hashes {
            self.points_map
                .entry(geo_hash.to_owned())
                .or_default()
                .insert(idx);

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

    fn increment_hash_value_counts(&mut self, geo_hash: &GeoHash) {
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

    fn increment_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
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

    fn decrement_hash_value_counts(&mut self, geo_hash: &GeoHash) {
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

    fn decrement_hash_point_counts(&mut self, geo_hashes: &[GeoHash]) {
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
}
