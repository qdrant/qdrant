use std::cmp::{max, min};
#[cfg(feature = "rocksdb")]
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "rocksdb")]
use std::str::FromStr;
#[cfg(feature = "rocksdb")]
use std::sync::Arc;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use itertools::Itertools;
use mutable_geo_index::InMemoryGeoMapIndex;
#[cfg(feature = "rocksdb")]
use parking_lot::RwLock;
#[cfg(feature = "rocksdb")]
use rocksdb::DB;
use serde_json::Value;
#[cfg(feature = "rocksdb")]
use smallvec::SmallVec;

use self::immutable_geo_index::ImmutableGeoMapIndex;
use self::mmap_geo_index::MmapGeoMapIndex;
use self::mutable_geo_index::MutableGeoMapIndex;
use super::FieldIndexBuilderTrait;
use crate::common::Flusher;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::{
    GeoHash, circle_hashes, common_hash_prefix, geo_hash_to_box, polygon_hashes,
    polygon_hashes_estimation, rectangle_hashes,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::index::payload_config::{IndexMutability, StorageType};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType};

pub mod immutable_geo_index;
pub mod mmap_geo_index;
pub mod mutable_geo_index;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

pub enum GeoMapIndex {
    Mutable(MutableGeoMapIndex),
    Immutable(ImmutableGeoMapIndex),
    Mmap(Box<MmapGeoMapIndex>),
}

impl GeoMapIndex {
    #[cfg(feature = "rocksdb")]
    pub fn new_memory(
        db: Arc<RwLock<DB>>,
        field: &str,
        is_appendable: bool,
        create_if_missing: bool,
    ) -> OperationResult<Option<Self>> {
        let store_cf_name = GeoMapIndex::storage_cf_name(field);
        let index = if is_appendable {
            MutableGeoMapIndex::open_rocksdb(db, &store_cf_name, create_if_missing)?
                .map(GeoMapIndex::Mutable)
        } else {
            ImmutableGeoMapIndex::open_rocksdb(db, &store_cf_name)?.map(GeoMapIndex::Immutable)
        };
        Ok(index)
    }

    pub fn new_mmap(path: &Path, is_on_disk: bool) -> OperationResult<Option<Self>> {
        let Some(mmap_index) = MmapGeoMapIndex::open(path, is_on_disk)? else {
            // Files don't exist, cannot load
            return Ok(None);
        };

        let index = if is_on_disk {
            GeoMapIndex::Mmap(Box::new(mmap_index))
        } else {
            GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(mmap_index))
        };

        Ok(Some(index))
    }

    pub fn new_gridstore(dir: PathBuf, create_if_missing: bool) -> OperationResult<Option<Self>> {
        Ok(MutableGeoMapIndex::open_gridstore(dir, create_if_missing)?.map(GeoMapIndex::Mutable))
    }

    #[cfg(feature = "rocksdb")]
    pub fn builder(db: Arc<RwLock<DB>>, field: &str) -> OperationResult<GeoMapIndexBuilder> {
        let index = Self::new_memory(db, field, true, true)?.ok_or_else(|| {
            OperationError::service_error("Failed to open GeoMapIndex after creating it")
        })?;
        Ok(GeoMapIndexBuilder(index))
    }

    #[cfg(all(test, feature = "rocksdb"))]
    pub fn builder_immutable(
        db: Arc<RwLock<DB>>,
        field: &str,
    ) -> OperationResult<GeoMapImmutableIndexBuilder> {
        let index = Self::new_memory(db.clone(), field, true, true)?.ok_or_else(|| {
            OperationError::service_error("Failed to open GeoMapIndex after creating it")
        })?;
        Ok(GeoMapImmutableIndexBuilder {
            index,
            field: field.to_owned(),
            db,
        })
    }

    pub fn builder_mmap(path: &Path, is_on_disk: bool) -> GeoMapIndexMmapBuilder {
        GeoMapIndexMmapBuilder {
            path: path.to_owned(),
            in_memory_index: InMemoryGeoMapIndex::new(),
            is_on_disk,
        }
    }

    pub fn builder_gridstore(dir: PathBuf) -> GeoMapIndexGridstoreBuilder {
        GeoMapIndexGridstoreBuilder::new(dir)
    }

    fn points_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_count(),
            GeoMapIndex::Immutable(index) => index.points_count(),
            GeoMapIndex::Mmap(index) => index.points_count(),
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_values_count(),
            GeoMapIndex::Immutable(index) => index.points_values_count(),
            GeoMapIndex::Mmap(index) => index.points_values_count(),
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    fn max_values_per_point(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.max_values_per_point(),
            GeoMapIndex::Immutable(index) => index.max_values_per_point(),
            GeoMapIndex::Mmap(index) => index.max_values_per_point(),
        }
    }

    fn points_of_hash(&self, hash: &GeoHash, hw_counter: &HardwareCounterCell) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Mmap(index) => index.points_of_hash(hash, hw_counter),
        }
    }

    fn values_of_hash(&self, hash: &GeoHash, hw_counter: &HardwareCounterCell) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Mmap(index) => index.values_of_hash(hash, hw_counter),
        }
    }

    #[cfg(feature = "rocksdb")]
    fn storage_cf_name(field: &str) -> String {
        format!("{field}_geo")
    }

    /// Encode db key
    ///
    /// Maximum length is 23 bytes, e.g.: `dr5ruj4477kd/4294967295`
    #[cfg(feature = "rocksdb")]
    fn encode_db_key(value: GeoHash, idx: PointOffsetType) -> SmallVec<[u8; 23]> {
        let mut result = SmallVec::new();
        write!(result, "{value}/{idx}").unwrap();
        result
    }

    #[cfg(feature = "rocksdb")]
    fn decode_db_key<K>(s: K) -> OperationResult<(GeoHash, PointOffsetType)>
    where
        K: AsRef<[u8]>,
    {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let s = s.as_ref();
        let separator_pos = s
            .iter()
            .rposition(|b| b == &b'/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let geohash = &s[..separator_pos];
        let idx_bytes = &s[separator_pos + 1..];
        // Use `from_ascii_radix` here once stabilized instead of intermediate string reference
        let idx = PointOffsetType::from_str(std::str::from_utf8(idx_bytes).map_err(|_| {
            OperationError::service_error("Index load error: UTF8 error while DB parsing")
        })?)
        .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((GeoHash::new(geohash).map_err(OperationError::from)?, idx))
    }

    #[cfg(feature = "rocksdb")]
    fn decode_db_value<T: AsRef<[u8]>>(value: T) -> OperationResult<GeoPoint> {
        let lat_bytes = value.as_ref()[0..8]
            .try_into()
            .map_err(|_| OperationError::service_error("invalid lat encoding"))?;

        let lon_bytes = value.as_ref()[8..16]
            .try_into()
            .map_err(|_| OperationError::service_error("invalid lat encoding"))?;

        let lat = f64::from_be_bytes(lat_bytes);
        let lon = f64::from_be_bytes(lon_bytes);

        Ok(GeoPoint::new_unchecked(lon, lat))
    }

    #[cfg(feature = "rocksdb")]
    fn encode_db_value(value: &GeoPoint) -> [u8; 16] {
        let mut result: [u8; 16] = [0; 16];
        result[0..8].clone_from_slice(&value.lat.to_be_bytes());
        result[8..16].clone_from_slice(&value.lon.to_be_bytes());
        result
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        hw_counter: &HardwareCounterCell,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        match self {
            GeoMapIndex::Mutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Immutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Mmap(index) => index.check_values_any(idx, hw_counter, check_fn),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.values_count(idx),
            GeoMapIndex::Immutable(index) => index.values_count(idx),
            GeoMapIndex::Mmap(index) => index.values_count(idx),
        }
    }

    pub fn get_values(
        &self,
        idx: PointOffsetType,
    ) -> Option<Box<dyn Iterator<Item = GeoPoint> + '_>> {
        match self {
            GeoMapIndex::Mutable(index) => index.get_values(idx).map(|x| Box::new(x.cloned()) as _),
            GeoMapIndex::Immutable(index) => {
                index.get_values(idx).map(|x| Box::new(x.cloned()) as _)
            }
            GeoMapIndex::Mmap(index) => index.get_values(idx).map(|x| Box::new(x) as _),
        }
    }

    pub fn match_cardinality(
        &self,
        values: &[GeoHash],
        hw_counter: &HardwareCounterCell,
    ) -> CardinalityEstimation {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return CardinalityEstimation::exact(0);
        }

        let Some(common_hash) = common_hash_prefix(values) else {
            return CardinalityEstimation::exact(0);
        };

        let total_points = self.points_of_hash(&common_hash, hw_counter);
        let total_values = self.values_of_hash(&common_hash, hw_counter);

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|region| self.points_of_hash(region, hw_counter))
            .fold((0, 0), |(sum, maximum), count| {
                (sum + count, max(maximum, count))
            });

        // Assume all selected points have `max_values_per_point` value hits.
        // Therefore number of points can't be less than `total_hits / max_values_per_point`
        // Note: max_values_per_point is never zero here because we check it above
        let min_hits_by_value_groups = sum / max_values_per_point;

        // Assume that we have selected all possible duplications of the points
        let point_duplications = total_values - total_points;
        let possible_non_duplicated = sum.saturating_sub(point_duplications);

        let estimation_min = max(
            max(min_hits_by_value_groups, possible_non_duplicated),
            maximum_per_hash,
        );
        let estimation_max = min(sum, total_points);

        // estimate_multi_value_selection_cardinality might overflow at some corner cases
        // so it is better to limit its value with min and max
        let estimation_exp =
            estimate_multi_value_selection_cardinality(total_points, total_values, sum).round()
                as usize;

        CardinalityEstimation {
            primary_clauses: vec![],
            min: estimation_min,
            exp: min(estimation_max, max(estimation_min, estimation_exp)),
            max: estimation_max,
        }
    }

    pub fn get_telemetry_data(&self) -> PayloadIndexTelemetry {
        PayloadIndexTelemetry {
            field_name: None,
            points_count: self.points_count(),
            points_values_count: self.points_values_count(),
            histogram_bucket_size: None,
            index_type: match self {
                GeoMapIndex::Mutable(_) => "mutable_geo",
                GeoMapIndex::Immutable(_) => "immutable_geo",
                GeoMapIndex::Mmap(_) => "mmap_geo",
            },
        }
    }

    fn iterator(&self, values: Vec<GeoHash>) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            GeoMapIndex::Mutable(index) => Box::new(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(top_geo_hash))
                    .unique(),
            ),
            GeoMapIndex::Immutable(index) => Box::new(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(top_geo_hash))
                    .unique(),
            ),
            GeoMapIndex::Mmap(index) => Box::new(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(top_geo_hash))
                    .unique(),
            ),
        }
    }

    /// Get iterator over smallest geo-hash regions larger than `threshold` points
    fn large_hashes(&self, threshold: usize) -> impl Iterator<Item = (GeoHash, usize)> + '_ {
        let filter_condition =
            |(hash, size): &(GeoHash, usize)| *size > threshold && !hash.is_empty();
        let mut large_regions = match self {
            GeoMapIndex::Mutable(index) => index
                .points_per_hash()
                .map(|(&hash, size)| (hash, size))
                .filter(filter_condition)
                .collect_vec(),
            GeoMapIndex::Immutable(index) => index
                .points_per_hash()
                .map(|(&hash, size)| (hash, size))
                .filter(filter_condition)
                .collect_vec(),
            GeoMapIndex::Mmap(index) => index
                .points_per_hash()
                .filter(filter_condition)
                .collect_vec(),
        };

        // smallest regions first
        large_regions.sort_by(|a, b| b.cmp(a));

        let mut edge_region = vec![];

        let mut current_region = GeoHash::default();

        for (region, size) in large_regions {
            if !current_region.starts_with(region) {
                current_region = region;
                edge_region.push((region, size));
            }
        }

        edge_region.into_iter()
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }

    pub fn is_on_disk(&self) -> bool {
        match self {
            GeoMapIndex::Mutable(_) => false,
            GeoMapIndex::Immutable(_) => false,
            GeoMapIndex::Mmap(index) => index.is_on_disk(),
        }
    }

    #[cfg(feature = "rocksdb")]
    pub fn is_rocksdb(&self) -> bool {
        match self {
            GeoMapIndex::Mutable(index) => index.is_rocksdb(),
            GeoMapIndex::Immutable(index) => index.is_rocksdb(),
            GeoMapIndex::Mmap(_) => false,
        }
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(_) => {}   // Not a mmap
            GeoMapIndex::Immutable(_) => {} // Not a mmap
            GeoMapIndex::Mmap(index) => index.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            // Only clears backing mmap storage if used, not in-memory representation
            GeoMapIndex::Mutable(index) => index.clear_cache(),
            // Only clears backing mmap storage if used, not in-memory representation
            GeoMapIndex::Immutable(index) => index.clear_cache(),
            GeoMapIndex::Mmap(index) => index.clear_cache(),
        }
    }

    pub fn get_mutability_type(&self) -> IndexMutability {
        match self {
            Self::Mutable(_) => IndexMutability::Mutable,
            Self::Immutable(_) => IndexMutability::Immutable,
            Self::Mmap(_) => IndexMutability::Immutable,
        }
    }

    pub fn get_storage_type(&self) -> StorageType {
        match self {
            Self::Mutable(index) => index.storage_type(),
            Self::Immutable(index) => index.storage_type(),
            Self::Mmap(index) => StorageType::Mmap {
                is_on_disk: index.is_on_disk(),
            },
        }
    }
}

#[cfg(feature = "rocksdb")]
pub struct GeoMapIndexBuilder(GeoMapIndex);

#[cfg(feature = "rocksdb")]
impl FieldIndexBuilderTrait for GeoMapIndexBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        match &self.0 {
            GeoMapIndex::Mutable(index) => index.clear(),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Cannot use immutable index as a builder type",
            )),
            GeoMapIndex::Mmap(_) => Err(OperationError::service_error(
                "Cannot use mmap index as a builder type",
            )),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.0.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

#[cfg(all(test, feature = "rocksdb"))]
pub struct GeoMapImmutableIndexBuilder {
    index: GeoMapIndex,
    field: String,
    db: Arc<RwLock<DB>>,
}

#[cfg(all(test, feature = "rocksdb"))]
impl FieldIndexBuilderTrait for GeoMapImmutableIndexBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        match &self.index {
            GeoMapIndex::Mutable(index) => index.clear(),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Cannot use immutable index as a builder type",
            )),
            GeoMapIndex::Mmap(_) => Err(OperationError::service_error(
                "Cannot use mmap index as a builder type",
            )),
        }
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.index.add_point(id, payload, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        drop(self.index);
        let immutable_index = GeoMapIndex::new_memory(self.db, &self.field, false, false)?
            .ok_or_else(|| {
                OperationError::service_error("Failed to open GeoMapIndex after creating it")
            })?;
        Ok(immutable_index)
    }
}

pub struct GeoMapIndexMmapBuilder {
    path: PathBuf,
    in_memory_index: InMemoryGeoMapIndex,
    is_on_disk: bool,
}

impl FieldIndexBuilderTrait for GeoMapIndexMmapBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let values = payload
            .iter()
            .flat_map(|value| <GeoMapIndex as ValueIndexer>::get_values(value))
            .collect::<Vec<_>>();
        self.in_memory_index
            .add_many_geo_points(id, &values, hw_counter)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(GeoMapIndex::Mmap(Box::new(MmapGeoMapIndex::build(
            self.in_memory_index,
            &self.path,
            self.is_on_disk,
        )?)))
    }
}

impl ValueIndexer for GeoMapIndex {
    type ValueType = GeoPoint;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<GeoPoint>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.add_many_geo_points(id, &values, hw_counter),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable geo index",
            )),
            GeoMapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap geo index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<GeoPoint> {
        match value {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return GeoPoint::new(lon, lat).ok();
                }
                None
            }
            _ => None,
        }
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.remove_point(id),
            GeoMapIndex::Immutable(index) => index.remove_point(id),
            GeoMapIndex::Mmap(index) => {
                index.remove_point(id);
                Ok(())
            }
        }
    }
}

pub struct GeoMapIndexGridstoreBuilder {
    dir: PathBuf,
    index: Option<GeoMapIndex>,
}

impl GeoMapIndexGridstoreBuilder {
    fn new(dir: PathBuf) -> Self {
        Self { dir, index: None }
    }
}

impl FieldIndexBuilderTrait for GeoMapIndexGridstoreBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        assert!(
            self.index.is_none(),
            "index must be initialized exactly once",
        );
        self.index.replace(
            GeoMapIndex::new_gridstore(self.dir.clone(), true)?.ok_or_else(|| {
                OperationError::service_error("Failed to open GeoMapIndex after creating it")
            })?,
        );
        Ok(())
    }

    fn add_point(
        &mut self,
        id: PointOffsetType,
        payload: &[&Value],
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        let Some(index) = &mut self.index else {
            return Err(OperationError::service_error(
                "GeoMapIndexGridstoreBuilder: index must be initialized before adding points",
            ));
        };
        index.add_point(id, payload, hw_counter)
    }

    fn finalize(mut self) -> OperationResult<Self::FieldIndexType> {
        let Some(index) = self.index.take() else {
            return Err(OperationError::service_error(
                "GeoMapIndexGridstoreBuilder: index must be initialized to finalize",
            ));
        };
        index.flusher()()?;
        Ok(index)
    }
}

impl PayloadFieldIndex for GeoMapIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn cleanup(self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.wipe(),
            GeoMapIndex::Immutable(index) => index.wipe(),
            GeoMapIndex::Mmap(index) => index.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            GeoMapIndex::Mutable(index) => index.flusher(),
            GeoMapIndex::Immutable(index) => index.flusher(),
            GeoMapIndex::Mmap(index) => index.flusher(),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match &self {
            GeoMapIndex::Mutable(index) => index.files(),
            GeoMapIndex::Immutable(index) => index.files(),
            GeoMapIndex::Mmap(index) => index.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match &self {
            GeoMapIndex::Mutable(_) => vec![],
            GeoMapIndex::Immutable(index) => index.immutable_files(),
            GeoMapIndex::Mmap(index) => index.immutable_files(),
        }
    }

    fn filter<'a>(
        &'a self,
        condition: &FieldCondition,
        hw_counter: &'a HardwareCounterCell,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + 'a>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_bounding_box.clone();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, hw_counter, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_radius.clone();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, hw_counter, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let geo_hashes = polygon_hashes(geo_polygon, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_polygon.convert();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, hw_counter, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        None
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
        hw_counter: &HardwareCounterCell,
    ) -> Option<CardinalityEstimation> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok()?;
            let mut estimation = self.match_cardinality(&geo_hashes, hw_counter);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Some(estimation);
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok()?;
            let mut estimation = self.match_cardinality(&geo_hashes, hw_counter);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Some(estimation);
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let (exterior_hashes, interior_hashes) =
                polygon_hashes_estimation(geo_polygon, GEO_QUERY_MAX_REGION);
            // The polygon cardinality estimation should consider its exterior and interiors.
            // Therefore, we compute exterior estimation first and then subtract all interior estimation.
            let mut exterior_estimation = self.match_cardinality(&exterior_hashes, hw_counter);

            for interior in &interior_hashes {
                let interior_estimation = self.match_cardinality(interior, hw_counter);
                exterior_estimation.min = max(0, exterior_estimation.min - interior_estimation.max);
                exterior_estimation.max = max(
                    exterior_estimation.min,
                    exterior_estimation.max - interior_estimation.min,
                );
                exterior_estimation.exp = max(
                    exterior_estimation.exp - interior_estimation.exp,
                    exterior_estimation.min,
                );
            }

            exterior_estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Some(exterior_estimation);
        }

        None
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.large_hashes(threshold)
                .map(move |(geo_hash, size)| PayloadBlockCondition {
                    condition: FieldCondition::new_geo_bounding_box(
                        key.clone(),
                        geo_hash_to_box(geo_hash),
                    ),
                    cardinality: size,
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};
    use std::ops::Range;

    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use itertools::Itertools;
    use ordered_float::OrderedFloat;
    use rand::SeedableRng;
    use rand::prelude::StdRng;
    use rstest::rstest;
    use serde_json::json;
    use tempfile::{Builder, TempDir};

    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::json_path::JsonPath;
    use crate::types::test_utils::build_polygon;
    use crate::types::{GeoBoundingBox, GeoLineString, GeoPolygon, GeoRadius};

    #[cfg(feature = "rocksdb")]
    type Database = std::sync::Arc<parking_lot::RwLock<DB>>;
    #[cfg(not(feature = "rocksdb"))]
    type Database = ();

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum IndexType {
        #[cfg(feature = "rocksdb")]
        Mutable,
        MutableGridstore,
        #[cfg(feature = "rocksdb")]
        Immutable,
        Mmap,
        RamMmap,
    }

    enum IndexBuilder {
        #[cfg(feature = "rocksdb")]
        Mutable(GeoMapIndexBuilder),
        MutableGridstore(GeoMapIndexGridstoreBuilder),
        #[cfg(feature = "rocksdb")]
        Immutable(GeoMapImmutableIndexBuilder),
        Mmap(GeoMapIndexMmapBuilder),
        RamMmap(GeoMapIndexMmapBuilder),
    }

    impl IndexBuilder {
        fn add_point(
            &mut self,
            id: PointOffsetType,
            payload: &[&Value],
            hw_counter: &HardwareCounterCell,
        ) -> OperationResult<()> {
            match self {
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Mutable(builder) => builder.add_point(id, payload, hw_counter),
                IndexBuilder::MutableGridstore(builder) => {
                    builder.add_point(id, payload, hw_counter)
                }
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Immutable(builder) => builder.add_point(id, payload, hw_counter),
                IndexBuilder::Mmap(builder) => builder.add_point(id, payload, hw_counter),
                IndexBuilder::RamMmap(builder) => builder.add_point(id, payload, hw_counter),
            }
        }

        fn finalize(self) -> OperationResult<GeoMapIndex> {
            match self {
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Mutable(builder) => builder.finalize(),
                IndexBuilder::MutableGridstore(builder) => builder.finalize(),
                #[cfg(feature = "rocksdb")]
                IndexBuilder::Immutable(builder) => builder.finalize(),
                IndexBuilder::Mmap(builder) => builder.finalize(),
                IndexBuilder::RamMmap(builder) => {
                    let GeoMapIndex::Mmap(index) = builder.finalize()? else {
                        panic!("expected mmap index");
                    };

                    // Load index from mmap
                    let index = GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(*index));
                    Ok(index)
                }
            }
        }
    }

    const NYC: GeoPoint = GeoPoint::new_unchecked(-73.991516, 40.75798);

    const BERLIN: GeoPoint = GeoPoint::new_unchecked(13.41053, 52.52437);

    const POTSDAM: GeoPoint = GeoPoint::new_unchecked(13.064473, 52.390569);

    const TOKYO: GeoPoint = GeoPoint::new_unchecked(139.691706, 35.689487);

    const LOS_ANGELES: GeoPoint = GeoPoint::new_unchecked(-118.243683, 34.052235);

    #[cfg(feature = "rocksdb")]
    const FIELD_NAME: &str = "test";

    fn condition_for_geo_radius(key: &str, geo_radius: GeoRadius) -> FieldCondition {
        FieldCondition::new_geo_radius(JsonPath::new(key), geo_radius)
    }

    fn condition_for_geo_polygon(key: &str, geo_polygon: GeoPolygon) -> FieldCondition {
        FieldCondition::new_geo_polygon(JsonPath::new(key), geo_polygon)
    }

    fn condition_for_geo_box(key: &str, geo_bounding_box: GeoBoundingBox) -> FieldCondition {
        FieldCondition::new_geo_bounding_box(JsonPath::new(key), geo_bounding_box)
    }

    #[cfg(feature = "testing")]
    fn create_builder(index_type: IndexType) -> (IndexBuilder, TempDir, Database) {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();

        #[cfg(feature = "rocksdb")]
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        #[cfg(not(feature = "rocksdb"))]
        let db = ();

        let mut builder = match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable => {
                IndexBuilder::Mutable(GeoMapIndex::builder(db.clone(), FIELD_NAME).unwrap())
            }
            IndexType::MutableGridstore => IndexBuilder::MutableGridstore(
                GeoMapIndex::builder_gridstore(temp_dir.path().to_path_buf()),
            ),
            #[cfg(feature = "rocksdb")]
            IndexType::Immutable => IndexBuilder::Immutable(
                GeoMapIndex::builder_immutable(db.clone(), FIELD_NAME).unwrap(),
            ),
            IndexType::Mmap => IndexBuilder::Mmap(GeoMapIndex::builder_mmap(temp_dir.path(), true)),
            IndexType::RamMmap => {
                IndexBuilder::RamMmap(GeoMapIndex::builder_mmap(temp_dir.path(), false))
            }
        };
        match &mut builder {
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Mutable(builder) => builder.init().unwrap(),
            IndexBuilder::MutableGridstore(builder) => builder.init().unwrap(),
            #[cfg(feature = "rocksdb")]
            IndexBuilder::Immutable(builder) => builder.init().unwrap(),
            IndexBuilder::Mmap(builder) => builder.init().unwrap(),
            IndexBuilder::RamMmap(builder) => builder.init().unwrap(),
        }
        (builder, temp_dir, db)
    }

    fn build_random_index(
        num_points: usize,
        num_geo_values: usize,
        index_type: IndexType,
    ) -> (GeoMapIndex, TempDir, Database) {
        let mut rnd = StdRng::seed_from_u64(42);
        let (mut builder, temp_dir, db) = create_builder(index_type);

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values..=num_geo_values);
            let array_payload = Value::Array(geo_points);
            builder
                .add_point(
                    idx as PointOffsetType,
                    &[&array_payload],
                    &HardwareCounterCell::new(),
                )
                .unwrap();
        }

        let index = builder.finalize().unwrap();
        assert_eq!(index.points_count(), num_points);
        assert_eq!(index.points_values_count(), num_points * num_geo_values);
        (index, temp_dir, db)
    }

    const EARTH_RADIUS_METERS: f64 = 6371.0 * 1000.;
    const LON_RANGE: Range<f64> = -180.0..180.0;
    const LAT_RANGE: Range<f64> = -90.0..90.0;
    const COORD_EPS: f64 = 1e-12;

    // util function to generate a bounding polygon of a geo_radius
    fn radius_to_polygon(circle: &GeoRadius) -> GeoPolygon {
        let angular_radius: f64 = circle.radius.0 / EARTH_RADIUS_METERS;

        let angular_lat = circle.center.lat.to_radians();
        let mut min_lat = (angular_lat - angular_radius).to_degrees();
        let mut max_lat = (angular_lat + angular_radius).to_degrees();

        let (min_lon, max_lon) = if LAT_RANGE.start < min_lat && max_lat < LAT_RANGE.end {
            let angular_lon = circle.center.lon.to_radians();
            let delta_lon = (angular_radius.sin() / angular_lat.cos()).asin();

            let min_lon = (angular_lon - delta_lon).to_degrees();
            let max_lon = (angular_lon + delta_lon).to_degrees();

            (min_lon, max_lon)
        } else {
            if LAT_RANGE.start > min_lat {
                min_lat = LAT_RANGE.start + COORD_EPS;
            }
            if max_lat > LAT_RANGE.end {
                max_lat = LAT_RANGE.end - COORD_EPS;
            }

            (LON_RANGE.start + COORD_EPS, LON_RANGE.end - COORD_EPS)
        };

        build_polygon(vec![
            (min_lon, min_lat),
            (min_lon, max_lat),
            (max_lon, max_lat),
            (max_lon, min_lat),
            (min_lon, min_lat),
        ])
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_polygon_with_exclusion(#[case] index_type: IndexType) {
        fn check_cardinality_match(
            hashes: Vec<GeoHash>,
            field_condition: FieldCondition,
            index_type: IndexType,
        ) {
            let (field_index, _, _) = build_random_index(500, 20, index_type);
            let exact_points_for_hashes = field_index.iterator(hashes).collect_vec();
            let real_cardinality = exact_points_for_hashes.len();

            let hw_counter = HardwareCounterCell::new();
            let card = field_index.estimate_cardinality(&field_condition, &hw_counter);
            let card = card.unwrap();

            eprintln!("real_cardinality = {real_cardinality:#?}");
            eprintln!("card = {card:#?}");

            assert!(card.min <= real_cardinality);
            assert!(card.max >= real_cardinality);

            assert!(card.exp >= card.min);
            assert!(card.exp <= card.max);
        }

        let europe = GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                GeoPoint::new_unchecked(-8.396395372995187, 46.85848915174239),
                GeoPoint::new_unchecked(-10.508661204875182, 35.64130367692255),
                GeoPoint::new_unchecked(0.9590825812569506, 36.55931431668104),
                GeoPoint::new_unchecked(17.925941188829, 34.89268498908065),
                GeoPoint::new_unchecked(26.378822944221042, 38.87157101630817),
                GeoPoint::new_unchecked(41.568021588510476, 47.7100126473878),
                GeoPoint::new_unchecked(29.149194109528253, 70.96161947624168),
                GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
            ],
        };

        let berlin = GeoLineString {
            points: vec![
                GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
                GeoPoint::new_unchecked(13.11841750240768, 52.550216162683455),
                GeoPoint::new_unchecked(13.11841750240768, 52.40371784468752),
                GeoPoint::new_unchecked(13.391870497137859, 52.40546474165669),
                GeoPoint::new_unchecked(13.653869963292806, 52.35739986654923),
                GeoPoint::new_unchecked(13.754088338324664, 52.44213360096185),
                GeoPoint::new_unchecked(13.60805584899208, 52.47702797300224),
                GeoPoint::new_unchecked(13.63382628828623, 52.53367235825061),
                GeoPoint::new_unchecked(13.48493041681067, 52.60241883100514),
                GeoPoint::new_unchecked(13.52788114896677, 52.6571647548233),
                GeoPoint::new_unchecked(13.257291536380365, 52.667584785254064),
                GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
            ],
        };

        let europe_no_berlin = GeoPolygon {
            exterior: europe,
            interiors: Some(vec![berlin]),
        };
        check_cardinality_match(
            polygon_hashes(&europe_no_berlin, GEO_QUERY_MAX_REGION).unwrap(),
            condition_for_geo_polygon("test", europe_no_berlin.clone()),
            index_type,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn match_cardinality(#[case] index_type: IndexType) {
        fn check_cardinality_match(
            hashes: Vec<GeoHash>,
            field_condition: FieldCondition,
            index_type: IndexType,
        ) {
            let (field_index, _, _) = build_random_index(500, 20, index_type);
            let exact_points_for_hashes = field_index.iterator(hashes).collect_vec();
            let real_cardinality = exact_points_for_hashes.len();

            let hw_counter = HardwareCounterCell::new();

            let card = field_index.estimate_cardinality(&field_condition, &hw_counter);
            let card = card.unwrap();

            eprintln!("real_cardinality = {real_cardinality:#?}");
            eprintln!("card = {card:#?}");

            assert!(card.min <= real_cardinality);
            assert!(card.max >= real_cardinality);

            assert!(card.exp >= card.min);
            assert!(card.exp <= card.max);
        }

        // geo_radius cardinality check
        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: OrderedFloat(r_meters),
        };
        let nyc_hashes = circle_hashes(&geo_radius, GEO_QUERY_MAX_REGION).unwrap();
        check_cardinality_match(
            nyc_hashes,
            condition_for_geo_radius("test", geo_radius.clone()),
            index_type,
        );

        // geo_polygon cardinality check
        let geo_polygon = radius_to_polygon(&geo_radius);
        let polygon_hashes = polygon_hashes(&geo_polygon, GEO_QUERY_MAX_REGION).unwrap();
        check_cardinality_match(
            polygon_hashes,
            condition_for_geo_polygon("test", geo_polygon),
            index_type,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn geo_indexed_filtering(#[case] index_type: IndexType) {
        fn check_geo_indexed_filtering<F>(
            field_condition: FieldCondition,
            check_fn: F,
            index_type: IndexType,
        ) where
            F: Fn(&GeoPoint) -> bool + Clone,
        {
            let (field_index, _, _) = build_random_index(1000, 5, index_type);

            let hw_counter = HardwareCounterCell::new();
            let mut matched_points = (0..field_index.count_indexed_points() as PointOffsetType)
                .filter_map(|idx| {
                    if field_index.check_values_any(idx, &hw_counter, check_fn.clone()) {
                        Some(idx as PointOffsetType)
                    } else {
                        None
                    }
                })
                .collect_vec();

            assert!(!matched_points.is_empty());

            let hw_acc = HwMeasurementAcc::new();
            let hw_counter = hw_acc.get_counter_cell();
            let mut indexed_matched_points = field_index
                .filter(&field_condition, &hw_counter)
                .unwrap()
                .collect_vec();

            matched_points.sort_unstable();
            indexed_matched_points.sort_unstable();

            assert_eq!(matched_points, indexed_matched_points);
        }

        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: OrderedFloat(r_meters),
        };
        check_geo_indexed_filtering(
            condition_for_geo_radius("test", geo_radius.clone()),
            |geo_point| geo_radius.check_point(geo_point),
            index_type,
        );

        let geo_polygon: GeoPolygon = build_polygon(vec![
            (-60.0, 37.0),
            (-60.0, 45.0),
            (-50.0, 45.0),
            (-50.0, 37.0),
            (-60.0, 37.0),
        ]);
        check_geo_indexed_filtering(
            condition_for_geo_polygon("test", geo_polygon.clone()),
            |geo_point| geo_polygon.convert().check_point(geo_point),
            index_type,
        );
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_payload_blocks(#[case] index_type: IndexType) {
        let (field_index, _, _) = build_random_index(1000, 5, index_type);
        let hw_counter = HardwareCounterCell::new();
        let top_level_points = field_index.points_of_hash(&Default::default(), &hw_counter);
        assert_eq!(top_level_points, 1_000);
        let block_hashes = field_index.large_hashes(100).collect_vec();
        assert!(!block_hashes.is_empty());
        for (geohash, size) in block_hashes {
            assert_eq!(geohash.len(), 1);
            assert!(size > 100);
            assert!(size < 1000);
        }

        let blocks = field_index
            .payload_blocks(100, JsonPath::new("test"))
            .collect_vec();
        blocks.iter().for_each(|block| {
            let hw_acc = HwMeasurementAcc::new();
            let hw_counter = hw_acc.get_counter_cell();
            let block_points = field_index
                .filter(&block.condition, &hw_counter)
                .unwrap()
                .collect_vec();
            assert_eq!(block_points.len(), block.cardinality);
        });
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn match_cardinality_point_with_multi_far_geo_payload(#[case] index_type: IndexType) {
        let (mut builder, _, _) = create_builder(index_type);

        let r_meters = 100.0;
        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            },
            {
                "lon": NYC.lon,
                "lat": NYC.lat
            }
        ]);
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
        let index = builder.finalize().unwrap();

        let hw_counter = HardwareCounterCell::new();

        // around NYC
        let nyc_geo_radius = GeoRadius {
            center: NYC,
            radius: OrderedFloat(r_meters),
        };
        let field_condition = condition_for_geo_radius("test", nyc_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&nyc_geo_radius));
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // around BERLIN
        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: OrderedFloat(r_meters),
        };
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // around TOKYO
        let tokyo_geo_radius = GeoRadius {
            center: TOKYO,
            radius: OrderedFloat(r_meters),
        };
        let field_condition = condition_for_geo_radius("test", tokyo_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);

        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&tokyo_geo_radius));
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn match_cardinality_point_with_multi_close_geo_payload(#[case] index_type: IndexType) {
        let (mut builder, _, _) = create_builder(index_type);
        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            },
            {
                "lon": POTSDAM.lon,
                "lat": POTSDAM.lat
            }
        ]);
        let hw_counter = HardwareCounterCell::new();
        builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
        let index = builder.finalize().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: OrderedFloat(50_000.0), // Berlin <-> Potsdam is 27 km
        };
        // check with geo_radius
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        // handle properly that a single point matches via two different geo payloads
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let card = index.estimate_cardinality(&field_condition, &hw_counter);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn load_from_disk(#[case] index_type: IndexType) {
        let temp_dir = {
            let (mut builder, temp_dir, _) = create_builder(index_type);

            let geo_values = json!([
                {
                    "lon": BERLIN.lon,
                    "lat": BERLIN.lat
                },
                {
                    "lon": POTSDAM.lon,
                    "lat": POTSDAM.lat
                }
            ]);
            let hw_counter = HardwareCounterCell::new();
            builder.add_point(1, &[&geo_values], &hw_counter).unwrap();
            builder.finalize().unwrap();
            temp_dir
        };

        #[cfg(feature = "rocksdb")]
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let new_index = match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable => GeoMapIndex::new_memory(db, FIELD_NAME, true, true)
                .unwrap()
                .unwrap(),
            IndexType::MutableGridstore => {
                GeoMapIndex::new_gridstore(temp_dir.path().to_path_buf(), true)
                    .unwrap()
                    .unwrap()
            }
            #[cfg(feature = "rocksdb")]
            IndexType::Immutable => GeoMapIndex::new_memory(db, FIELD_NAME, false, true)
                .unwrap()
                .unwrap(),
            IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false)
                .unwrap()
                .unwrap(),
            IndexType::RamMmap => GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(
                MmapGeoMapIndex::open(temp_dir.path(), false)
                    .unwrap()
                    .unwrap(),
            )),
        };

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: OrderedFloat(50_000.0), // Berlin <-> Potsdam is 27 km
        };

        // check with geo_radius
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let point_offsets = new_index
            .filter(&field_condition, &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![1]);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let point_offsets = new_index
            .filter(&field_condition, &hw_counter)
            .unwrap()
            .collect_vec();
        assert_eq!(point_offsets, vec![1]);
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn same_geo_index_between_points_test(#[case] index_type: IndexType) {
        let temp_dir = {
            let (mut builder, temp_dir, _) = create_builder(index_type);

            let geo_values = json!([
                {
                    "lon": BERLIN.lon,
                    "lat": BERLIN.lat
                },
                {
                    "lon": POTSDAM.lon,
                    "lat": POTSDAM.lat
                }
            ]);
            let hw_counter = HardwareCounterCell::new();
            let payload = [&geo_values];
            builder.add_point(1, &payload, &hw_counter).unwrap();
            builder.add_point(2, &payload, &hw_counter).unwrap();
            let mut index = builder.finalize().unwrap();

            index.remove_point(1).unwrap();
            index.flusher()().unwrap();

            assert_eq!(index.points_count(), 1);
            if index_type != IndexType::Mmap {
                assert_eq!(index.points_values_count(), 2);
            }
            drop(index);
            temp_dir
        };

        #[cfg(feature = "rocksdb")]
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let new_index = match index_type {
            #[cfg(feature = "rocksdb")]
            IndexType::Mutable => GeoMapIndex::new_memory(db, FIELD_NAME, true, true)
                .unwrap()
                .unwrap(),
            IndexType::MutableGridstore => {
                GeoMapIndex::new_gridstore(temp_dir.path().to_path_buf(), true)
                    .unwrap()
                    .unwrap()
            }
            #[cfg(feature = "rocksdb")]
            IndexType::Immutable => GeoMapIndex::new_memory(db, FIELD_NAME, false, true)
                .unwrap()
                .unwrap(),
            IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path(), false)
                .unwrap()
                .unwrap(),
            IndexType::RamMmap => GeoMapIndex::Immutable(ImmutableGeoMapIndex::open_mmap(
                MmapGeoMapIndex::open(temp_dir.path(), false)
                    .unwrap()
                    .unwrap(),
            )),
        };
        assert_eq!(new_index.points_count(), 1);
        if index_type != IndexType::Mmap {
            assert_eq!(new_index.points_values_count(), 2);
        }
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn test_empty_index_cardinality(#[case] index_type: IndexType) {
        let polygon = GeoPolygon {
            exterior: GeoLineString {
                points: vec![
                    GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                    GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                    GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                    GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                ],
            },
            interiors: None,
        };
        let polygon_with_interior = GeoPolygon {
            exterior: polygon.exterior.clone(),
            interiors: Some(vec![GeoLineString {
                points: vec![
                    GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
                    GeoPoint::new_unchecked(13.11841750240768, 52.550216162683455),
                    GeoPoint::new_unchecked(13.11841750240768, 52.40371784468752),
                    GeoPoint::new_unchecked(13.2257943327987, 52.62328249733332),
                ],
            }]),
        };
        let hashes = polygon_hashes(&polygon, GEO_QUERY_MAX_REGION).unwrap();
        let hashes_with_interior =
            polygon_hashes(&polygon_with_interior, GEO_QUERY_MAX_REGION).unwrap();

        let hw_counter = HardwareCounterCell::new();

        let (field_index, _, _) = build_random_index(0, 0, index_type);
        assert!(
            field_index
                .match_cardinality(&hashes, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );
        assert!(
            field_index
                .match_cardinality(&hashes_with_interior, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );

        let (field_index, _, _) = build_random_index(0, 100, index_type);
        assert!(
            field_index
                .match_cardinality(&hashes, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );
        assert!(
            field_index
                .match_cardinality(&hashes_with_interior, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );

        let (field_index, _, _) = build_random_index(100, 100, index_type);
        assert!(
            !field_index
                .match_cardinality(&hashes, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );
        assert!(
            !field_index
                .match_cardinality(&hashes_with_interior, &hw_counter)
                .equals_min_exp_max(&CardinalityEstimation::exact(0)),
        );
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Mutable))]
    #[case(IndexType::MutableGridstore)]
    #[cfg_attr(feature = "rocksdb", case(IndexType::Immutable))]
    #[case(IndexType::Mmap)]
    #[case(IndexType::RamMmap)]
    fn query_across_antimeridian(#[case] index_type: IndexType) {
        let (mut builder, _, _) = create_builder(index_type);
        // Index BERLIN
        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            }
        ]);
        let hw_counter = HardwareCounterCell::new();

        builder.add_point(1, &[&geo_values], &hw_counter).unwrap();

        // Index LOS_ANGELES
        let geo_values = json!([
            {
                "lon": LOS_ANGELES.lon,
                "lat": LOS_ANGELES.lat
            }
        ]);
        builder.add_point(2, &[&geo_values], &hw_counter).unwrap();

        // Index TOKYO
        let geo_values = json!([
            {
                "lon": TOKYO.lon,
                "lat": TOKYO.lat
            }
        ]);
        builder.add_point(3, &[&geo_values], &hw_counter).unwrap();

        let new_index = builder.finalize().unwrap();
        assert_eq!(new_index.points_count(), 3);
        assert_eq!(new_index.points_values_count(), 3);

        // Large bounding box around the USA: (74.071028, 167), (18.7763, -66.885417)
        let bounding_box = GeoBoundingBox {
            top_left: GeoPoint::new_unchecked(167.0, 74.071028),
            bottom_right: GeoPoint::new_unchecked(-66.885417, 18.7763),
        };

        // check with geo_radius
        let field_condition = condition_for_geo_box("test", bounding_box);
        let hw_acc = HwMeasurementAcc::new();
        let hw_counter = hw_acc.get_counter_cell();
        let point_offsets = new_index
            .filter(&field_condition, &hw_counter)
            .unwrap()
            .collect_vec();
        // Only LOS_ANGELES is in the bounding box
        assert_eq!(point_offsets, vec![2]);
    }

    #[rstest]
    #[cfg_attr(feature = "rocksdb", case(&[IndexType::Mutable, IndexType::MutableGridstore, IndexType::Immutable, IndexType::Mmap, IndexType::RamMmap], false))]
    #[cfg_attr(feature = "rocksdb", case(&[IndexType::Mutable, IndexType::MutableGridstore, IndexType::Immutable, IndexType::RamMmap], true))]
    #[cfg_attr(not(feature = "rocksdb"), case(&[IndexType::MutableGridstore, IndexType::Mmap, IndexType::RamMmap], false))]
    #[cfg_attr(not(feature = "rocksdb"), case(&[IndexType::MutableGridstore, IndexType::RamMmap], true))]
    fn test_congruence(#[case] types: &[IndexType], #[case] deleted: bool) {
        const POINT_COUNT: usize = 500;

        let (mut indices, _data): (Vec<_>, Vec<_>) = types
            .iter()
            .copied()
            .map(|index_type| {
                let (index, temp_dir, db) = build_random_index(POINT_COUNT, 20, index_type);
                (index, (temp_dir, db))
            })
            .unzip();

        let polygon = GeoPolygon {
            exterior: GeoLineString {
                points: vec![
                    GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                    GeoPoint::new_unchecked(2.4664944437317615, 61.852748225727254),
                    GeoPoint::new_unchecked(2.713789718828849, 51.80793869181895),
                    GeoPoint::new_unchecked(19.415558242000287, 69.18533258102943),
                ],
            },
            interiors: None,
        };
        let hashes = polygon_hashes(&polygon, GEO_QUERY_MAX_REGION).unwrap();

        if deleted {
            for index in indices.iter_mut() {
                index.remove_point(10).unwrap();
                index.remove_point(11).unwrap();
                index.remove_point(12).unwrap();
                index.remove_point(100).unwrap();
                index.remove_point(150).unwrap();
            }
        }

        for index in &indices[1..] {
            assert_eq!(indices[0].points_count(), index.points_count());
            if !deleted {
                assert_eq!(
                    indices[0].points_values_count(),
                    index.points_values_count(),
                );
            }
            assert_eq!(
                indices[0].max_values_per_point(),
                index.max_values_per_point(),
            );
            let hw_counter = HardwareCounterCell::disposable();
            for hash in &hashes {
                assert_eq!(
                    indices[0].points_of_hash(hash, &hw_counter),
                    index.points_of_hash(hash, &hw_counter),
                );
                assert_eq!(
                    indices[0].values_of_hash(hash, &hw_counter),
                    index.values_of_hash(hash, &hw_counter),
                );
            }
            assert_eq!(
                indices[0]
                    .large_hashes(20)
                    .map(|(hash, _)| hash)
                    .collect::<BTreeSet<_>>(),
                index
                    .large_hashes(20)
                    .map(|(hash, _)| hash)
                    .collect::<BTreeSet<_>>(),
            );
            assert_eq!(
                indices[0].iterator(hashes.clone()).collect::<HashSet<_>>(),
                index.iterator(hashes.clone()).collect::<HashSet<_>>(),
            );
            for point_id in 0..POINT_COUNT {
                assert_eq!(
                    indices[0].values_count(point_id as PointOffsetType),
                    index.values_count(point_id as PointOffsetType),
                );
                if !deleted {
                    assert_eq!(
                        indices[0]
                            .get_values(point_id as PointOffsetType)
                            .unwrap()
                            .collect::<BTreeSet<_>>(),
                        index
                            .get_values(point_id as PointOffsetType)
                            .unwrap()
                            .collect::<BTreeSet<_>>(),
                    );
                }
                assert_eq!(
                    indices[0].values_is_empty(point_id as PointOffsetType),
                    index.values_is_empty(point_id as PointOffsetType),
                );
            }
        }
    }
}
