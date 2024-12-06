use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use itertools::Itertools;
use mutable_geo_index::InMemoryGeoMapIndex;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;
use smol_str::{format_smolstr, SmolStr};

use self::immutable_geo_index::ImmutableGeoMapIndex;
use self::mmap_geo_index::MmapGeoMapIndex;
use self::mutable_geo_index::MutableGeoMapIndex;
use super::FieldIndexBuilderTrait;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::index::field_index::geo_hash::{
    circle_hashes, common_hash_prefix, geo_hash_to_box, polygon_hashes, polygon_hashes_estimation,
    rectangle_hashes, GeoHash,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
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
    pub fn new_memory(db: Arc<RwLock<DB>>, field: &str, is_appendable: bool) -> Self {
        let store_cf_name = GeoMapIndex::storage_cf_name(field);
        if is_appendable {
            GeoMapIndex::Mutable(MutableGeoMapIndex::new(db, &store_cf_name))
        } else {
            GeoMapIndex::Immutable(ImmutableGeoMapIndex::new(db, &store_cf_name))
        }
    }

    pub fn new_mmap(path: &Path) -> OperationResult<Self> {
        Ok(GeoMapIndex::Mmap(Box::new(MmapGeoMapIndex::load(path)?)))
    }

    pub fn builder(db: Arc<RwLock<DB>>, field: &str) -> GeoMapIndexBuilder {
        GeoMapIndexBuilder(Self::new_memory(db, field, true))
    }

    #[cfg(test)]
    pub fn builder_immutable(db: Arc<RwLock<DB>>, field: &str) -> GeoMapImmutableIndexBuilder {
        GeoMapImmutableIndexBuilder {
            index: Self::new_memory(db.clone(), field, true),
            field: field.to_owned(),
            db,
        }
    }

    pub fn mmap_builder(path: &Path) -> GeoMapIndexMmapBuilder {
        GeoMapIndexMmapBuilder {
            path: path.to_owned(),
            in_memory_index: InMemoryGeoMapIndex::new(),
        }
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

    fn points_of_hash(&self, hash: &GeoHash) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.points_of_hash(hash),
            GeoMapIndex::Mmap(index) => index.points_of_hash(hash),
        }
    }

    fn values_of_hash(&self, hash: &GeoHash) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Immutable(index) => index.values_of_hash(hash),
            GeoMapIndex::Mmap(index) => index.values_of_hash(hash),
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_geo")
    }

    fn encode_db_key(value: GeoHash, idx: PointOffsetType) -> SmolStr {
        let value_str = SmolStr::from(value);
        format_smolstr!("{value_str}/{idx}")
    }

    fn decode_db_key(s: &str) -> OperationResult<(GeoHash, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let geohash_str = &s[..separator_pos];
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((
            GeoHash::new(geohash_str).map_err(OperationError::from)?,
            idx,
        ))
    }

    fn decode_db_value<T: AsRef<[u8]>>(value: T) -> OperationResult<GeoPoint> {
        let lat_bytes = value.as_ref()[0..8]
            .try_into()
            .map_err(|_| OperationError::service_error("invalid lat encoding"))?;

        let lon_bytes = value.as_ref()[8..16]
            .try_into()
            .map_err(|_| OperationError::service_error("invalid lat encoding"))?;

        let lat = f64::from_be_bytes(lat_bytes);
        let lon = f64::from_be_bytes(lon_bytes);

        Ok(GeoPoint { lon, lat })
    }

    fn encode_db_value(value: &GeoPoint) -> [u8; 16] {
        let mut result: [u8; 16] = [0; 16];
        result[0..8].clone_from_slice(&value.lat.to_be_bytes());
        result[8..16].clone_from_slice(&value.lon.to_be_bytes());
        result
    }

    pub fn flusher(&self) -> Flusher {
        match self {
            GeoMapIndex::Mutable(index) => index.db_wrapper().flusher(),
            GeoMapIndex::Immutable(index) => index.db_wrapper().flusher(),
            GeoMapIndex::Mmap(index) => index.flusher(),
        }
    }

    pub fn check_values_any(
        &self,
        idx: PointOffsetType,
        check_fn: impl Fn(&GeoPoint) -> bool,
    ) -> bool {
        match self {
            GeoMapIndex::Mutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Immutable(index) => index.check_values_any(idx, check_fn),
            GeoMapIndex::Mmap(index) => index.check_values_any(idx, check_fn),
        }
    }

    pub fn values_count(&self, idx: PointOffsetType) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.values_count(idx),
            GeoMapIndex::Immutable(index) => index.values_count(idx),
            GeoMapIndex::Mmap(index) => index.values_count(idx),
        }
    }

    pub fn match_cardinality(&self, values: &[GeoHash]) -> CardinalityEstimation {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return CardinalityEstimation::exact(0);
        }

        let Some(common_hash) = common_hash_prefix(values) else {
            return CardinalityEstimation::exact(0);
        };

        let total_points = self.points_of_hash(&common_hash);
        let total_values = self.values_of_hash(&common_hash);

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|region| self.points_of_hash(region))
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
        }
    }

    fn iterator(&self, values: Vec<GeoHash>) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            GeoMapIndex::Mutable(index) => Box::new(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(&top_geo_hash))
                    .unique(),
            ),
            GeoMapIndex::Immutable(index) => Box::new(
                values
                    .into_iter()
                    .flat_map(|top_geo_hash| index.stored_sub_regions(&top_geo_hash))
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
    fn large_hashes(&self, threshold: usize) -> Box<dyn Iterator<Item = (GeoHash, usize)> + '_> {
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

        Box::new(edge_region.into_iter())
    }

    pub fn values_is_empty(&self, idx: PointOffsetType) -> bool {
        self.values_count(idx) == 0
    }
}

pub struct GeoMapIndexBuilder(GeoMapIndex);

impl FieldIndexBuilderTrait for GeoMapIndexBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        match &self.0 {
            GeoMapIndex::Mutable(index) => index.db_wrapper().recreate_column_family(),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Cannot use immutable index as a builder type",
            )),
            GeoMapIndex::Mmap(_) => Err(OperationError::service_error(
                "Cannot use mmap index as a builder type",
            )),
        }
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.0.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(self.0)
    }
}

#[cfg(test)]
pub struct GeoMapImmutableIndexBuilder {
    index: GeoMapIndex,
    field: String,
    db: Arc<RwLock<DB>>,
}

#[cfg(test)]
impl FieldIndexBuilderTrait for GeoMapImmutableIndexBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        match &self.index {
            GeoMapIndex::Mutable(index) => index.db_wrapper().recreate_column_family(),
            GeoMapIndex::Immutable(_) => Err(OperationError::service_error(
                "Cannot use immutable index as a builder type",
            )),
            GeoMapIndex::Mmap(_) => Err(OperationError::service_error(
                "Cannot use mmap index as a builder type",
            )),
        }
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        self.index.add_point(id, payload)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        drop(self.index);
        let mut immutable_index = GeoMapIndex::new_memory(self.db, &self.field, false);
        immutable_index.load()?;
        Ok(immutable_index)
    }
}

pub struct GeoMapIndexMmapBuilder {
    path: PathBuf,
    in_memory_index: InMemoryGeoMapIndex,
}

impl FieldIndexBuilderTrait for GeoMapIndexMmapBuilder {
    type FieldIndexType = GeoMapIndex;

    fn init(&mut self) -> OperationResult<()> {
        Ok(())
    }

    fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
        let values = payload
            .iter()
            .flat_map(|value| <GeoMapIndex as ValueIndexer>::get_values(value))
            .collect::<Vec<_>>();
        self.in_memory_index.add_many_geo_points(id, &values)
    }

    fn finalize(self) -> OperationResult<Self::FieldIndexType> {
        Ok(GeoMapIndex::Mmap(Box::new(MmapGeoMapIndex::new(
            self.in_memory_index,
            &self.path,
        )?)))
    }
}

impl ValueIndexer for GeoMapIndex {
    type ValueType = GeoPoint;

    fn add_many(&mut self, id: PointOffsetType, values: Vec<GeoPoint>) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.add_many_geo_points(id, &values),
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

impl PayloadFieldIndex for GeoMapIndex {
    fn count_indexed_points(&self) -> usize {
        self.points_count()
    }

    fn load(&mut self) -> OperationResult<bool> {
        match self {
            GeoMapIndex::Mutable(index) => index.load(),
            GeoMapIndex::Immutable(index) => index.load(),
            // Mmap index is always loaded
            GeoMapIndex::Mmap(_) => Ok(true),
        }
    }

    fn cleanup(self) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.db_wrapper().remove_column_family(),
            GeoMapIndex::Immutable(index) => index.db_wrapper().remove_column_family(),
            GeoMapIndex::Mmap(index) => index.clear(),
        }
    }

    fn flusher(&self) -> Flusher {
        GeoMapIndex::flusher(self)
    }

    fn files(&self) -> Vec<PathBuf> {
        match &self {
            GeoMapIndex::Mutable(index) => index.files(),
            GeoMapIndex::Immutable(index) => index.files(),
            GeoMapIndex::Mmap(index) => index.files(),
        }
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_bounding_box.clone();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_radius.clone();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let geo_hashes = polygon_hashes(geo_polygon, GEO_QUERY_MAX_REGION).ok()?;
            let geo_condition_copy = geo_polygon.convert();
            return Some(Box::new(self.iterator(geo_hashes).filter(move |point| {
                self.check_values_any(*point, |geo_point| {
                    geo_condition_copy.check_point(geo_point)
                })
            })));
        }

        None
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION).ok()?;
            let mut estimation = self.match_cardinality(&geo_hashes);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(Box::new(condition.clone())));
            return Some(estimation);
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION).ok()?;
            let mut estimation = self.match_cardinality(&geo_hashes);
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
            let mut exterior_estimation = self.match_cardinality(&exterior_hashes);

            for interior in &interior_hashes {
                let interior_estimation = self.match_cardinality(interior);
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
    use std::ops::Range;

    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use rstest::rstest;
    use serde_json::json;
    use tempfile::{Builder, TempDir};

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::json_path::JsonPath;
    use crate::types::test_utils::build_polygon;
    use crate::types::{GeoBoundingBox, GeoLineString, GeoPolygon, GeoRadius};

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum IndexType {
        Mutable,
        Immutable,
        Mmap,
    }

    enum IndexBuilder {
        Mutable(GeoMapIndexBuilder),
        Immutable(GeoMapImmutableIndexBuilder),
        Mmap(GeoMapIndexMmapBuilder),
    }

    impl IndexBuilder {
        fn add_point(&mut self, id: PointOffsetType, payload: &[&Value]) -> OperationResult<()> {
            match self {
                IndexBuilder::Mutable(builder) => builder.add_point(id, payload),
                IndexBuilder::Immutable(builder) => builder.add_point(id, payload),
                IndexBuilder::Mmap(builder) => builder.add_point(id, payload),
            }
        }

        fn finalize(self) -> OperationResult<GeoMapIndex> {
            match self {
                IndexBuilder::Mutable(builder) => builder.finalize(),
                IndexBuilder::Immutable(builder) => builder.finalize(),
                IndexBuilder::Mmap(builder) => builder.finalize(),
            }
        }
    }

    const NYC: GeoPoint = GeoPoint {
        lat: 40.75798,
        lon: -73.991516,
    };

    const BERLIN: GeoPoint = GeoPoint {
        lat: 52.52437,
        lon: 13.41053,
    };

    const POTSDAM: GeoPoint = GeoPoint {
        lat: 52.390569,
        lon: 13.064473,
    };

    const TOKYO: GeoPoint = GeoPoint {
        lat: 35.689487,
        lon: 139.691706,
    };

    const LOS_ANGELES: GeoPoint = GeoPoint {
        lat: 34.052235,
        lon: -118.243683,
    };

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

    fn create_builder(index_type: IndexType) -> (IndexBuilder, TempDir, Arc<RwLock<DB>>) {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let mut builder = match index_type {
            IndexType::Mutable => {
                IndexBuilder::Mutable(GeoMapIndex::builder(db.clone(), FIELD_NAME))
            }
            IndexType::Immutable => {
                IndexBuilder::Immutable(GeoMapIndex::builder_immutable(db.clone(), FIELD_NAME))
            }
            IndexType::Mmap => IndexBuilder::Mmap(GeoMapIndex::mmap_builder(temp_dir.path())),
        };
        match &mut builder {
            IndexBuilder::Mutable(builder) => builder.init().unwrap(),
            IndexBuilder::Immutable(builder) => builder.init().unwrap(),
            IndexBuilder::Mmap(builder) => builder.init().unwrap(),
        }
        (builder, temp_dir, db)
    }

    fn build_random_index(
        num_points: usize,
        num_geo_values: usize,
        index_type: IndexType,
    ) -> (GeoMapIndex, TempDir, Arc<RwLock<DB>>) {
        let mut rnd = StdRng::seed_from_u64(42);
        let (mut builder, temp_dir, db) = create_builder(index_type);

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values..=num_geo_values);
            let array_payload = Value::Array(geo_points);
            builder
                .add_point(idx as PointOffsetType, &[&array_payload])
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
        let angular_radius: f64 = circle.radius / EARTH_RADIUS_METERS;

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
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_polygon_with_exclusion(#[case] index_type: IndexType) {
        fn check_cardinality_match(
            hashes: Vec<GeoHash>,
            field_condition: FieldCondition,
            index_type: IndexType,
        ) {
            let (field_index, _, _) = build_random_index(500, 20, index_type);
            let exact_points_for_hashes = field_index.iterator(hashes).collect_vec();
            let real_cardinality = exact_points_for_hashes.len();

            let card = field_index.estimate_cardinality(&field_condition);
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
                GeoPoint {
                    lon: 19.415558242000287,
                    lat: 69.18533258102943,
                },
                GeoPoint {
                    lon: 2.4664944437317615,
                    lat: 61.852748225727254,
                },
                GeoPoint {
                    lon: 2.713789718828849,
                    lat: 51.80793869181895,
                },
                GeoPoint {
                    lon: -8.396395372995187,
                    lat: 46.85848915174239,
                },
                GeoPoint {
                    lon: -10.508661204875182,
                    lat: 35.64130367692255,
                },
                GeoPoint {
                    lon: 0.9590825812569506,
                    lat: 36.55931431668104,
                },
                GeoPoint {
                    lon: 17.925941188829,
                    lat: 34.89268498908065,
                },
                GeoPoint {
                    lon: 26.378822944221042,
                    lat: 38.87157101630817,
                },
                GeoPoint {
                    lon: 41.568021588510476,
                    lat: 47.7100126473878,
                },
                GeoPoint {
                    lon: 29.149194109528253,
                    lat: 70.96161947624168,
                },
                GeoPoint {
                    lon: 19.415558242000287,
                    lat: 69.18533258102943,
                },
            ],
        };

        let berlin = GeoLineString {
            points: vec![
                GeoPoint {
                    lon: 13.2257943327987,
                    lat: 52.62328249733332,
                },
                GeoPoint {
                    lon: 13.11841750240768,
                    lat: 52.550216162683455,
                },
                GeoPoint {
                    lon: 13.11841750240768,
                    lat: 52.40371784468752,
                },
                GeoPoint {
                    lon: 13.391870497137859,
                    lat: 52.40546474165669,
                },
                GeoPoint {
                    lon: 13.653869963292806,
                    lat: 52.35739986654923,
                },
                GeoPoint {
                    lon: 13.754088338324664,
                    lat: 52.44213360096185,
                },
                GeoPoint {
                    lon: 13.60805584899208,
                    lat: 52.47702797300224,
                },
                GeoPoint {
                    lon: 13.63382628828623,
                    lat: 52.53367235825061,
                },
                GeoPoint {
                    lon: 13.48493041681067,
                    lat: 52.60241883100514,
                },
                GeoPoint {
                    lon: 13.52788114896677,
                    lat: 52.6571647548233,
                },
                GeoPoint {
                    lon: 13.257291536380365,
                    lat: 52.667584785254064,
                },
                GeoPoint {
                    lon: 13.2257943327987,
                    lat: 52.62328249733332,
                },
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
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn match_cardinality(#[case] index_type: IndexType) {
        fn check_cardinality_match(
            hashes: Vec<GeoHash>,
            field_condition: FieldCondition,
            index_type: IndexType,
        ) {
            let (field_index, _, _) = build_random_index(500, 20, index_type);
            let exact_points_for_hashes = field_index.iterator(hashes).collect_vec();
            let real_cardinality = exact_points_for_hashes.len();

            let card = field_index.estimate_cardinality(&field_condition);
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
            radius: r_meters,
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
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn geo_indexed_filtering(#[case] index_type: IndexType) {
        fn check_geo_indexed_filtering<F>(
            field_condition: FieldCondition,
            check_fn: F,
            index_type: IndexType,
        ) where
            F: Fn(&GeoPoint) -> bool + Clone,
        {
            let (field_index, _, _) = build_random_index(1000, 5, index_type);

            let mut matched_points = (0..field_index.count_indexed_points() as PointOffsetType)
                .filter_map(|idx| {
                    if field_index.check_values_any(idx, check_fn.clone()) {
                        Some(idx as PointOffsetType)
                    } else {
                        None
                    }
                })
                .collect_vec();

            assert!(!matched_points.is_empty());

            let mut indexed_matched_points =
                field_index.filter(&field_condition).unwrap().collect_vec();

            matched_points.sort_unstable();
            indexed_matched_points.sort_unstable();

            assert_eq!(matched_points, indexed_matched_points);
        }

        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
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
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_payload_blocks(#[case] index_type: IndexType) {
        let (field_index, _, _) = build_random_index(1000, 5, index_type);
        let top_level_points = field_index.points_of_hash(&Default::default());
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
            let block_points = field_index.filter(&block.condition).unwrap().collect_vec();
            assert_eq!(block_points.len(), block.cardinality);
        });
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
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
        builder.add_point(1, &[&geo_values]).unwrap();
        let index = builder.finalize().unwrap();

        // around NYC
        let nyc_geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test", nyc_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition = condition_for_geo_polygon("test", radius_to_polygon(&nyc_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // around BERLIN
        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // around TOKYO
        let tokyo_geo_radius = GeoRadius {
            center: TOKYO,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test", tokyo_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);

        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&tokyo_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
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
        builder.add_point(1, &[&geo_values]).unwrap();
        let index = builder.finalize().unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };
        // check with geo_radius
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // handle properly that a single point matches via two different geo payloads
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
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
            builder.add_point(1, &[&geo_values]).unwrap();
            builder.finalize().unwrap();
            temp_dir
        };

        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let mut new_index = match index_type {
            IndexType::Mutable => GeoMapIndex::new_memory(db, FIELD_NAME, true),
            IndexType::Immutable => GeoMapIndex::new_memory(db, FIELD_NAME, false),
            IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path()).unwrap(),
        };
        new_index.load().unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };

        // check with geo_radius
        let field_condition = condition_for_geo_radius("test", berlin_geo_radius.clone());
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1]);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test", radius_to_polygon(&berlin_geo_radius));
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1]);
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
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
            let payload = [&geo_values];
            builder.add_point(1, &payload).unwrap();
            builder.add_point(2, &payload).unwrap();
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

        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let mut new_index = match index_type {
            IndexType::Mutable => GeoMapIndex::new_memory(db, FIELD_NAME, true),
            IndexType::Immutable => GeoMapIndex::new_memory(db, FIELD_NAME, false),
            IndexType::Mmap => GeoMapIndex::new_mmap(temp_dir.path()).unwrap(),
        };
        new_index.load().unwrap();
        assert_eq!(new_index.points_count(), 1);
        if index_type != IndexType::Mmap {
            assert_eq!(new_index.points_values_count(), 2);
        }
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn test_empty_index_cardinality(#[case] index_type: IndexType) {
        let polygon = GeoPolygon {
            exterior: GeoLineString {
                points: vec![
                    GeoPoint {
                        lon: 19.415558242000287,
                        lat: 69.18533258102943,
                    },
                    GeoPoint {
                        lon: 2.4664944437317615,
                        lat: 61.852748225727254,
                    },
                    GeoPoint {
                        lon: 2.713789718828849,
                        lat: 51.80793869181895,
                    },
                    GeoPoint {
                        lon: 19.415558242000287,
                        lat: 69.18533258102943,
                    },
                ],
            },
            interiors: None,
        };
        let polygon_with_interior = GeoPolygon {
            exterior: polygon.exterior.clone(),
            interiors: Some(vec![GeoLineString {
                points: vec![
                    GeoPoint {
                        lon: 13.2257943327987,
                        lat: 52.62328249733332,
                    },
                    GeoPoint {
                        lon: 13.11841750240768,
                        lat: 52.550216162683455,
                    },
                    GeoPoint {
                        lon: 13.11841750240768,
                        lat: 52.40371784468752,
                    },
                    GeoPoint {
                        lon: 13.2257943327987,
                        lat: 52.62328249733332,
                    },
                ],
            }]),
        };
        let hashes = polygon_hashes(&polygon, GEO_QUERY_MAX_REGION).unwrap();
        let hashes_with_interior =
            polygon_hashes(&polygon_with_interior, GEO_QUERY_MAX_REGION).unwrap();

        let (field_index, _, _) = build_random_index(0, 0, index_type);
        assert!(field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);

        let (field_index, _, _) = build_random_index(0, 100, index_type);
        assert!(field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);

        let (field_index, _, _) = build_random_index(100, 100, index_type);
        assert!(!field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(!field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
    }

    #[rstest]
    #[case(IndexType::Mutable)]
    #[case(IndexType::Immutable)]
    #[case(IndexType::Mmap)]
    fn query_across_antimeridian(#[case] index_type: IndexType) {
        let (mut builder, _, _) = create_builder(index_type);
        // Index BERLIN
        let geo_values = json!([
            {
                "lon": BERLIN.lon,
                "lat": BERLIN.lat
            }
        ]);
        builder.add_point(1, &[&geo_values]).unwrap();

        // Index LOS_ANGELES
        let geo_values = json!([
            {
                "lon": LOS_ANGELES.lon,
                "lat": LOS_ANGELES.lat
            }
        ]);
        builder.add_point(2, &[&geo_values]).unwrap();

        // Index TOKYO
        let geo_values = json!([
            {
                "lon": TOKYO.lon,
                "lat": TOKYO.lat
            }
        ]);
        builder.add_point(3, &[&geo_values]).unwrap();

        let new_index = builder.finalize().unwrap();
        assert_eq!(new_index.points_count(), 3);
        assert_eq!(new_index.points_values_count(), 3);

        // Large bounding box around the USA: (74.071028, 167), (18.7763, -66.885417)
        let bounding_box = GeoBoundingBox {
            top_left: GeoPoint {
                lat: 74.071028,
                lon: 167.0,
            },
            bottom_right: GeoPoint {
                lat: 18.7763,
                lon: -66.885417,
            },
        };

        // check with geo_radius
        let field_condition = condition_for_geo_box("test", bounding_box);
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        // Only LOS_ANGELES is in the bounding box
        assert_eq!(point_offsets, vec![2]);
    }
}
