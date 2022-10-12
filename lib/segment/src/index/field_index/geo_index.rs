use std::cmp::{max, min};
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::entry::entry_point::{OperationError, OperationResult};
use crate::index::field_index::geo_hash::{
    circle_hashes, common_hash_prefix, encode_max_precision, geo_hash_to_box, rectangle_hashes,
    GeoHash,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, GeoBoundingBox, GeoPoint, GeoRadius, PayloadKeyType, PointOffsetType,
};

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

pub struct GeoMapIndex {
    /**
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
    points_per_hash: BTreeMap<GeoHash, usize>,
    values_per_hash: BTreeMap<GeoHash, usize>,
    /**
    {
        "dr5ru": {1},
        "dr5rr": {2, 3},
        ...
    }
     */
    points_map: BTreeMap<GeoHash, HashSet<PointOffsetType>>,
    point_to_values: Vec<Vec<GeoPoint>>,
    points_count: usize,
    values_count: usize,
    max_values_per_point: usize,
    db_wrapper: DatabaseColumnWrapper,
}

impl GeoMapIndex {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = Self::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
        GeoMapIndex {
            points_per_hash: Default::default(),
            values_per_hash: Default::default(),
            points_map: Default::default(),
            point_to_values: vec![],
            points_count: 0,
            values_count: 0,
            max_values_per_point: 1,
            db_wrapper,
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_geo")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper.recreate_column_family()
    }

    fn load(&mut self) -> OperationResult<bool> {
        if !self.db_wrapper.has_column_family()? {
            return Ok(false);
        };

        for (key, value) in self.db_wrapper.lock_db().iter()? {
            let key_str = std::str::from_utf8(&key).map_err(|_| {
                OperationError::service_error("Index load error: UTF8 error while DB parsing")
            })?;

            let (geo_hash, idx) = Self::decode_db_key(key_str)?;
            let geo_point = Self::decode_db_value(value)?;

            if self.point_to_values.len() <= idx as usize {
                self.point_to_values.resize(idx as usize + 1, Vec::new())
            }

            if self.point_to_values[idx as usize].is_empty() {
                self.points_count += 1;
            }

            self.point_to_values[idx as usize].push(geo_point);
            self.points_map.entry(geo_hash).or_default().insert(idx);
        }
        Ok(true)
    }

    fn encode_db_key(value: &str, idx: PointOffsetType) -> String {
        format!("{}/{}", value, idx)
    }

    fn decode_db_key(s: &str) -> OperationResult<(GeoHash, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let geohash = s[..separator_pos].to_string();
        let idx_str = &s[separator_pos + 1..];
        let idx = PointOffsetType::from_str(idx_str)
            .map_err(|_| OperationError::service_error(DECODE_ERR))?;
        Ok((geohash, idx))
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
        self.db_wrapper.flusher()
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&Vec<GeoPoint>> {
        self.point_to_values.get(idx as usize)
    }

    pub fn check_radius(&self, idx: PointOffsetType, radius: &GeoRadius) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| radius.check_point(x.lon, x.lat)))
            .unwrap_or(false)
    }

    pub fn check_box(&self, idx: PointOffsetType, bbox: &GeoBoundingBox) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| bbox.check_point(x.lon, x.lat)))
            .unwrap_or(false)
    }

    pub fn match_cardinality(&self, values: &[GeoHash]) -> CardinalityEstimation {
        let common_hash = common_hash_prefix(values);

        let total_points = self.points_per_hash.get(&common_hash).copied().unwrap_or(0);
        let total_values = self.values_per_hash.get(&common_hash).copied().unwrap_or(0);

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|region| self.points_per_hash.get(region).cloned().unwrap_or(0))
            .fold((0, 0), |(sum, maximum), count| {
                (sum + count, max(maximum, count))
            });

        // Assume all selected points have `max_values_per_point` value hits.
        // Therefore number of points can't be less than `total_hits / max_values_per_point`
        let min_hits_by_value_groups = sum / self.max_values_per_point;

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
            points_count: self.points_count,
            points_values_count: self.values_count,
            histogram_bucket_size: None,
        }
    }

    fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
        if self.point_to_values.len() <= idx as usize {
            return Ok(()); // Already removed or never actually existed
        }

        let removed_points = std::mem::take(&mut self.point_to_values[idx as usize]);

        if removed_points.is_empty() {
            return Ok(());
        }

        self.points_count -= 1;
        self.values_count -= removed_points.len();

        let mut seen_hashes: HashSet<&str> = Default::default();
        let mut geo_hashes = vec![];

        for removed_point in removed_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_point.lon, removed_point.lat).unwrap();
            geo_hashes.push(removed_geo_hash);
        }

        for removed_geo_hash in &geo_hashes {
            let hash_points = self.points_map.get_mut(removed_geo_hash);

            if let Some(ref offsets) = hash_points {
                for point_offset in offsets.iter() {
                    let key = Self::encode_db_key(removed_geo_hash, *point_offset);
                    self.db_wrapper.remove(&key)?;
                }
            }

            let is_last = match hash_points {
                None => false,
                Some(points_set) => {
                    points_set.remove(&idx);
                    points_set.is_empty()
                }
            };
            if is_last {
                self.points_map.remove(removed_geo_hash);
            }

            for i in 0..=removed_geo_hash.len() {
                let sub_geo_hash = &removed_geo_hash[0..i];
                if let Some(count) = self.values_per_hash.get_mut(sub_geo_hash) {
                    *count -= 1;
                }
                if !seen_hashes.contains(sub_geo_hash) {
                    if let Some(count) = self.points_per_hash.get_mut(sub_geo_hash) {
                        *count -= 1;
                    }
                    seen_hashes.insert(sub_geo_hash);
                }
            }
        }

        Ok(())
    }

    fn add_many_geo_points(
        &mut self,
        idx: PointOffsetType,
        values: &[GeoPoint],
    ) -> OperationResult<()> {
        if let Some(existing_vals) = self.get_values(idx) {
            if !existing_vals.is_empty() {
                self.remove_point(idx)?;
            }
        }

        if values.is_empty() {
            return Ok(());
        }

        if self.point_to_values.len() <= idx as usize {
            // That's a smart reallocation
            self.point_to_values.resize(idx as usize + 1, vec![]);
        }

        self.point_to_values[idx as usize] = values.to_vec();

        let mut seen_hashes: HashSet<&str> = Default::default();
        let mut geo_hashes = vec![];

        for added_point in values {
            let added_geo_hash: GeoHash = encode_max_precision(added_point.lon, added_point.lat)
                .map_err(|e| {
                    OperationError::service_error(&format!("Malformed geo points: {}", e))
                })?;

            let key = Self::encode_db_key(&added_geo_hash, idx);
            let value = Self::encode_db_value(added_point);

            geo_hashes.push(added_geo_hash);

            self.db_wrapper.put(&key, &value)?;
        }

        for geo_hash in &geo_hashes {
            self.points_map
                .entry(geo_hash.to_owned())
                .or_insert_with(HashSet::new)
                .insert(idx);

            for i in 0..=geo_hash.len() {
                let sub_geo_hash = &geo_hash[0..i];
                match self.values_per_hash.get_mut(sub_geo_hash) {
                    None => {
                        self.values_per_hash.insert(sub_geo_hash.to_string(), 1);
                    }
                    Some(count) => {
                        *count += 1;
                    }
                };
                if !seen_hashes.contains(sub_geo_hash) {
                    match self.points_per_hash.get_mut(sub_geo_hash) {
                        None => {
                            self.points_per_hash.insert(sub_geo_hash.to_string(), 1);
                        }
                        Some(count) => {
                            *count += 1;
                        }
                    }
                    seen_hashes.insert(sub_geo_hash);
                }
            }
        }

        self.values_count += values.len();
        self.points_count += 1;
        self.max_values_per_point = self.max_values_per_point.max(values.len());
        Ok(())
    }

    fn get_stored_sub_regions(
        &self,
        geo: &GeoHash,
    ) -> Box<dyn Iterator<Item = (&GeoHash, &HashSet<PointOffsetType>)> + '_> {
        let geo_clone = geo.to_string();
        Box::new(
            self.points_map
                .range(geo.to_string()..)
                .take_while(move |(p, _h)| p.starts_with(&geo_clone)),
        )
    }

    fn get_iterator(&self, values: Vec<GeoHash>) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(
            values
                .into_iter()
                .flat_map(|top_geo_hash| {
                    self.get_stored_sub_regions(&top_geo_hash)
                        .flat_map(|(_geohash, points)| points.iter().copied())
                })
                .unique(),
        )
    }

    /// Get iterator over smallest geo-hash regions larger than `threshold` points
    fn get_large_hashes(
        &self,
        threshold: usize,
    ) -> Box<dyn Iterator<Item = (&GeoHash, usize)> + '_> {
        let mut large_regions = self
            .points_per_hash
            .iter()
            .filter(|(hash, size)| **size > threshold && !hash.is_empty())
            .collect_vec();

        // smallest regions first
        large_regions.sort_by(|a, b| b.cmp(a));

        let mut edge_region = vec![];

        let mut current_region = "";

        for (region, size) in large_regions.into_iter() {
            if current_region.starts_with(region) {
                continue;
            } else {
                current_region = region;
                edge_region.push((region, *size));
            }
        }

        Box::new(edge_region.into_iter())
    }
}

impl ValueIndexer<GeoPoint> for GeoMapIndex {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<GeoPoint>) -> OperationResult<()> {
        self.add_many_geo_points(id, &values)
    }

    fn get_value(&self, value: &Value) -> Option<GeoPoint> {
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
        self.remove_point(id)
    }
}

impl PayloadFieldIndex for GeoMapIndex {
    fn indexed_points(&self) -> usize {
        self.points_count
    }

    fn load(&mut self) -> OperationResult<bool> {
        GeoMapIndex::load(self)
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper.remove_column_family()
    }

    fn flusher(&self) -> Flusher {
        GeoMapIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> Option<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION);
            let geo_condition_copy = geo_bounding_box.clone();
            return Some(Box::new(self.get_iterator(geo_hashes).filter(
                move |point| {
                    self.point_to_values
                        .get(*point as usize)
                        .unwrap()
                        .iter()
                        .any(|point| geo_condition_copy.check_point(point.lon, point.lat))
                },
            )));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION);
            let geo_condition_copy = geo_radius.clone();
            return Some(Box::new(self.get_iterator(geo_hashes).filter(
                move |point| {
                    self.point_to_values
                        .get(*point as usize)
                        .unwrap()
                        .iter()
                        .any(|point| geo_condition_copy.check_point(point.lon, point.lat))
                },
            )));
        }

        None
    }

    fn estimate_cardinality(&self, condition: &FieldCondition) -> Option<CardinalityEstimation> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION);
            let mut estimation = self.match_cardinality(&geo_hashes);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            return Some(estimation);
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION);
            let mut estimation = self.match_cardinality(&geo_hashes);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            return Some(estimation);
        }

        None
    }

    fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        Box::new(
            self.get_large_hashes(threshold)
                .map(move |(geo_hash, size)| PayloadBlockCondition {
                    condition: FieldCondition::new_geo_bounding_box(
                        key.clone(),
                        geo_hash_to_box(geo_hash),
                    ),
                    cardinality: size,
                }),
        )
    }

    fn count_indexed_points(&self) -> usize {
        self.points_count
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::types::GeoRadius;

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

    const FIELD_NAME: &str = "test";

    fn condition_for_geo_radius(key: String, geo_radius: GeoRadius) -> FieldCondition {
        FieldCondition::new_geo_radius(key, geo_radius)
    }

    fn build_random_index(num_points: usize, num_geo_values: usize) -> GeoMapIndex {
        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

        let mut rnd = StdRng::seed_from_u64(42);
        let mut index = GeoMapIndex::new(db, FIELD_NAME);

        index.recreate().unwrap();

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values..=num_geo_values);
            index
                .add_point(idx as PointOffsetType, &Value::Array(geo_points))
                .unwrap();
        }
        assert_eq!(index.points_count, num_points);
        assert_eq!(index.values_count, num_points * num_geo_values);

        index
    }

    #[test]
    fn match_cardinality() {
        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };

        let field_index = build_random_index(500, 20);

        let nyc_hashes = circle_hashes(&geo_radius, GEO_QUERY_MAX_REGION);

        let exact_points_for_hashes = field_index.get_iterator(nyc_hashes).collect_vec();
        let real_cardinality = exact_points_for_hashes.len();

        let field_condition = condition_for_geo_radius("test".to_string(), geo_radius);
        let card = field_index.estimate_cardinality(&field_condition);
        let card = card.unwrap();

        eprintln!("real_cardinality = {:#?}", real_cardinality);
        eprintln!("card = {:#?}", card);

        assert!(card.min <= real_cardinality);
        assert!(card.max >= real_cardinality);

        assert!(card.exp >= card.min);
        assert!(card.exp <= card.max);
    }

    #[test]
    fn geo_indexed_filtering() {
        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };

        let field_index = build_random_index(1000, 5);

        let mut matched_points = field_index
            .point_to_values
            .iter()
            .enumerate()
            .filter(|(_idx, geo_points)| {
                geo_points
                    .iter()
                    .any(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat))
            })
            .map(|(idx, _geo_points)| idx as PointOffsetType)
            .collect_vec();

        assert!(!matched_points.is_empty());

        let field_condition = condition_for_geo_radius("test".to_string(), geo_radius);

        let mut indexed_matched_points =
            field_index.filter(&field_condition).unwrap().collect_vec();

        matched_points.sort_unstable();
        indexed_matched_points.sort_unstable();

        assert_eq!(matched_points, indexed_matched_points);
    }

    #[test]
    fn test_payload_blocks() {
        let field_index = build_random_index(1000, 5);
        let top_level_points = field_index.points_per_hash.get("").unwrap();
        assert_eq!(*top_level_points, 1_000);
        let block_hashes = field_index.get_large_hashes(100).collect_vec();
        assert!(!block_hashes.is_empty());
        for (geohash, size) in block_hashes {
            assert_eq!(geohash.len(), 1);
            assert!(size > 100);
            assert!(size < 1000);
        }

        let blocks = field_index
            .payload_blocks(100, "test".to_string())
            .collect_vec();
        blocks.iter().for_each(|block| {
            let block_points = field_index.filter(&block.condition).unwrap().collect_vec();
            assert_eq!(block_points.len(), block.cardinality);
        });
    }

    #[test]
    fn match_cardinality_point_with_multi_far_geo_payload() {
        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

        let mut index = GeoMapIndex::new(db, FIELD_NAME);

        index.recreate().unwrap();

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

        index.add_point(1, &geo_values).unwrap();

        // around NYC
        let nyc_geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test".to_string(), nyc_geo_radius);
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
        let field_condition = condition_for_geo_radius("test".to_string(), berlin_geo_radius);
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
        let field_condition = condition_for_geo_radius("test".to_string(), tokyo_geo_radius);
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);
    }

    #[test]
    fn match_cardinality_point_with_multi_close_geo_payload() {
        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

        let mut index = GeoMapIndex::new(db, FIELD_NAME);

        index.recreate().unwrap();

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
        index.add_point(1, &geo_values).unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };
        let field_condition = condition_for_geo_radius("test".to_string(), berlin_geo_radius);
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // handle properly that a single point matches via two different geo payloads
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);
    }

    #[test]
    fn load_from_disk() {
        let tmp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        {
            let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();

            let mut index = GeoMapIndex::new(db, FIELD_NAME);

            index.recreate().unwrap();

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
            index.add_point(1, &geo_values).unwrap();
            index.flusher()().unwrap();
            drop(index);
        }

        let db = open_db_with_existing_cf(&tmp_dir.path().join("test_db")).unwrap();
        let mut new_index = GeoMapIndex::new(db, FIELD_NAME);
        new_index.load().unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };

        let field_condition = condition_for_geo_radius("test".to_string(), berlin_geo_radius);
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1]);
    }
}
