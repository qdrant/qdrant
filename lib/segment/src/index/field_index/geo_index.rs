use std::cmp::{max, min};
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use common::types::PointOffsetType;
use itertools::Itertools;
use parking_lot::RwLock;
use rocksdb::DB;
use serde_json::Value;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::rocksdb_wrapper::DatabaseColumnWrapper;
use crate::common::Flusher;
use crate::index::field_index::geo_hash::{
    circle_hashes, common_hash_prefix, encode_max_precision, geo_hash_to_box, polygon_hashes,
    polygon_hashes_estimation, rectangle_hashes, GeoHash,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, PayloadBlockCondition, PayloadFieldIndex, PrimaryCondition, ValueIndexer,
};
use crate::telemetry::PayloadIndexTelemetry;
use crate::types::{
    FieldCondition, GeoBoundingBox, GeoPoint, GeoRadius, PayloadKeyType, PolygonWrapper,
};

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

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
    points_per_hash: BTreeMap<GeoHash, usize>,
    values_per_hash: BTreeMap<GeoHash, usize>,
    /*
    {
        "dr5ru": {1},
        "dr5rr": {2, 3},
        ...
    }
     */
    points_map: BTreeMap<GeoHash, HashSet<PointOffsetType>>,
    point_to_values: Vec<Vec<GeoPoint>>,
    points_count: usize,
    points_values_count: usize,
    max_values_per_point: usize,
    db_wrapper: DatabaseColumnWrapper,
}

pub enum GeoMapIndex {
    Mutable(MutableGeoMapIndex),
}

impl MutableGeoMapIndex {
    fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        let store_cf_name = GeoMapIndex::storage_cf_name(field);
        let db_wrapper = DatabaseColumnWrapper::new(db, &store_cf_name);
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

    fn db_wrapper(&self) -> &DatabaseColumnWrapper {
        &self.db_wrapper
    }

    fn get_values(&self, idx: PointOffsetType) -> Option<&[GeoPoint]> {
        self.point_to_values.get(idx as usize).map(Vec::as_slice)
    }

    fn get_points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)> {
        self.points_per_hash
            .iter()
            .map(|(hash, count)| (hash, *count))
    }

    fn get_points_of_hash(&self, hash: &GeoHash) -> usize {
        self.points_per_hash.get(hash).cloned().unwrap_or(0)
    }

    fn get_values_of_hash(&self, hash: &GeoHash) -> usize {
        self.values_per_hash.get(hash).cloned().unwrap_or(0)
    }

    fn load(&mut self) -> OperationResult<bool> {
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

    fn remove_point(&mut self, idx: PointOffsetType) -> OperationResult<()> {
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

    fn add_many_geo_points(
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

    fn get_stored_sub_regions(
        &self,
        geo: &GeoHash,
    ) -> Box<dyn Iterator<Item = (&GeoHash, &HashSet<PointOffsetType>)> + '_> {
        let geo_clone = geo.clone();
        Box::new(
            self.points_map
                .range(geo.clone()..)
                .take_while(move |(p, _h)| p.starts_with(geo_clone.as_str())),
        )
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

impl GeoMapIndex {
    pub fn new(db: Arc<RwLock<DB>>, field: &str) -> Self {
        GeoMapIndex::Mutable(MutableGeoMapIndex::new(db, field))
    }

    fn db_wrapper(&self) -> &DatabaseColumnWrapper {
        match self {
            GeoMapIndex::Mutable(index) => index.db_wrapper(),
        }
    }

    fn points_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_count,
        }
    }

    fn points_values_count(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.points_values_count,
        }
    }

    /// Maximum number of values per point
    ///
    /// # Warning
    ///
    /// Zero if the index is empty.
    fn max_values_per_point(&self) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.max_values_per_point,
        }
    }

    fn get_points_per_hash(&self) -> impl Iterator<Item = (&GeoHash, usize)> {
        match self {
            GeoMapIndex::Mutable(index) => index.get_points_per_hash(),
        }
    }

    fn get_points_of_hash(&self, hash: &GeoHash) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.get_points_of_hash(hash),
        }
    }

    fn get_values_of_hash(&self, hash: &GeoHash) -> usize {
        match self {
            GeoMapIndex::Mutable(index) => index.get_values_of_hash(hash),
        }
    }

    fn get_stored_sub_regions(
        &self,
        geo: &GeoHash,
    ) -> Box<dyn Iterator<Item = (&GeoHash, &HashSet<PointOffsetType>)> + '_> {
        match self {
            GeoMapIndex::Mutable(index) => index.get_stored_sub_regions(geo),
        }
    }

    fn storage_cf_name(field: &str) -> String {
        format!("{field}_geo")
    }

    pub fn recreate(&self) -> OperationResult<()> {
        self.db_wrapper().recreate_column_family()
    }

    fn encode_db_key(value: &str, idx: PointOffsetType) -> String {
        format!("{value}/{idx}")
    }

    fn decode_db_key(s: &str) -> OperationResult<(GeoHash, PointOffsetType)> {
        const DECODE_ERR: &str = "Index db parsing error: wrong data format";
        let separator_pos = s
            .rfind('/')
            .ok_or_else(|| OperationError::service_error(DECODE_ERR))?;
        if separator_pos == s.len() - 1 {
            return Err(OperationError::service_error(DECODE_ERR));
        }
        let geohash = s[..separator_pos].into();
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
        self.db_wrapper().flusher()
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<&[GeoPoint]> {
        match self {
            GeoMapIndex::Mutable(index) => index.get_values(idx),
        }
    }

    pub fn check_radius(&self, idx: PointOffsetType, radius: &GeoRadius) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| radius.check_point(x)))
            .unwrap_or(false)
    }

    pub fn check_box(&self, idx: PointOffsetType, bbox: &GeoBoundingBox) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| bbox.check_point(x)))
            .unwrap_or(false)
    }

    pub fn check_polygon(&self, idx: PointOffsetType, polygon: &PolygonWrapper) -> bool {
        self.get_values(idx)
            .map(|values| values.iter().any(|x| polygon.check_point(x)))
            .unwrap_or(false)
    }

    pub fn match_cardinality(&self, values: &[GeoHash]) -> CardinalityEstimation {
        let max_values_per_point = self.max_values_per_point();
        if max_values_per_point == 0 {
            return CardinalityEstimation::exact(0);
        }

        let common_hash = common_hash_prefix(values);

        let total_points = self.get_points_of_hash(&common_hash);
        let total_values = self.get_values_of_hash(&common_hash);

        let (sum, maximum_per_hash) = values
            .iter()
            .map(|region| self.get_points_of_hash(region))
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
            .get_points_per_hash()
            .filter(|(hash, size)| *size > threshold && !hash.is_empty())
            .collect_vec();

        // smallest regions first
        large_regions.sort_by(|a, b| b.cmp(a));

        let mut edge_region = vec![];

        let mut current_region = GeoHash::default();

        for (region, size) in large_regions.into_iter() {
            if current_region.starts_with(region.as_str()) {
                continue;
            } else {
                current_region = region.clone();
                edge_region.push((region, size));
            }
        }

        Box::new(edge_region.into_iter())
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        self.get_values(point_id).map(|x| x.len()).unwrap_or(0)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_values(point_id)
            .map(|x| x.is_empty())
            .unwrap_or(true)
    }
}

impl ValueIndexer<GeoPoint> for GeoMapIndex {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<GeoPoint>) -> OperationResult<()> {
        match self {
            GeoMapIndex::Mutable(index) => index.add_many_geo_points(id, &values),
        }
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
        match self {
            GeoMapIndex::Mutable(index) => index.remove_point(id),
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
        }
    }

    fn clear(self) -> OperationResult<()> {
        self.db_wrapper().remove_column_family()
    }

    fn flusher(&self) -> Flusher {
        GeoMapIndex::flusher(self)
    }

    fn filter(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<Box<dyn Iterator<Item = PointOffsetType> + '_>> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = geo_bounding_box.clone();
            return Ok(Box::new(self.get_iterator(geo_hashes).filter(
                move |point| {
                    self.get_values(*point)
                        .unwrap()
                        .iter()
                        .any(|point| geo_condition_copy.check_point(point))
                },
            )));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = geo_radius.clone();
            return Ok(Box::new(self.get_iterator(geo_hashes).filter(
                move |point| {
                    self.get_values(*point)
                        .unwrap()
                        .iter()
                        .any(|point| geo_condition_copy.check_point(point))
                },
            )));
        }

        if let Some(geo_polygon) = &condition.geo_polygon {
            let geo_hashes = polygon_hashes(geo_polygon, GEO_QUERY_MAX_REGION)?;
            let geo_condition_copy = geo_polygon.convert();
            return Ok(Box::new(self.get_iterator(geo_hashes).filter(
                move |point| {
                    self.get_values(*point)
                        .unwrap()
                        .iter()
                        .any(|point| geo_condition_copy.check_point(point))
                },
            )));
        }

        Err(OperationError::service_error("failed to filter"))
    }

    fn estimate_cardinality(
        &self,
        condition: &FieldCondition,
    ) -> OperationResult<CardinalityEstimation> {
        if let Some(geo_bounding_box) = &condition.geo_bounding_box {
            let geo_hashes = rectangle_hashes(geo_bounding_box, GEO_QUERY_MAX_REGION)?;
            let mut estimation = self.match_cardinality(&geo_hashes);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            return Ok(estimation);
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION)?;
            let mut estimation = self.match_cardinality(&geo_hashes);
            estimation
                .primary_clauses
                .push(PrimaryCondition::Condition(condition.clone()));
            return Ok(estimation);
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
                .push(PrimaryCondition::Condition(condition.clone()));
            return Ok(exterior_estimation);
        }

        Err(OperationError::service_error(
            "failed to estimate cardinality",
        ))
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
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use serde_json::json;
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::open_db_with_existing_cf;
    use crate::common::utils::MultiValue;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::types::test_utils::build_polygon;
    use crate::types::{GeoLineString, GeoPolygon, GeoRadius};

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

    fn condition_for_geo_polygon(key: String, geo_polygon: GeoPolygon) -> FieldCondition {
        FieldCondition::new_geo_polygon(key, geo_polygon)
    }

    fn build_random_index(num_points: usize, num_geo_values: usize) -> GeoMapIndex {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

        let mut rnd = StdRng::seed_from_u64(42);
        let mut index = GeoMapIndex::new(db, FIELD_NAME);

        index.recreate().unwrap();

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values..=num_geo_values);
            let array_payload = Value::Array(geo_points);
            let payload = MultiValue::one(&array_payload);
            index.add_point(idx as PointOffsetType, &payload).unwrap();
        }
        assert_eq!(index.points_count(), num_points);
        assert_eq!(index.points_values_count(), num_points * num_geo_values);

        index
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

    #[test]
    fn test_polygon_with_exclusion() {
        fn check_cardinality_match(hashes: Vec<GeoHash>, field_condition: FieldCondition) {
            let field_index = build_random_index(500, 20);
            let exact_points_for_hashes = field_index.get_iterator(hashes).collect_vec();
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
            exterior: europe.clone(),
            interiors: Some(vec![berlin.clone()]),
        };
        check_cardinality_match(
            polygon_hashes(&europe_no_berlin, GEO_QUERY_MAX_REGION).unwrap(),
            condition_for_geo_polygon("test".to_string(), europe_no_berlin.clone()),
        );
    }

    #[test]
    fn match_cardinality() {
        fn check_cardinality_match(hashes: Vec<GeoHash>, field_condition: FieldCondition) {
            let field_index = build_random_index(500, 20);
            let exact_points_for_hashes = field_index.get_iterator(hashes).collect_vec();
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
            condition_for_geo_radius("test".to_string(), geo_radius.clone()),
        );

        // geo_polygon cardinality check
        let geo_polygon = radius_to_polygon(&geo_radius);
        let polygon_hashes = polygon_hashes(&geo_polygon, GEO_QUERY_MAX_REGION).unwrap();
        check_cardinality_match(
            polygon_hashes,
            condition_for_geo_polygon("test".to_string(), geo_polygon),
        );
    }

    #[test]
    fn geo_indexed_filtering() {
        fn check_geo_indexed_filtering<F>(field_condition: FieldCondition, check_fn: F)
        where
            F: Fn(&GeoPoint) -> bool,
        {
            let field_index = build_random_index(1000, 5);

            let mut matched_points = (0..field_index.count_indexed_points() as PointOffsetType)
                .map(|idx| (idx, field_index.get_values(idx).unwrap()))
                .filter(|(_idx, geo_points)| geo_points.iter().any(&check_fn))
                .map(|(idx, _geo_points)| idx as PointOffsetType)
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
            condition_for_geo_radius("test".to_string(), geo_radius.clone()),
            |geo_point| geo_radius.check_point(geo_point),
        );

        let geo_polygon: GeoPolygon = build_polygon(vec![
            (-60.0, 37.0),
            (-60.0, 45.0),
            (-50.0, 45.0),
            (-50.0, 37.0),
            (-60.0, 37.0),
        ]);
        check_geo_indexed_filtering(
            condition_for_geo_polygon("test".to_string(), geo_polygon.clone()),
            |geo_point| geo_polygon.convert().check_point(geo_point),
        );
    }

    #[test]
    fn test_payload_blocks() {
        let field_index = build_random_index(1000, 5);
        let top_level_points = field_index.get_points_of_hash(&Default::default());
        assert_eq!(top_level_points, 1_000);
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
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

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
        let payload = MultiValue::one(&geo_values);
        index.add_point(1, &payload).unwrap();

        // around NYC
        let nyc_geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test".to_string(), nyc_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition =
            condition_for_geo_polygon("test".to_string(), radius_to_polygon(&nyc_geo_radius));
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
        let field_condition =
            condition_for_geo_radius("test".to_string(), berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        let field_condition =
            condition_for_geo_polygon("test".to_string(), radius_to_polygon(&berlin_geo_radius));
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
        let field_condition =
            condition_for_geo_radius("test".to_string(), tokyo_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);

        let field_condition =
            condition_for_geo_polygon("test".to_string(), radius_to_polygon(&tokyo_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);
    }

    #[test]
    fn match_cardinality_point_with_multi_close_geo_payload() {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

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
        let payload = MultiValue::one(&geo_values);
        index.add_point(1, &payload).unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };
        // check with geo_radius
        let field_condition =
            condition_for_geo_radius("test".to_string(), berlin_geo_radius.clone());
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // handle properly that a single point matches via two different geo payloads
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test".to_string(), radius_to_polygon(&berlin_geo_radius));
        let card = index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);
    }

    #[test]
    fn load_from_disk() {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        {
            let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();

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
            let payload = MultiValue::one(&geo_values);
            index.add_point(1, &payload).unwrap();
            index.flusher()().unwrap();
            drop(index);
        }

        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let mut new_index = GeoMapIndex::new(db, FIELD_NAME);
        new_index.load().unwrap();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };

        // check with geo_radius
        let field_condition =
            condition_for_geo_radius("test".to_string(), berlin_geo_radius.clone());
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1]);

        // check with geo_polygon
        let field_condition =
            condition_for_geo_polygon("test".to_string(), radius_to_polygon(&berlin_geo_radius));
        let point_offsets = new_index.filter(&field_condition).unwrap().collect_vec();
        assert_eq!(point_offsets, vec![1]);
    }

    #[test]
    fn same_geo_index_between_points_test() {
        let temp_dir = Builder::new().prefix("test_dir").tempdir().unwrap();
        {
            let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
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
            let payload = MultiValue::one(&geo_values);
            index.add_point(1, &payload).unwrap();
            index.add_point(2, &payload).unwrap();
            index.remove_point(1).unwrap();
            index.flusher()().unwrap();

            assert_eq!(index.points_count(), 1);
            assert_eq!(index.points_values_count(), 2);
            drop(index);
        }

        let db = open_db_with_existing_cf(&temp_dir.path().join("test_db")).unwrap();
        let mut new_index = GeoMapIndex::new(db, FIELD_NAME);
        new_index.load().unwrap();
        assert_eq!(new_index.points_count(), 1);
        assert_eq!(new_index.points_values_count(), 2);
    }

    #[test]
    fn test_empty_index_cardinality() {
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

        let field_index = build_random_index(0, 0);
        assert!(field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);

        let field_index = build_random_index(0, 100);
        assert!(field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);

        let field_index = build_random_index(100, 100);
        assert!(!field_index
            .match_cardinality(&hashes)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
        assert!(!field_index
            .match_cardinality(&hashes_with_interior)
            .equals_min_exp_max(&CardinalityEstimation::exact(0)),);
    }
}
