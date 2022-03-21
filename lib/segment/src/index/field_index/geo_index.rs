use crate::index::field_index::geo_hash::{
    circle_hashes, common_hash_prefix, encode_max_precision, geo_hash_to_box, rectangle_hashes,
    GeoHash,
};
use crate::index::field_index::stat_tools::estimate_multi_value_selection_cardinality;
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, PrimaryCondition, ValueIndexer,
};
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType, PointOffsetType};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::{max, min};
use std::collections::{BTreeMap, HashSet};
use std::mem;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

#[derive(Serialize, Deserialize, Default)]
pub struct PersistedGeoMapIndex {
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
}

impl PersistedGeoMapIndex {
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

    #[allow(dead_code)]
    fn remove_point(&mut self, idx: PointOffsetType) {
        if self.point_to_values.len() <= idx as usize {
            return; // Already removed or never actually existed
        }
        let removed_points = std::mem::take(&mut self.point_to_values[idx as usize]);
        let mut seen_hashes: HashSet<&str> = Default::default();
        let mut geo_hashes = vec![];

        for removed_point in removed_points {
            let removed_geo_hash: GeoHash =
                encode_max_precision(removed_point.lon, removed_point.lat).unwrap();
            geo_hashes.push(removed_geo_hash);
        }

        for removed_geo_hash in &geo_hashes {
            let hash_points = self.points_map.get_mut(removed_geo_hash);
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
    }

    fn add_many_geo_points(&mut self, idx: PointOffsetType, values: &[GeoPoint]) {
        if self.point_to_values.len() <= idx as usize {
            // That's a smart reallocation
            self.point_to_values.resize(idx as usize + 1, vec![]);
        }
        self.point_to_values[idx as usize] = values.to_vec();

        let mut seen_hashes: HashSet<&str> = Default::default();
        let mut geo_hashes = vec![];

        for added_point in values {
            let added_geo_hash: GeoHash =
                encode_max_precision(added_point.lon, added_point.lat).unwrap();
            geo_hashes.push(added_geo_hash);
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

impl ValueIndexer<GeoPoint> for PersistedGeoMapIndex {
    fn add_many(&mut self, id: PointOffsetType, values: Vec<GeoPoint>) {
        self.add_many_geo_points(id, &values)
    }

    fn get_value(&self, value: &Value) -> Option<GeoPoint> {
        match value {
            Value::Object(obj) => {
                let lon_op = obj.get("lon").and_then(|x| x.as_f64());
                let lat_op = obj.get("lat").and_then(|x| x.as_f64());

                if let (Some(lon), Some(lat)) = (lon_op, lat_op) {
                    return Some(GeoPoint { lon, lat });
                }
                None
            }
            _ => None,
        }
    }
}

impl PayloadFieldIndexBuilder for PersistedGeoMapIndex {
    fn add(&mut self, id: PointOffsetType, value: &Value) {
        self.add_point(id, value)
    }

    fn build(&mut self) -> FieldIndex {
        /*
        points_map contains full hashes:
        {
            "dr5ruj4477ku": [1,2,3],
            "dr5ruj4477kk": [1,2,4],
            "dr5ruj4477k7": [1,2,5],
            ...
        }
         */
        let points_map = mem::take(&mut self.points_map);
        let point_to_values = mem::take(&mut self.point_to_values);
        let points_per_hash = mem::take(&mut self.points_per_hash);
        let values_per_hash = mem::take(&mut self.values_per_hash);

        let values_count = self.values_count;
        let points_count = self.points_count;
        let max_values_per_point = self.max_values_per_point;

        FieldIndex::GeoIndex(PersistedGeoMapIndex {
            points_per_hash,
            values_per_hash,
            points_map,
            point_to_values,
            values_count,
            points_count,
            max_values_per_point,
        })
    }
}

impl PayloadFieldIndex for PersistedGeoMapIndex {
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
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: None,
                        range: None,
                        geo_bounding_box: Some(geo_hash_to_box(geo_hash)),
                        geo_radius: None,
                    },
                    cardinality: size,
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::types::GeoRadius;
    use itertools::Itertools;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use serde_json::json;

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

    fn condition_for_geo_radius(key: String, geo_radius: GeoRadius) -> FieldCondition {
        FieldCondition {
            key,
            r#match: None,
            range: None,
            geo_bounding_box: None,
            geo_radius: Some(geo_radius),
        }
    }

    fn build_random_index(num_points: usize, num_geo_values: usize) -> FieldIndex {
        let mut rnd = StdRng::seed_from_u64(42);
        let mut index = PersistedGeoMapIndex::default();

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values);
            index.add(idx as PointOffsetType, &Value::Array(geo_points))
        }
        assert_eq!(index.points_count, num_points);
        assert_eq!(index.values_count, num_points * num_geo_values);

        index.build()
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

        let real_cardinality = match &field_index {
            FieldIndex::GeoIndex(geo_index) => {
                let exact_points_for_hashes = geo_index.get_iterator(nyc_hashes).collect_vec();
                exact_points_for_hashes.len()
            }
            _ => panic!("Wrong index created"),
        };

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

        let mut matched_points = match &field_index {
            FieldIndex::GeoIndex(geo_index) => geo_index
                .point_to_values
                .iter()
                .enumerate()
                .filter(|(_idx, geo_points)| {
                    geo_points
                        .iter()
                        .any(|geo_point| geo_radius.check_point(geo_point.lon, geo_point.lat))
                })
                .map(|(idx, _geo_points)| idx as PointOffsetType)
                .collect_vec(),
            _ => panic!("wrong index"),
        };

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
        match &field_index {
            FieldIndex::GeoIndex(geo_index) => {
                let top_level_points = geo_index.points_per_hash.get("").unwrap();
                assert_eq!(*top_level_points, 1_000);
                let block_hashes = geo_index.get_large_hashes(100).collect_vec();
                assert!(!block_hashes.is_empty());
                for (geohash, size) in block_hashes {
                    assert_eq!(geohash.len(), 1);
                    assert!(size > 100);
                    assert!(size < 1000);
                }
            }
            _ => panic!("wrong index"),
        };
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
        let mut index = PersistedGeoMapIndex::default();

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

        index.add(1, &geo_values);
        let field_index = index.build();

        // around NYC
        let nyc_geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };
        let field_condition = condition_for_geo_radius("test".to_string(), nyc_geo_radius);
        let card = field_index.estimate_cardinality(&field_condition);
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
        let card = field_index.estimate_cardinality(&field_condition);
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
        let card = field_index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // no points found
        assert_eq!(card.min, 0);
        assert_eq!(card.max, 0);
        assert_eq!(card.exp, 0);
    }

    #[test]
    fn match_cardinality_point_with_multi_close_geo_payload() {
        let mut index = PersistedGeoMapIndex::default();
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
        index.add(1, &geo_values);
        let field_index = index.build();

        let berlin_geo_radius = GeoRadius {
            center: BERLIN,
            radius: 50_000.0, // Berlin <-> Potsdam is 27 km
        };
        let field_condition = condition_for_geo_radius("test".to_string(), berlin_geo_radius);
        let card = field_index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        // handle properly that a single point matches via two different geo payloads
        assert_eq!(card.min, 1);
        assert_eq!(card.max, 1);
        assert_eq!(card.exp, 1);
    }
}
