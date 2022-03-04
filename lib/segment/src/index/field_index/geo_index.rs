use crate::index::field_index::geo_hash::{
    circle_hashes, decompose_geo_hash, encode_max_precision, rectangle_hashes, GeoHash,
};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, PrimaryCondition,
};
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType, PayloadType, PointOffsetType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

// Max number of points per geo_hash in the index to keep lookup fast
const GEO_MAX_BUCKET_SIZE: usize = 10_000;

fn compute_cardinality_per_region(
    points_map: &HashMap<GeoHash, Vec<PointOffsetType>>,
) -> BTreeMap<GeoHash, usize> {
    let mut cardinalities: BTreeMap<GeoHash, usize> = BTreeMap::new();
    points_map.iter().for_each(|(geo_hash, points)| {
        let points_count = points.len();
        let geo_hashes = decompose_geo_hash(geo_hash);
        geo_hashes.into_iter().for_each(|g| {
            match cardinalities.get(&g) {
                None => cardinalities.insert(g, points_count),
                Some(&count) => cardinalities.insert(g, count + points_count),
            };
        });
    });
    cardinalities
}

// FIXME
fn group_points_per_region(
    cardinality_info: &BTreeMap<GeoHash, usize>,
    points_map: &HashMap<GeoHash, Vec<PointOffsetType>>,
    max_bucket_size: usize,
) -> HashMap<GeoHash, Vec<PointOffsetType>> {
    let mut top_regions: HashSet<char> = HashSet::new();
    cardinality_info.keys().for_each(|geo_hash| {
        let first_char = geo_hash.chars().next().unwrap();
        top_regions.insert(first_char);
    });
    let mut res = HashMap::new();
    println!("max_bucket {} top {:?}", max_bucket_size, &top_regions);
    top_regions.into_iter().for_each(|top| {
        // find cutoff point in cardinality for each top region
        let cutoff = cardinality_info
            .iter()
            .rev()
            .find(|&(geo_hash, count)| {
                let first_char = geo_hash.chars().next().unwrap();
                //println!("hash {} count {}", geo_hash, count);
                first_char == top && count > &max_bucket_size
            })
            .map(|(k, _)| k.clone());
        //println!("cutoff {:?}", &cutoff);
        let prefix = cutoff.unwrap_or_else(|| top.to_string());
        points_map
            .iter()
            .filter(|(k, _points)| k.starts_with(&prefix))
            .for_each(|(k, points)| {
                res.entry(k.clone()).or_insert_with(Vec::new).extend(points);
            });
    });
    res
}

#[derive(Serialize, Deserialize, Default)]
pub struct PersistedGeoMapIndex {
    cardinality_map: BTreeMap<GeoHash, usize>,
    points_map: HashMap<GeoHash, Vec<PointOffsetType>>,
}

impl PersistedGeoMapIndex {
    pub fn match_cardinality(&self, values: &[GeoHash]) -> CardinalityEstimation {
        let mut top_regions: HashSet<char> = HashSet::new();
        values.iter().for_each(|geo_hash| {
            let first_char = geo_hash.chars().next().unwrap();
            top_regions.insert(first_char);
        });

        let mut all_possible_regions = HashSet::new();
        values.iter().for_each(|geo_hash| {
            // fallback to decreasing precision within the map if not found
            decompose_geo_hash(geo_hash).into_iter().for_each(|g| {
                all_possible_regions.insert(g);
                ()
            })
        });

        let mut values_count = 0;

        // finds the appropriate precision to use to estimate each regions
        top_regions.into_iter().for_each(|t| {
            let sub_regions: Vec<_> = all_possible_regions
                .iter()
                .filter(|r| r.starts_with(t))
                .collect();
            let highest_precision = sub_regions
                .iter()
                .max_by(|a, b| a.len().cmp(&b.len()))
                .unwrap()
                .len();
            for p in (0..=highest_precision).rev() {
                let sum_region_count: usize = sub_regions
                    .iter()
                    .filter(|r| r.len() == p)
                    .filter_map(|g| self.cardinality_map.get(g.clone()))
                    .sum();
                if sum_region_count != 0 {
                    values_count += sum_region_count;
                    break;
                }
            }
        });

        CardinalityEstimation {
            primary_clauses: vec![],
            min: values_count,
            exp: values_count,
            max: values_count,
        }
    }

    fn add_many(&mut self, idx: PointOffsetType, values: &[GeoPoint]) {
        for geo_point in values {
            let geo_hash = encode_max_precision(geo_point.lon, geo_point.lat).unwrap();
            let vec = match self.points_map.get_mut(&geo_hash) {
                None => {
                    let new_vec = vec![];
                    self.points_map.insert(geo_hash.clone(), new_vec);
                    self.points_map.get_mut(&geo_hash).unwrap()
                }
                Some(vec) => vec,
            };
            vec.push(idx);
        }
    }

    fn get_iterator(&self, values: &[GeoHash]) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let mut res: Vec<PointOffsetType> = vec![];
        values.iter().for_each(|geo_hash| {
            if let Some(vec) = self.points_map.get(geo_hash) {
                res.extend(vec)
            }
        });
        Box::new(res.into_iter())
    }
}

impl PayloadFieldIndexBuilder for PersistedGeoMapIndex {
    fn add(&mut self, id: PointOffsetType, value: &PayloadType) {
        match value {
            PayloadType::Geo(geo_points) => self.add_many(id, geo_points),
            _ => panic!("Unexpected payload type: {:?}", value),
        }
    }

    fn build(&mut self) -> FieldIndex {
        let all_ingested_points = mem::take(&mut self.points_map);

        // build more efficient representation to support queries per region
        let cardinality_map = compute_cardinality_per_region(&all_ingested_points);
        let points_map =
            group_points_per_region(&cardinality_map, &all_ingested_points, GEO_MAX_BUCKET_SIZE);

        FieldIndex::GeoIndex(PersistedGeoMapIndex {
            cardinality_map,
            points_map,
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
            return Some(self.get_iterator(&geo_hashes));
        }

        if let Some(geo_radius) = &condition.geo_radius {
            let geo_hashes = circle_hashes(geo_radius, GEO_QUERY_MAX_REGION);
            return Some(self.get_iterator(&geo_hashes));
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
        let iter = self
            .points_map
            .iter()
            .filter(move |(_geo_hash, point_ids)| point_ids.len() > threshold)
            .map(move |(_geo_hash, point_ids)| PayloadBlockCondition {
                condition: FieldCondition {
                    key: key.clone(),
                    r#match: None,
                    range: None,
                    geo_bounding_box: None, // TODO turn geohash into a filtering geo_bounding_box
                    geo_radius: None,
                },
                cardinality: point_ids.len(),
            });
        Box::new(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::payload_fixtures::random_geo_payload;
    use crate::types::GeoRadius;
    use geo::algorithm::haversine_distance::HaversineDistance;
    use geo::Point;

    const NYC: GeoPoint = GeoPoint {
        lat: 40.75798,
        lon: -73.991516,
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

    #[test]
    fn cardinality_per_region() {
        let input = HashMap::from([
            ("dr5ruj4477kd".to_string(), vec![1, 2, 3]),
            ("dr5ruj4477ku".to_string(), vec![4, 5, 6]),
        ]);
        let cardinality = compute_cardinality_per_region(&input);
        let expected = BTreeMap::from([
            ("d".to_string(), 6),
            ("dr".to_string(), 6),
            ("dr5".to_string(), 6),
            ("dr5r".to_string(), 6),
            ("dr5ru".to_string(), 6),
            ("dr5ruj".to_string(), 6),
            ("dr5ruj4".to_string(), 6),
            ("dr5ruj44".to_string(), 6),
            ("dr5ruj447".to_string(), 6),
            ("dr5ruj4477".to_string(), 6),
            ("dr5ruj4477k".to_string(), 6),
            ("dr5ruj4477kd".to_string(), 3),
            ("dr5ruj4477ku".to_string(), 3),
        ]);

        assert_eq!(cardinality, expected)
    }

    #[test]
    fn match_cardinality() {
        let mut rnd = rand::thread_rng();
        let mut index = PersistedGeoMapIndex::default();

        let r_meters = 500_000.0;
        let geo_radius = GeoRadius {
            center: NYC,
            radius: r_meters,
        };
        let nyc_point = Point::new(NYC.lon, NYC.lat);

        let num_points = 10000;
        let num_geo_values = 1;
        let mut within_condition = 0;

        for idx in 0..num_points {
            let geo_points = random_geo_payload(&mut rnd, num_geo_values);
            match &geo_points {
                PayloadType::Geo(points) => {
                    let p = Point::new(points[0].lon, points[0].lat);
                    if p.haversine_distance(&nyc_point) < r_meters {
                        within_condition += 1;
                    }
                }
                _ => {}
            }
            index.add(idx, &geo_points)
        }

        let field_index = index.build();
        let field_condition = condition_for_geo_radius("test".to_string(), geo_radius);

        let card = field_index.estimate_cardinality(&field_condition);
        let card = card.unwrap();
        assert!(card.min >= within_condition);
        assert!(card.max >= within_condition);
        assert!(card.exp >= within_condition);
    }

    #[test]
    fn group_per_region() {
        let input = HashMap::from([
            ("dr5ruj4477kd".to_string(), vec![1, 2, 3]),
            ("dr5ruj4477ku".to_string(), vec![4, 5, 6, 7]),
            ("u33dc1v0xupz".to_string(), vec![8, 9, 10, 11]),
        ]);
        let cardinality = compute_cardinality_per_region(&input);

        // max bucket size not reached
        let groups = group_points_per_region(&cardinality, &input, 5);
        assert_eq!(groups, input);

        // max bucket size reached
        let groups = group_points_per_region(&cardinality, &input, 4);
        assert_ne!(groups, input);
    }
}
