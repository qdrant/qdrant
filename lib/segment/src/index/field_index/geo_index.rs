use crate::index::field_index::geo_hash::{
    circle_hashes, decompose_geo_hash, encode_max_precision, geo_hash_to_box, rectangle_hashes,
    GeoHash,
};
use crate::index::field_index::{
    CardinalityEstimation, FieldIndex, PayloadBlockCondition, PayloadFieldIndex,
    PayloadFieldIndexBuilder, PrimaryCondition,
};
use crate::types::{FieldCondition, GeoPoint, PayloadKeyType, PayloadType, PointOffsetType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::mem;

/// Max number of sub-regions computed for an input geo query
// TODO discuss value, should it be dynamically computed?
const GEO_QUERY_MAX_REGION: usize = 12;

// Max number of points per geo_hash in the index to keep lookup fast
const GEO_MAX_BUCKET_SIZE: usize = 10_000;

fn compute_cardinality_per_region(
    points_map: &BTreeMap<GeoHash, Vec<PointOffsetType>>,
) -> BTreeMap<GeoHash, usize> {
    let mut cardinalities: BTreeMap<GeoHash, usize> = BTreeMap::new();
    points_map.iter().for_each(|(geo_hash, points)| {
        let points_count = points.len();
        let geo_hashes = decompose_geo_hash(geo_hash);
        geo_hashes.into_iter().for_each(|g| {
            let count_for_g = cardinalities.entry(g).or_insert(0);
            *count_for_g += points_count;
        });
    });
    cardinalities
}

fn group_points_per_region(
    cardinality_info: &BTreeMap<GeoHash, usize>,
    points_map: &BTreeMap<GeoHash, Vec<PointOffsetType>>,
    max_bucket_size: usize,
) -> BTreeMap<GeoHash, Vec<PointOffsetType>> {
    let top_regions: HashSet<(&String, &usize)> = cardinality_info
        .iter()
        .filter(|(geo_hash, _count)| geo_hash.len() == 1)
        .collect();
    let mut groups = BTreeMap::new();
    top_regions.into_iter().for_each(|(top, top_count)| {
        // find cutoff point in cardinality for each top region
        let cutoff = if top_count < &max_bucket_size {
            // no need for a cutoff points
            None
        } else {
            // the top region contains too many elements, find the cutoff points
            cardinality_info
                .iter()
                .rev() // start from the highest precision region
                .filter(|&(geo_hash, _count)| geo_hash.starts_with(top)) // filter tiles from top region
                .filter(|&(geo_hash, _count)| geo_hash.len() < 12) // filter out leave regions
                .take_while(|&(_geo_hash, count)| count <= &max_bucket_size) // take while under limit
                .map(|(k, _)| k.clone())
                .next()
        };
        match cutoff {
            None => {
                points_map
                    .iter()
                    .filter(|(k, _points)| k.starts_with(top))
                    .for_each(|(k, points)| {
                        groups
                            .entry(k.clone())
                            .or_insert_with(Vec::new)
                            .extend(points);
                    });
            }
            Some(cut) => {
                points_map
                    .iter()
                    .filter(|(k, _points)| k.starts_with(&cut))
                    .for_each(|(_k, points)| {
                        // aggregate points into the cutoff entry
                        groups
                            .entry(cut.clone())
                            .or_insert_with(Vec::new)
                            .extend(points);
                    });
            }
        }
    });
    groups
}

#[derive(Serialize, Deserialize, Default)]
pub struct PersistedGeoMapIndex {
    cardinality_map: BTreeMap<GeoHash, usize>,
    points_map: BTreeMap<GeoHash, Vec<PointOffsetType>>,
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
                    .filter_map(|g| self.cardinality_map.get(*g))
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
            self.points_map
                .entry(geo_hash)
                .or_insert_with(Vec::new)
                .push(idx);
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
            .map(move |(geo_hash, point_ids)| PayloadBlockCondition {
                condition: FieldCondition {
                    key: key.clone(),
                    r#match: None,
                    range: None,
                    geo_bounding_box: Some(geo_hash_to_box(geo_hash)),
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
        let input = BTreeMap::from([
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
                _ => panic!(),
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
        let input = BTreeMap::from([
            ("dr5ruj4477kb".to_string(), vec![10]),
            ("dr5ruj4477kc".to_string(), vec![11]),
            ("dr5ruj4477kd".to_string(), vec![12]),
            ("dr5ruj4477ke".to_string(), vec![13]),
            ("dr5ruj4477kf".to_string(), vec![14]),
            ("dr5ruj4477kg".to_string(), vec![15]),
            ("dr5ruj4477kh".to_string(), vec![16]),
            ("dr5ruj4477ki".to_string(), vec![17]),
            ("dr5ruj4477kj".to_string(), vec![18]),
            ("dr5ruj4477kk".to_string(), vec![19]),
            ("dr5ruj4477km".to_string(), vec![20]),
            ("u33dc1v0xupz".to_string(), vec![42]),
        ]);
        let cardinality = compute_cardinality_per_region(&input);

        // max bucket size not reached, use the highest precision
        let groups = group_points_per_region(&cardinality, &input, 12);
        assert_eq!(groups, input);

        // max bucket size reached - aggregate points in lower precision
        let groups = group_points_per_region(&cardinality, &input, 11);
        // aggregates points into "dr5ruj4477k" region
        let expected = BTreeMap::from([
            (
                "dr5ruj4477k".to_string(),
                vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            ),
            ("u33dc1v0xupz".to_string(), vec![42]),
        ]);
        assert_eq!(groups, expected);
    }
}
