use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use segment::types::{ExtendedPointId, ScoredPoint};

type Hits = HashSet<ScoredPoint>;

struct GroupsAggregator {
    groups: HashMap<String, Hits>,
    max_group_size: usize,
    grouped_by: String,
    max_groups: usize,
}

impl GroupsAggregator {
    fn new(groups: usize, group_size: usize, grouped_by: String) -> Self {
        Self {
            groups: HashMap::with_capacity(groups),
            max_group_size: group_size,
            grouped_by,
            max_groups: groups,
        }
    }

    /// Adds a point to the group that corresponds based on the group_by field, assumes that the point has the group_by field
    fn add_point(&mut self, point: &ScoredPoint) {
        // if the key contains multiple values, grabs the first one
        let group_value = point
            .payload
            .as_ref()
            .unwrap()
            .get_value(&self.grouped_by)
            .values()
            .first()
            .unwrap()
            .to_string()
            .trim_matches('"')
            .to_owned();

        println!("point_id: {:?}, group_value: {:#?}", point.id, group_value);

        if !self.groups.contains_key(&group_value) && self.groups.len() >= self.max_groups {
            return;
        }

        let group = self
            .groups
            .entry(group_value)
            .or_insert_with(|| HashSet::with_capacity(self.max_group_size));

        if group.len() >= self.max_group_size {
            return;
        }

        group.insert(point.clone());
    }

    /// Adds multiple points to the group that they corresponds based on the group_by field, assumes that the points always have the group_by field
    fn add_points(&mut self, points: &[ScoredPoint]) {
        print_points(points);
        points.iter().for_each(|point| self.add_point(point));
        for group in self.groups.iter() {
            println!("group: {:#?}", group);
        }
    }

    fn len(&self) -> usize {
        self.groups.len()
    }

    // gets the keys of the groups that have less than the max group size
    fn keys_of_unfilled_groups(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter(|(_, hits)| hits.len() < self.max_group_size)
            .map(|(key, _)| key.clone())
            .collect()
    }

    // gets the keys of the groups that have reached or exceeded the max group size
    fn keys_of_filled_groups(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter(|(_, hits)| hits.len() >= self.max_group_size)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Gets the ids of the already present points in all of the groups
    fn ids(&self) -> HashSet<ExtendedPointId> {
        self.groups
            .iter()
            .flat_map(|(_, hits)| hits.iter())
            .map(|p| p.id)
            .collect()
    }

    fn flatten(&self) -> Vec<ScoredPoint> {
        self.groups.values().flatten().cloned().collect()
    }

    /// Copies the payload and vector from the provided points to the points inside of each of the groups
    fn hydrate_from(&mut self, points: &[ScoredPoint]) {
        for point in points {
            self.groups.iter_mut().for_each(|(_, ps)| {
                if ps.contains(point) {
                    ps.replace(point.clone())
                        .expect("The point should be in the group before replacing it! 😱");
                }
            });
        }
    }
}

fn print_points(points: &[ScoredPoint]) {
    for point in points {
        println!("{point:?}");
    }
    println!("-------------------");
}

#[cfg(test)]
mod unit_tests {

    use segment::types::Payload;

    use super::*;

    #[test]
    fn it_adds_single_points() {
        let mut aggregator = GroupsAggregator::new(3, 2, "docId".to_string());

        // cases
        [
            (
                // point
                &ScoredPoint {
                    id: 1.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                // key
                "a",
                // group size
                1,
                // groups count
                1,
            ),
            (
                &ScoredPoint {
                    id: 1.into(), // same id as the previous one
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                "a",
                1, // should not add it because it already has a point with the same id
                1,
            ),
            (
                &ScoredPoint {
                    id: 2.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                "a",
                2,
                1, // add it to same group
            ),
            (
                &ScoredPoint {
                    id: 3.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                "a",
                2, // group already full
                1,
            ),
            (
                &ScoredPoint {
                    id: 4.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "b"}))),
                    vector: None,
                },
                "b",
                1,
                2,
            ),
            (
                &ScoredPoint {
                    id: 5.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "c"}))),
                    vector: None,
                },
                "c",
                1,
                3,
            ),
            (
                &ScoredPoint {
                    id: 6.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "d"}))),
                    vector: None,
                },
                "d",
                0, // already enough groups
                3,
            ),
        ]
        .into_iter()
        .enumerate()
        .for_each(|(_case_num, (point, key, size, groups))| {
            aggregator.add_point(point);

            assert_eq!(aggregator.len(), groups);

            if size > 0 {
                assert_eq!(aggregator.groups.get(key).unwrap().len(), size);
            } else {
                assert!(aggregator.groups.get(key).is_none());
            }
        });
    }

    #[test]
    fn hydrate_from() {
        let mut aggregator = GroupsAggregator::new(2, 2, "docId".to_string());

        aggregator.groups.insert(
            "a".to_string(),
            [
                ScoredPoint {
                    id: 1.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
                ScoredPoint {
                    id: 2.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
            ]
            .into(),
        );

        aggregator.groups.insert(
            "b".to_string(),
            [
                ScoredPoint {
                    id: 3.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
                ScoredPoint {
                    id: 4.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
            ]
            .into(),
        );

        let payload_a = Payload::from(serde_json::json!({"some_key": "some value a"}));
        let payload_b = Payload::from(serde_json::json!({"some_key": "some value b"}));

        let hydrated = vec![
            ScoredPoint {
                id: 1.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 2.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 3.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 4.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
            },
        ];

        aggregator.hydrate_from(&hydrated);

        assert_eq!(aggregator.groups.len(), 2);
        assert_eq!(aggregator.groups.get("a").unwrap().len(), 2);
        assert_eq!(aggregator.groups.get("b").unwrap().len(), 2);

        let a = aggregator.groups.get("a").unwrap();
        let b = aggregator.groups.get("b").unwrap();

        assert!(a.iter().all(|x| x.payload == Some(payload_a.clone())));
        assert!(b.iter().all(|x| x.payload == Some(payload_b.clone())));
    }
}
