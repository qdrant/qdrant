use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use segment::types::{ExtendedPointId, ScoredPoint};
use serde_json::Value;

use super::types::AggregatorError::{self, *};
use super::types::{Group, GroupKey, HashablePoint};

type Hits = HashSet<HashablePoint>;

pub(super) struct GroupsAggregator {
    groups: HashMap<GroupKey, Hits>,
    max_group_size: usize,
    grouped_by: String,
    max_groups: usize,
    full_groups: HashSet<GroupKey>,
}

impl GroupsAggregator {
    pub(super) fn new(groups: usize, group_size: usize, grouped_by: String) -> Self {
        Self {
            groups: HashMap::with_capacity(groups),
            max_group_size: group_size,
            grouped_by,
            max_groups: groups,
            full_groups: HashSet::with_capacity(groups),
        }
    }

    /// Adds a point to the group that corresponds based on the group_by field, assumes that the point has the group_by field
    fn add_point(&mut self, point: &ScoredPoint) -> Result<(), AggregatorError> {
        if self.full_groups.len() == self.max_groups {
            return Err(AllGroupsFull);
        }

        // if the key contains multiple values, grabs the first one
        let group_key = point
            .payload
            .as_ref()
            .and_then(|p| p.get_value(&self.grouped_by).next().cloned())
            .ok_or(KeyNotFound)
            .and_then(GroupKey::try_from)?;

        let group = self
            .groups
            .entry(group_key.clone())
            .or_insert_with(|| HashSet::with_capacity(self.max_group_size));

        group.insert(HashablePoint::minimal_from(point));

        if group.len() == self.max_group_size {
            self.full_groups.insert(group_key);
        }

        Ok(())
    }

    /// Adds multiple points to the group that they corresponds based on the group_by field, assumes that the points always have the grouped_by field, else it just ignores them
    pub(super) fn add_points(&mut self, points: &[ScoredPoint]) {
        points
            .iter()
            .map(|point| self.add_point(point))
            .any(|r| matches!(r, Err(AllGroupsFull)));
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.groups.len()
    }

    // Gets the keys of the groups that have less than the max group size
    pub(super) fn keys_of_unfilled_groups(&self) -> Vec<Value> {
        self.groups
            .iter()
            .filter(|(_, hits)| hits.len() < self.max_group_size)
            .map(|(key, _)| key.0.clone())
            .collect()
    }

    // gets the keys of the groups that have reached the max group size
    pub(super) fn keys_of_filled_groups(&self) -> Vec<Value> {
        self.full_groups.iter().map(|k| k.0.clone()).collect()
    }

    /// Gets the ids of the already present points across all the groups
    pub(super) fn ids(&self) -> HashSet<ExtendedPointId> {
        self.groups
            .iter()
            .flat_map(|(_, hits)| hits.iter())
            .map(|p| p.0.id)
            .collect()
    }

    // Returns the best groups sorted by their best hit. The hits are sorted too
    pub(super) fn distill(&self) -> Vec<Group> {
        self.groups
            .iter()
            .map(|(k, v)| {
                let hits = v
                    .iter()
                    .sorted()
                    .rev()
                    .take(self.max_group_size)
                    .cloned()
                    .map(ScoredPoint::from)
                    .collect::<Vec<_>>();
                let mut group_id = serde_json::Map::new();
                group_id.insert(self.grouped_by.clone(), k.0.clone());
                Group { hits, group_id }
            })
            // TODO: what happens when the best groups are not the most filled ones?
            .sorted_by_key(|group| group.hits.len())
            .rev()
            .take(self.max_groups)
            .sorted_by_key(|group| group.hits.get(0).cloned())
            .rev()
            .collect()
    }
}

#[cfg(test)]
mod unit_tests {

    use segment::types::Payload;
    use serde_json::json;

    use super::*;

    /// Used for convenience
    impl From<&str> for GroupKey {
        fn from(s: &str) -> Self {
            Self(serde_json::Value::String(s.to_string()))
        }
    }

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
                json!("a"),
                // expected group size
                1,
                // expected groups count
                1,
                // expected result
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 1.into(), // same id as the previous one
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                json!("a"),
                1, // should not add it because it already has a point with the same id
                1,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 2.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                json!("a"),
                2,
                1, // add it to same group
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 3.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                json!("a"),
                3, // group already full
                1,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 4.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "b"}))),
                    vector: None,
                },
                json!("b"),
                1,
                2,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 5.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": 3}))),
                    vector: None,
                },
                json!(3),
                1,
                3,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 6.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "d"}))),
                    vector: None,
                },
                json!("d"),
                1, // already enough groups
                4,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 7.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "b"}))),
                    vector: None,
                },
                json!("b"),
                2,
                4,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 8.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": false}))),
                    vector: None,
                },
                json!("false"),
                0,
                4,
                Err(BadKeyType),
            ),
            (
                &ScoredPoint {
                    id: 9.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
                json!("none"),
                0,
                4,
                Err(KeyNotFound),
            ),
            (
                &ScoredPoint {
                    id: 10.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": 3}))),
                    vector: None,
                },
                json!(3),
                2,
                4,
                Ok(()),
            ),
            (
                &ScoredPoint {
                    id: 11.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": 3}))),
                    vector: None,
                },
                json!(3),
                2,
                4,
                Err(AllGroupsFull),
            ),
        ]
        .into_iter()
        .enumerate()
        .for_each(|(_case, (point, key, size, groups, res))| {
            let result = aggregator.add_point(point);

            assert_eq!(result, res, "case {_case}");

            assert_eq!(aggregator.len(), groups, "case {_case}");

            let key = &GroupKey(key);
            if size > 0 {
                assert_eq!(
                    aggregator.groups.get(key).unwrap().len(),
                    size,
                    "case {_case}"
                );
            } else {
                assert!(aggregator.groups.get(key).is_none(), "case {_case}");
            }
        });

        // assert final groups
        assert_eq!(aggregator.full_groups.len(), 3);

        let groups = aggregator.groups;

        [
            (
                GroupKey::from("a"),
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
                    ScoredPoint {
                        id: 3.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                    },
                ]
                .to_vec(),
            ),
            (
                GroupKey::from("b"),
                [
                    ScoredPoint {
                        id: 4.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                    },
                    ScoredPoint {
                        id: 7.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                    },
                ]
                .to_vec(),
            ),
            (
                GroupKey(json!(3)),
                [
                    ScoredPoint {
                        id: 5.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                    },
                    ScoredPoint {
                        id: 10.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                    },
                ]
                .to_vec(),
            ),
            (
                GroupKey::from("d"),
                [ScoredPoint {
                    id: 6.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                }]
                .to_vec(),
            ),
        ]
        .iter()
        .for_each(|(key, points)| {
            let group = groups.get(key).unwrap();

            for point in points {
                assert!(group.contains(&point.clone().into()));
            }

            assert_eq!(group.len(), points.len());
        });
    }
}
