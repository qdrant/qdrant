use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use priority_queue::PriorityQueue;
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
    best_group_keys: PriorityQueue<GroupKey, HashablePoint>,
}

impl GroupsAggregator {
    pub(super) fn new(groups: usize, group_size: usize, grouped_by: String) -> Self {
        Self {
            groups: HashMap::with_capacity(groups),
            max_group_size: group_size,
            grouped_by,
            max_groups: groups,
            full_groups: HashSet::with_capacity(groups),
            best_group_keys: PriorityQueue::with_capacity(groups),
        }
    }

    /// Adds a point to the group that corresponds based on the group_by field, assumes that the point has the group_by field
    fn add_point(&mut self, point: ScoredPoint) -> Result<(), AggregatorError> {
        // if the key contains multiple values, grabs the first one
        let group_key = point
            .payload
            .as_ref()
            .and_then(|p| p.get_value(&self.grouped_by).into_iter().next().cloned())
            .ok_or(KeyNotFound)
            .and_then(GroupKey::try_from)?;

        let point = HashablePoint::minimal_from(point);

        let group = self
            .groups
            .entry(group_key.clone())
            .or_insert_with(|| HashSet::with_capacity(self.max_group_size));

        group.insert(point.clone());

        if group.len() == self.max_group_size {
            self.full_groups.insert(group_key.clone());
        }

        // keep track of best groups
        self.best_group_keys.push_increase(group_key, point);

        Ok(())
    }

    /// Adds multiple points to the group that they corresponds based on the group_by field, assumes that the points always have the grouped_by field, else it just ignores them
    pub(super) fn add_points(&mut self, points: &[ScoredPoint]) {
        for point in points {
            match self.add_point(point.to_owned()) {
                Ok(()) | Err(KeyNotFound | BadKeyType) => continue, // ignore points that don't have the group_by field
            }
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.groups.len()
    }

    // Gets the keys of the groups that have less than the max group size
    pub(super) fn keys_of_unfilled_best_groups(&self) -> Vec<Value> {
        self.best_group_keys
            .clone()
            .into_sorted_vec()
            .into_iter()
            .take(self.max_groups)
            .filter_map(|key| {
                let hits = self.groups.get(&key)?;

                if hits.len() < self.max_group_size {
                    Some(Value::from(key))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Gets the keys of the groups that have reached the max group size
    pub(super) fn keys_of_filled_groups(&self) -> Vec<Value> {
        self.full_groups.iter().cloned().map(Value::from).collect()
    }

    /// Gets the amount of best groups that have reached the max group size
    pub(super) fn len_of_filled_best_groups(&self) -> usize {
        self.best_group_keys
            .clone()
            .into_sorted_vec()
            .iter()
            .take(self.max_groups)
            .filter(|key| self.full_groups.contains(key))
            .count()
    }

    /// Gets the ids of the already present points across all the groups
    pub(super) fn ids(&self) -> HashSet<ExtendedPointId> {
        self.groups
            .iter()
            .flat_map(|(_, hits)| hits.iter())
            .map(|p| p.id())
            .collect()
    }

    /// Returns the best groups sorted by their best hit. The hits are sorted too.
    pub(super) fn distill(&self) -> Vec<Group> {
        self.best_group_keys
            .clone()
            .into_sorted_vec()
            .iter()
            .filter_map(|key| {
                let hits = self
                    .groups
                    .get(key)? // it should always have it
                    .iter()
                    .sorted_by(|a, b| b.cmp(a))
                    .take(self.max_group_size)
                    .cloned()
                    .collect::<Vec<_>>();

                Some(Group {
                    hits,
                    key: key.clone(),
                    group_by: self.grouped_by.clone(),
                })
            })
            .take(self.max_groups)
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
            Self::String(s.to_string())
        }
    }

    #[test]
    fn it_adds_single_points() {
        let mut aggregator = GroupsAggregator::new(3, 2, "docId".to_string());

        // cases
        [
            (
                // point
                ScoredPoint {
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
                ScoredPoint {
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
                ScoredPoint {
                    id: 2.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": ["a", "b"]}))),
                    vector: None,
                },
                json!("a"),
                2,
                1, // add it to same group
                Ok(()),
            ),
            (
                ScoredPoint {
                    id: 3.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "a"}))),
                    vector: None,
                },
                json!("a"),
                3, // group already full, but will still keep growing (we want the best results)
                1,
                Ok(()),
            ),
            (
                ScoredPoint {
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
                ScoredPoint {
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
                ScoredPoint {
                    id: 6.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": "d"}))),
                    vector: None,
                },
                json!("d"),
                1, // already enough groups, but we still want to grow to keep the best ones
                4,
                Ok(()),
            ),
            (
                ScoredPoint {
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
                ScoredPoint {
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
                ScoredPoint {
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
                ScoredPoint {
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
                ScoredPoint {
                    id: 11.into(),
                    version: 0,
                    score: 1.0,
                    payload: Some(Payload::from(serde_json::json!({"docId": 3}))),
                    vector: None,
                },
                json!(3),
                3,
                4,
                Ok(()),
            ),
        ]
        .into_iter()
        .enumerate()
        .for_each(|(_case, (point, key, size, groups, res))| {
            let result = aggregator.add_point(point);

            assert_eq!(result, res, "case {_case}");

            assert_eq!(aggregator.len(), groups, "case {_case}");

            let key = &GroupKey::try_from(key).unwrap();
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
                GroupKey::try_from(json!(3)).unwrap(),
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
                    ScoredPoint {
                        id: 11.into(),
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
