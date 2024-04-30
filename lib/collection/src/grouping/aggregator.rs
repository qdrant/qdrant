use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use segment::data_types::groups::GroupId;
use segment::json_path::JsonPath;
use segment::spaces::tools::{peek_top_largest_iterable, peek_top_smallest_iterable};
use segment::types::{ExtendedPointId, Order, PayloadContainer, PointIdType, ScoredPoint};
use serde_json::Value;

use super::types::AggregatorError::{self, *};
use super::types::Group;

type Hits = HashMap<PointIdType, ScoredPoint>;
pub(super) struct GroupsAggregator {
    groups: HashMap<GroupId, Hits>,
    max_group_size: usize,
    grouped_by: JsonPath,
    max_groups: usize,
    full_groups: HashSet<GroupId>,
    group_best_scores: HashMap<GroupId, ScoreType>,
    all_ids: HashSet<ExtendedPointId>,
    order: Order,
}

impl GroupsAggregator {
    pub(super) fn new(
        groups: usize,
        group_size: usize,
        grouped_by: JsonPath,
        order: Order,
    ) -> Self {
        Self {
            groups: HashMap::with_capacity(groups),
            max_group_size: group_size,
            grouped_by,
            max_groups: groups,
            full_groups: HashSet::with_capacity(groups),
            group_best_scores: HashMap::with_capacity(groups),
            all_ids: HashSet::with_capacity(groups * group_size),
            order,
        }
    }

    /// Adds a point to the group that corresponds based on the group_by field, assumes that the point has the group_by field
    fn add_point(&mut self, point: ScoredPoint) -> Result<(), AggregatorError> {
        // extract all values from the group_by field
        let payload_values: Vec<_> = point
            .payload
            .as_ref()
            .map(|p| {
                p.get_value(&self.grouped_by)
                    .into_iter()
                    .flat_map(|v| match v {
                        Value::Array(arr) => arr.iter().collect(),
                        _ => vec![v],
                    })
                    .collect()
            })
            .ok_or(KeyNotFound)?;

        let group_keys = payload_values
            .into_iter()
            .map(GroupId::try_from)
            .collect::<Result<Vec<GroupId>, ()>>()
            .map_err(|_| BadKeyType)?;

        let unique_group_keys: Vec<_> = group_keys.into_iter().unique().collect();

        for group_key in unique_group_keys {
            let group = self
                .groups
                .entry(group_key.clone())
                .or_insert_with(|| HashMap::with_capacity(self.max_group_size));

            let entry = group.entry(point.id);

            // if the point is already in the group, check if it has newer version
            match entry {
                Entry::Occupied(mut o) => {
                    if o.get().version < point.version {
                        o.insert(point.clone());
                    }
                }
                Entry::Vacant(v) => {
                    v.insert(point.clone());
                    self.all_ids.insert(point.id);
                }
            }

            if group.len() == self.max_group_size {
                self.full_groups.insert(group_key.clone());
            }

            // Insert score if better than the group best score
            self.group_best_scores
                .entry(group_key.clone())
                .and_modify(|e| {
                    *e = match self.order {
                        Order::LargeBetter => point.score.max(*e),
                        Order::SmallBetter => point.score.min(*e),
                    }
                })
                .or_insert(point.score);
        }
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

    /// Return `max_groups` number of keys of the groups with the best score
    fn best_group_keys(&self) -> impl Iterator<Item = &GroupId> {
        self.group_best_scores
            .iter()
            .sorted_by_key(|(_, score)| match self.order {
                Order::LargeBetter => -OrderedFloat(**score),
                Order::SmallBetter => OrderedFloat(**score),
            })
            .take(self.max_groups)
            .map(|(k, _)| k)
    }

    /// Gets the keys of the groups that have less than the max group size
    pub(super) fn keys_of_unfilled_best_groups(&self) -> Vec<Value> {
        let best_group_keys: HashSet<_> = self.best_group_keys().cloned().collect();
        best_group_keys
            .difference(&self.full_groups)
            .cloned()
            .map_into()
            .collect()
    }

    /// Gets the keys of the groups that have reached the max group size
    pub(super) fn keys_of_filled_groups(&self) -> Vec<Value> {
        self.full_groups.iter().cloned().map_into().collect()
    }

    /// Gets the amount of best groups that have reached the max group size
    pub(super) fn len_of_filled_best_groups(&self) -> usize {
        let best_group_keys: HashSet<_> = self.best_group_keys().cloned().collect();
        best_group_keys.intersection(&self.full_groups).count()
    }

    /// Gets the ids of the already present points across all the groups
    pub(super) fn ids(&self) -> &HashSet<ExtendedPointId> {
        &self.all_ids
    }

    /// Returns the best groups sorted by their best hit. The hits are sorted too.
    pub(super) fn distill(mut self) -> Vec<Group> {
        let best_groups: Vec<_> = self.best_group_keys().cloned().collect();
        let mut groups = Vec::with_capacity(best_groups.len());

        for group_key in best_groups {
            let mut group = self.groups.remove(&group_key).unwrap();
            let scored_points_iter = group.drain().map(|(_, hit)| hit);
            let hits = match self.order {
                Order::LargeBetter => {
                    peek_top_largest_iterable(scored_points_iter, self.max_group_size)
                }
                Order::SmallBetter => {
                    peek_top_smallest_iterable(scored_points_iter, self.max_group_size)
                }
            };
            groups.push(Group {
                hits,
                key: group_key,
            });
        }

        groups
    }
}

#[cfg(test)]
mod unit_tests {

    use segment::types::Payload;
    use serde_json::json;

    use super::*;

    fn point(idx: u64, score: ScoreType, payloads: Value) -> ScoredPoint {
        ScoredPoint {
            id: idx.into(),
            version: 0,
            score,
            payload: Some(Payload::from(serde_json::json!({ "docId": payloads }))),
            vector: None,
            shard_key: None,
        }
    }

    fn empty_point(idx: u64, score: ScoreType) -> ScoredPoint {
        ScoredPoint {
            id: idx.into(),
            version: 0,
            score,
            payload: None,
            vector: None,
            shard_key: None,
        }
    }

    #[test]
    fn test_group_with_multiple_payload_values() {
        let scored_points = vec![
            point(1, 0.99, json!(["a", "a"])),
            point(2, 0.85, json!(["a", "b"])),
            point(3, 0.75, json!("b")),
        ];

        let mut aggregator =
            GroupsAggregator::new(3, 2, "docId".parse().unwrap(), Order::LargeBetter);
        for point in scored_points {
            aggregator.add_point(point).unwrap();
        }

        let result = aggregator.distill();

        assert_eq!(result.len(), 2);

        assert_eq!(result[0].hits.len(), 2);
        assert_eq!(result[0].hits[0].id, 1.into());
        assert_eq!(result[0].hits[1].id, 2.into());

        assert_eq!(result[1].hits.len(), 2);
        assert_eq!(result[1].hits[0].id, 2.into());
        assert_eq!(result[1].hits[1].id, 3.into());
    }

    struct Case {
        point: ScoredPoint,
        key: Value,
        group_size: usize,
        groups_count: usize,
        expected_result: Result<(), AggregatorError>,
    }

    impl Case {
        fn new(
            key: Value,
            group_size: usize,
            groups_count: usize,
            expected_result: Result<(), AggregatorError>,
            point: ScoredPoint,
        ) -> Self {
            Self {
                point,
                key,
                group_size,
                groups_count,
                expected_result,
            }
        }
    }

    #[test]
    fn it_adds_single_points() {
        let mut aggregator =
            GroupsAggregator::new(4, 3, "docId".parse().unwrap(), Order::LargeBetter);

        // cases
        #[rustfmt::skip]
        [
            Case::new(json!("a"), 1, 1, Ok(()), point(1, 0.99, json!("a"))),
            Case::new(json!("a"), 1, 1, Ok(()), point(1, 0.97, json!("a"))), // should not add it because it already has a point with the same id
            Case::new(json!("a"), 2, 2, Ok(()), point(2, 0.81, json!(["a", "b"]))), // to both groups
            Case::new(json!("b"), 2, 2, Ok(()), point(3, 0.84, json!("b"))), // check that `b` of size 2
            Case::new(json!("a"), 3, 2, Ok(()), point(4, 0.9, json!("a"))), // grow beyond the max groups, as we sort later
            Case::new(json!(3), 1, 3, Ok(()), point(5, 0.4, json!(3))),     // check that `3` of size 2
            Case::new(json!("d"), 1, 4, Ok(()), point(6, 0.3, json!("d"))),
            Case::new(json!("a"), 4, 4, Ok(()), point(100, 0.31, json!("a"))), // small score 'a'
            Case::new(json!("a"), 5, 4, Ok(()), point(101, 0.32, json!("a"))), // small score 'a'
            Case::new(json!("a"), 6, 4, Ok(()), point(102, 0.33, json!("a"))), // small score 'a'
            Case::new(json!("a"), 7, 4, Ok(()), point(103, 0.34, json!("a"))), // small score 'a'
            Case::new(json!("a"), 8, 4, Ok(()), point(104, 0.35, json!("a"))), // small score 'a'
            Case::new(json!("a"), 9, 4, Ok(()), point(105, 0.36, json!("a"))), // small score 'a'
            Case::new(json!("b"), 3, 4, Ok(()), point(7, 1.0, json!("b"))),
            Case::new(json!("false"), 0, 4, Err(BadKeyType), point(8, 1.0, json!(false))),
            Case::new(json!("none"), 0, 4, Err(KeyNotFound), empty_point(9, 1.0)),
            Case::new(json!(3), 2, 4, Ok(()), point(10, 0.6, json!(3))),
            Case::new(json!(3), 3, 4, Ok(()), point(11, 0.1, json!(3))),
        ]
        .into_iter()
        .enumerate()
        .for_each(|(case_idx, case)| {
            let result = aggregator.add_point(case.point);

            assert_eq!(result, case.expected_result, "case {case_idx}");

            assert_eq!(aggregator.len(), case.groups_count, "case {case_idx}");

            let key = &GroupId::try_from(&case.key).unwrap();
            if case.group_size > 0 {
                assert_eq!(
                    aggregator.groups.get(key).unwrap().len(),
                    case.group_size,
                    "case {case_idx}"
                );
            } else {
                assert!(!aggregator.groups.contains_key(key), "case {case_idx}");
            }
        });

        // assert final groups
        assert_eq!(aggregator.full_groups.len(), 3);

        assert_eq!(aggregator.keys_of_unfilled_best_groups(), vec![json!("d")]);

        assert_eq!(aggregator.len_of_filled_best_groups(), 3);

        let groups = aggregator.distill();

        #[rustfmt::skip]
        let expected_groups = vec![
            (
                GroupId::from("b"),
                vec![
                    empty_point(7, 1.0),
                    empty_point(3, 0.84),
                    empty_point(2, 0.81),
                ],
            ),
            (
                GroupId::from("a"),
                vec![
                    empty_point(1, 0.99),
                    empty_point(4, 0.9),
                    empty_point(2, 0.81)
                ],
            ),
            (
                GroupId::try_from(&json!(3)).unwrap(),
                vec![
                    empty_point(10, 0.6),
                    empty_point(5, 0.4),
                    empty_point(11, 0.1),
                ],
            ),
            (
                GroupId::from("d"),
                vec![
                    empty_point(6, 0.3),
                ],
            ),
        ];

        for ((key, expected_group_points), group) in
            expected_groups.into_iter().zip(groups.into_iter())
        {
            assert_eq!(key, group.key);
            let expected_id_score: Vec<_> = expected_group_points
                .into_iter()
                .map(|x| (x.id, x.score))
                .collect();
            let group_id_score: Vec<_> = group.hits.into_iter().map(|x| (x.id, x.score)).collect();
            assert_eq!(expected_id_score, group_id_score);
        }
    }

    #[test]
    fn test_aggregate_less_groups() {
        let mut aggregator =
            GroupsAggregator::new(3, 2, "docId".parse().unwrap(), Order::LargeBetter);

        // cases
        [
            point(1, 0.99, json!("a")),
            point(1, 0.97, json!("a")), // should not add it because it already has a point with the same id
            point(2, 0.81, json!(["a", "b"])), // to both groups
            point(3, 0.84, json!("b")), // check that `b` of size 2
            point(4, 0.9, json!("a")),  // grow beyond the max groups, as we sort later
            point(5, 0.4, json!(3)),    // check that `3` of size 2
            point(6, 0.3, json!("d")),
            point(100, 0.31, json!("a")), // small score 'a'
            point(101, 0.32, json!("a")), // small score 'a'
            point(102, 0.33, json!("a")), // small score 'a'
            point(103, 0.34, json!("a")), // small score 'a'
            point(104, 0.35, json!("a")), // small score 'a'
            point(105, 0.36, json!("a")), // small score 'a'
            point(7, 1.0, json!("b")),
            point(10, 0.6, json!(3)),
            point(11, 0.1, json!(3)),
        ]
        .into_iter()
        .for_each(|point| {
            aggregator.add_point(point).unwrap();
        });

        let groups = aggregator.distill();

        #[rustfmt::skip]
        let expected_groups = vec![
            (
                GroupId::from("b"),
                vec![
                    empty_point(7, 1.0),
                    empty_point(3, 0.84),
                ],
            ),
            (
                GroupId::from("a"),
                vec![
                    empty_point(1, 0.99),
                    empty_point(4, 0.9),
                ],
            ),
            (
                GroupId::try_from(&json!(3)).unwrap(),
                vec![
                    empty_point(10, 0.6),
                    empty_point(5, 0.4),
                ],
            ),
        ];
        for ((key, expected_group_points), group) in
            expected_groups.into_iter().zip(groups.into_iter())
        {
            assert_eq!(key, group.key);
            let expected_id_score: Vec<_> = expected_group_points
                .into_iter()
                .map(|x| (x.id, x.score))
                .collect();
            let group_id_score: Vec<_> = group.hits.into_iter().map(|x| (x.id, x.score)).collect();
            assert_eq!(expected_id_score, group_id_score);
        }
    }
}
