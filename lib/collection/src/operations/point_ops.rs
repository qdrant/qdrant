use std::collections::HashMap;

use ahash::AHashMap;
use api::rest::ShardKeySelector;
use itertools::izip;
use schemars::JsonSchema;
use segment::common::utils::transpose_map_into_named_vector;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::VectorInternal;
use segment::types::Filter;
use serde::{Deserialize, Serialize};
pub use shard::operations::point_ops::*;
use validator::{Validate, ValidationErrors};

use super::{OperationToShard, SplitByShard, point_to_shards, split_iter_by_shard};
use crate::hash_ring::HashRingRouter;
use crate::shards::shard::ShardId;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum PointsSelector {
    /// Select points by list of IDs
    PointIdsSelector(PointIdsList),
    /// Select points by filtering condition
    FilterSelector(FilterSelector),
}

impl Validate for PointsSelector {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            PointsSelector::PointIdsSelector(ids) => ids.validate(),
            PointsSelector::FilterSelector(filter) => filter.validate(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct FilterSelector {
    pub filter: Filter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

/// Defines write ordering guarantees for collection operations
///
/// * `weak` - write operations may be reordered, works faster, default
///
/// * `medium` - write operations go through dynamically selected leader, may be inconsistent for a short period of time in case of leader change
///
/// * `strong` - Write operations go through the permanent leader, consistent, but may be unavailable if leader is down
///
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum WriteOrdering {
    #[default]
    Weak,
    Medium,
    Strong,
}

impl SplitByShard for PointOperations {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points
                .split_by_shard(ring)
                .map(PointOperations::UpsertPoints),
            PointOperations::UpsertPointsConditional(conditional_upsert) => conditional_upsert
                .split_by_shard(ring)
                .map(PointOperations::UpsertPointsConditional),
            PointOperations::DeletePoints { ids } => split_iter_by_shard(ids, |id| *id, ring)
                .map(|ids| PointOperations::DeletePoints { ids }),
            by_filter @ PointOperations::DeletePointsByFilter(_) => {
                OperationToShard::to_all(by_filter)
            }
            PointOperations::SyncPoints(_) => {
                #[cfg(debug_assertions)]
                panic!("SyncPoints operation is intended to by applied to specific shard only");
                #[cfg(not(debug_assertions))]
                OperationToShard::by_shard(vec![])
            }
        }
    }
}

impl SplitByShard for PointInsertOperationsInternal {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        match self {
            PointInsertOperationsInternal::PointsBatch(batch) => batch
                .split_by_shard(ring)
                .map(PointInsertOperationsInternal::PointsBatch),
            PointInsertOperationsInternal::PointsList(list) => list
                .split_by_shard(ring)
                .map(PointInsertOperationsInternal::PointsList),
        }
    }
}

impl SplitByShard for ConditionalInsertOperationInternal {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        let ConditionalInsertOperationInternal {
            points_op,
            condition,
        } = self;

        let points_op = points_op.split_by_shard(ring);
        match points_op {
            OperationToShard::ByShard(by_shards) => OperationToShard::ByShard(
                by_shards
                    .into_iter()
                    .map(|(shard_id, upsert_operation)| {
                        (
                            shard_id,
                            ConditionalInsertOperationInternal {
                                points_op: upsert_operation,
                                condition: condition.clone(),
                            },
                        )
                    })
                    .collect(),
            ),
            OperationToShard::ToAll(upsert_operation) => OperationToShard::ToAll(Self {
                points_op: upsert_operation,
                condition,
            }),
        }
    }
}

impl SplitByShard for BatchPersisted {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        let batch = self;
        let mut batch_by_shard: AHashMap<ShardId, BatchPersisted> = AHashMap::new();
        let BatchPersisted {
            ids,
            vectors,
            payloads,
        } = batch;

        if let Some(payloads) = payloads {
            match vectors {
                BatchVectorStructPersisted::Single(vectors) => {
                    for (id, vector, payload) in izip!(ids, vectors, payloads) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::Single(vec![]),
                                        payloads: Some(vec![]),
                                    });
                            batch.ids.push(id);
                            match &mut batch.vectors {
                                BatchVectorStructPersisted::Single(vectors) => {
                                    vectors.push(vector.clone())
                                }
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                            batch.payloads.as_mut().unwrap().push(payload.clone());
                        }
                    }
                }
                BatchVectorStructPersisted::MultiDense(vectors) => {
                    for (id, vector, payload) in izip!(ids, vectors, payloads) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::MultiDense(vec![]),
                                        payloads: Some(vec![]),
                                    });
                            batch.ids.push(id);
                            match &mut batch.vectors {
                                BatchVectorStructPersisted::MultiDense(vectors) => {
                                    vectors.push(vector.clone())
                                }
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                            batch.payloads.as_mut().unwrap().push(payload.clone());
                        }
                    }
                }
                BatchVectorStructPersisted::Named(named_vectors) => {
                    let named_vectors_list = if !named_vectors.is_empty() {
                        transpose_map_into_named_vector(named_vectors)
                    } else {
                        vec![NamedVectors::default(); ids.len()]
                    };
                    for (id, named_vector, payload) in izip!(ids, named_vectors_list, payloads) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::Named(HashMap::new()),
                                        payloads: Some(vec![]),
                                    });
                            batch.ids.push(id);
                            for (name, vector) in named_vector.clone() {
                                let name = name.into_owned();
                                let vector: VectorInternal = vector.to_owned();
                                match &mut batch.vectors {
                                    BatchVectorStructPersisted::Named(batch_vectors) => {
                                        batch_vectors
                                            .entry(name)
                                            .or_default()
                                            .push(VectorPersisted::from(vector))
                                    }
                                    _ => unreachable!(), // TODO(sparse) propagate error
                                }
                            }
                            batch.payloads.as_mut().unwrap().push(payload.clone());
                        }
                    }
                }
            }
        } else {
            match vectors {
                BatchVectorStructPersisted::Single(vectors) => {
                    for (id, vector) in izip!(ids, vectors) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::Single(vec![]),
                                        payloads: None,
                                    });
                            batch.ids.push(id);
                            match &mut batch.vectors {
                                BatchVectorStructPersisted::Single(vectors) => {
                                    vectors.push(vector.clone())
                                }
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                        }
                    }
                }
                BatchVectorStructPersisted::MultiDense(vectors) => {
                    for (id, vector) in izip!(ids, vectors) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::MultiDense(vec![]),
                                        payloads: None,
                                    });
                            batch.ids.push(id);
                            match &mut batch.vectors {
                                BatchVectorStructPersisted::MultiDense(vectors) => {
                                    vectors.push(vector.clone())
                                }
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                        }
                    }
                }
                BatchVectorStructPersisted::Named(named_vectors) => {
                    let named_vectors_list = if !named_vectors.is_empty() {
                        transpose_map_into_named_vector(named_vectors)
                    } else {
                        vec![NamedVectors::default(); ids.len()]
                    };
                    for (id, named_vector) in izip!(ids, named_vectors_list) {
                        for shard_id in point_to_shards(&id, ring) {
                            let batch =
                                batch_by_shard
                                    .entry(shard_id)
                                    .or_insert_with(|| BatchPersisted {
                                        ids: vec![],
                                        vectors: BatchVectorStructPersisted::Named(HashMap::new()),
                                        payloads: None,
                                    });
                            batch.ids.push(id);
                            for (name, vector) in named_vector.clone() {
                                let name = name.into_owned();
                                let vector: VectorInternal = vector.to_owned();
                                match &mut batch.vectors {
                                    BatchVectorStructPersisted::Named(batch_vectors) => {
                                        batch_vectors
                                            .entry(name)
                                            .or_default()
                                            .push(VectorPersisted::from(vector))
                                    }
                                    _ => unreachable!(), // TODO(sparse) propagate error
                                }
                            }
                        }
                    }
                }
            }
        }
        OperationToShard::by_shard(batch_by_shard)
    }
}

impl SplitByShard for Vec<PointStructPersisted> {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use api::rest::{Batch, BatchVectorStruct, PointInsertOperations, PointsBatch};
    use segment::types::{ExtendedPointId, PointIdType};

    use super::*;

    #[test]
    fn split_point_operations() {
        let id1 = ExtendedPointId::from_str("4072cda9-8ac6-46fa-9367-7372bc5e4798").unwrap();
        let id2 = ExtendedPointId::from(321);
        let id3 = ExtendedPointId::from_str("fe23809b-dcc9-40ba-8255-a45dce6f10be").unwrap();
        let id4 = ExtendedPointId::from_str("a307aa98-d5b5-4b0c-aec9-f963171acb74").unwrap();
        let id5 = ExtendedPointId::from_str("aaf3bb55-dc48-418c-ba76-18badc0d7fc5").unwrap();
        let id6 = ExtendedPointId::from_str("63000b52-641b-45cf-bc15-14de3944b9dd").unwrap();
        let id7 = ExtendedPointId::from_str("aab5ef35-83ad-49ea-a629-508e308872f7").unwrap();
        let id8 = ExtendedPointId::from(0);
        let id9 = ExtendedPointId::from(100500);

        let all_ids = vec![id1, id2, id3, id4, id5, id6, id7, id8, id9];

        let points: Vec<_> = all_ids
            .iter()
            .map(|id| PointStructPersisted {
                id: *id,
                vector: VectorStructPersisted::from(vec![0.1, 0.2, 0.3]),
                payload: None,
            })
            .collect();

        let mut hash_ring = HashRingRouter::single();
        hash_ring.add(0);
        hash_ring.add(1);
        hash_ring.add(2);

        let operation_to_shard = points.split_by_shard(&hash_ring);

        match operation_to_shard {
            OperationToShard::ByShard(by_shard) => {
                for (shard_id, points) in by_shard {
                    for point in points {
                        // Important: This mapping should not change with new updates!
                        if point.id == id1 {
                            assert_eq!(shard_id, 2);
                        }
                        if point.id == id2 {
                            assert_eq!(shard_id, 1);
                        }
                        if point.id == id3 {
                            assert_eq!(shard_id, 2);
                        }
                        if point.id == id4 {
                            assert_eq!(shard_id, 2);
                        }
                        if point.id == id5 {
                            assert_eq!(shard_id, 0);
                        }
                        if point.id == id6 {
                            assert_eq!(shard_id, 0);
                        }
                        if point.id == id7 {
                            assert_eq!(shard_id, 0);
                        }
                        if point.id == id8 {
                            assert_eq!(shard_id, 2);
                        }
                        if point.id == id9 {
                            assert_eq!(shard_id, 1);
                        }
                    }
                }
            }
            OperationToShard::ToAll(_) => panic!("expected ByShard"),
        }
    }

    #[test]
    fn validate_batch() {
        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![]),
                payloads: None,
            },
            shard_key: None,
            update_filter: None,
        });
        assert!(batch.validate().is_err());

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![vec![0.1]]),
                payloads: None,
            },
            shard_key: None,
            update_filter: None,
        });
        assert!(batch.validate().is_ok());

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![vec![0.1]]),
                payloads: Some(vec![]),
            },
            shard_key: None,
            update_filter: None,
        });
        assert!(batch.validate().is_err());
    }
}
