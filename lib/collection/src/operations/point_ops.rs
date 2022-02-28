use crate::{operations::types::VectorType, shard::ShardId};
use schemars::JsonSchema;
use segment::types::{Filter, PayloadInterface, PayloadKeyType, PointIdType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{
    point_to_shard, split_iter_by_shard,
    types::{CollectionError, CollectionResult},
    OperationToShard, SplitByShard, Validate,
};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    /// Point id
    pub id: PointIdType,
    /// Vector
    pub vector: VectorType,
    /// Payload values (optional)
    pub payload: Option<HashMap<PayloadKeyType, PayloadInterface>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: Vec<VectorType>,
    pub payloads: Option<Vec<Option<HashMap<PayloadKeyType, PayloadInterface>>>>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PointsBatch {
    pub batch: Batch,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PointsList {
    pub points: Vec<PointStruct>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct PointIdsList {
    pub points: Vec<PointIdType>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct FilterSelector {
    pub filter: Filter,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PointsSelector {
    /// Select points by list of IDs
    PointIdsSelector(PointIdsList),
    /// Select points by filtering condition
    FilterSelector(FilterSelector),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    PointsBatch(PointsBatch),
    /// Insert points from a list
    PointsList(PointsList),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperations),
    /// Delete point if exists
    DeletePoints { ids: Vec<PointIdType> },
    /// Delete points by given filter criteria
    DeletePointsByFilter(Filter),
}

impl Validate for PointOperations {
    fn validate(&self) -> CollectionResult<()> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points.validate(),
            PointOperations::DeletePoints { ids: _ } => Ok(()),
            PointOperations::DeletePointsByFilter(_) => Ok(()),
        }
    }
}

impl Validate for PointInsertOperations {
    fn validate(&self) -> CollectionResult<()> {
        match self {
            PointInsertOperations::PointsList(_) => Ok(()),
            PointInsertOperations::PointsBatch(PointsBatch { batch }) => {
                if batch.ids.len() != batch.vectors.len() {
                    return Err(CollectionError::BadInput {
                        description: format!(
                            "Amount of ids ({}) and vectors ({}) does not match",
                            batch.ids.len(),
                            batch.vectors.len()
                        ),
                    });
                }
                if let Some(payload_vector) = &batch.payloads {
                    if payload_vector.len() != batch.ids.len() {
                        return Err(CollectionError::BadInput {
                            description: format!(
                                "Amount of ids ({}) and payloads ({}) does not match",
                                batch.ids.len(),
                                payload_vector.len()
                            ),
                        });
                    }
                }
                Ok(())
            }
        }
    }
}

impl SplitByShard for PointsBatch {
    fn split_by_shard(self) -> OperationToShard<Self> {
        let PointsBatch { batch } = self;
        let mut batch_by_shard: HashMap<ShardId, PointsBatch> = HashMap::new();
        for i in 0..batch.ids.len() {
            let shard_id = point_to_shard(batch.ids[i]);
            let shard_batch = batch_by_shard
                .entry(shard_id)
                .or_insert_with(PointsBatch::default);
            shard_batch.batch.ids.push(batch.ids[i]);
            shard_batch.batch.vectors.push(batch.vectors[i].clone());
            if let Some(payloads) = &batch.payloads {
                shard_batch
                    .batch
                    .payloads
                    .get_or_insert(Vec::new())
                    .push(payloads[i].clone())
            }
        }
        OperationToShard::by_shard(batch_by_shard)
    }
}

impl SplitByShard for PointsList {
    fn split_by_shard(self) -> OperationToShard<Self> {
        split_iter_by_shard(self.points, |point| point.id).map(|points| PointsList { points })
    }
}

impl SplitByShard for PointOperations {
    fn split_by_shard(self) -> OperationToShard<Self> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points
                .split_by_shard()
                .map(PointOperations::UpsertPoints),
            PointOperations::DeletePoints { ids } => {
                split_iter_by_shard(ids, |id| *id).map(|ids| PointOperations::DeletePoints { ids })
            }
            by_filter @ PointOperations::DeletePointsByFilter(_) => {
                OperationToShard::to_all(by_filter)
            }
        }
    }
}

impl SplitByShard for PointInsertOperations {
    fn split_by_shard(self) -> OperationToShard<Self> {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch
                .split_by_shard()
                .map(PointInsertOperations::PointsBatch),
            PointInsertOperations::PointsList(list) => {
                list.split_by_shard().map(PointInsertOperations::PointsList)
            }
        }
    }
}

impl From<Batch> for PointInsertOperations {
    fn from(batch: Batch) -> Self {
        PointInsertOperations::PointsBatch(PointsBatch { batch })
    }
}

impl From<Vec<PointStruct>> for PointInsertOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperations::PointsList(PointsList { points })
    }
}

impl From<Batch> for PointOperations {
    fn from(batch: Batch) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsBatch(PointsBatch { batch }))
    }
}

impl From<Vec<PointStruct>> for PointOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(PointsList { points }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_batch() {
        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: vec![],
                payloads: None,
            },
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: vec![vec![0.1]],
                payloads: None,
            },
        });
        assert!(matches!(batch.validate(), Ok(())));

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: vec![vec![0.1]],
                payloads: Some(vec![]),
            },
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));
    }
}
