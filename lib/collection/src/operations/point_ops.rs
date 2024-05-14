use std::borrow::Cow;
use std::collections::HashMap;

use api::rest::{BatchVectorStruct, VectorStruct};
use itertools::izip;
use schemars::JsonSchema;
use segment::common::utils::transpose_map_into_named_vector;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{Vector, DEFAULT_VECTOR_NAME};
use segment::types::{Filter, Payload, PointIdType};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use validator::Validate;

use super::{point_to_shard, split_iter_by_shard, OperationToShard, SplitByShard};
use crate::operations::shard_key_selector::ShardKeySelector;
use crate::operations::types::Record;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::ShardHashRing;

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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    #[serde(alias = "vectors")]
    #[validate]
    pub vector: VectorStruct,
    /// Payload values (optional)
    pub payload: Option<Payload>,
}

/// Warn: panics if the vector is empty
impl TryFrom<Record> for PointStruct {
    type Error = String;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        let Record {
            id,
            payload,
            vector,
            shard_key: _,
        } = record;

        if vector.is_none() {
            return Err("Vector is empty".to_string());
        }

        Ok(Self {
            id,
            payload,
            vector: api::rest::VectorStruct::from(vector.unwrap()),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: BatchVectorStruct,
    pub payloads: Option<Vec<Option<Payload>>>,
}

impl Batch {
    pub fn empty() -> Self {
        Self {
            ids: vec![],
            vectors: BatchVectorStruct::Multi(HashMap::new()),
            payloads: Some(vec![]),
        }
    }

    pub fn empty_no_payload() -> Self {
        Self {
            ids: vec![],
            vectors: BatchVectorStruct::Multi(HashMap::new()),
            payloads: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointIdsList {
    pub points: Vec<PointIdType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

impl From<Vec<PointIdType>> for PointIdsList {
    fn from(points: Vec<PointIdType>) -> Self {
        Self {
            points,
            shard_key: None,
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, rename_all = "snake_case")]
pub enum PointsSelector {
    /// Select points by list of IDs
    PointIdsSelector(PointIdsList),
    /// Select points by filtering condition
    FilterSelector(FilterSelector),
}

impl Validate for PointsSelector {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            PointsSelector::PointIdsSelector(ids) => ids.validate(),
            PointsSelector::FilterSelector(filter) => filter.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct PointSyncOperation {
    /// Minimal id of the sync range
    pub from_id: Option<PointIdType>,
    /// Maximal id og
    pub to_id: Option<PointIdType>,
    pub points: Vec<PointStruct>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Validate, JsonSchema)]
pub struct PointsBatch {
    #[validate]
    pub batch: Batch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

#[derive(Debug, Deserialize, Serialize, Clone, JsonSchema, Validate)]
pub struct PointsList {
    #[validate]
    pub points: Vec<PointStruct>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_key: Option<ShardKeySelector>,
}

impl<'de> serde::Deserialize<'de> for PointInsertOperations {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Object(map) => {
                if map.contains_key("batch") {
                    PointsBatch::deserialize(serde_json::Value::Object(map))
                        .map(PointInsertOperations::PointsBatch)
                        .map_err(serde::de::Error::custom)
                } else if map.contains_key("points") {
                    PointsList::deserialize(serde_json::Value::Object(map))
                        .map(PointInsertOperations::PointsList)
                        .map_err(serde::de::Error::custom)
                } else {
                    Err(serde::de::Error::custom(
                        "Invalid PointInsertOperations format",
                    ))
                }
            }
            _ => Err(serde::de::Error::custom(
                "Invalid PointInsertOperations format",
            )),
        }
    }
}

#[derive(Debug, Serialize, Clone, JsonSchema)]
#[serde(untagged)]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    PointsBatch(PointsBatch),
    /// Insert points from a list
    PointsList(PointsList),
}

impl Validate for PointInsertOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch.validate(),
            PointInsertOperations::PointsList(list) => list.validate(),
        }
    }
}

impl PointInsertOperations {
    pub fn decompose(self) -> (Option<ShardKeySelector>, PointInsertOperationsInternal) {
        match self {
            PointInsertOperations::PointsBatch(batch) => (batch.shard_key, batch.batch.into()),
            PointInsertOperations::PointsList(list) => (list.shard_key, list.points.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum PointInsertOperationsInternal {
    /// Inset points from a batch.
    #[serde(rename = "batch")]
    PointsBatch(Batch),
    /// Insert points from a list
    #[serde(rename = "points")]
    PointsList(Vec<PointStruct>),
}

impl Validate for PointInsertOperationsInternal {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            PointInsertOperationsInternal::PointsBatch(batch) => batch.validate(),
            PointInsertOperationsInternal::PointsList(_list) => Ok(()),
        }
    }
}

impl Validate for Batch {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        let batch = self;

        let bad_input_description = |ids: usize, vecs: usize| -> String {
            format!("number of ids and vectors must be equal ({ids} != {vecs})")
        };
        let create_error = |message: String| -> validator::ValidationErrors {
            let mut errors = validator::ValidationErrors::new();
            errors.add("batch", {
                let mut error = validator::ValidationError::new("point_insert_operation");
                error.message.replace(Cow::from(message));
                error
            });
            errors
        };

        self.vectors.validate()?;
        match &batch.vectors {
            BatchVectorStruct::Single(vectors) => {
                if batch.ids.len() != vectors.len() {
                    return Err(create_error(bad_input_description(
                        batch.ids.len(),
                        vectors.len(),
                    )));
                }
            }
            BatchVectorStruct::Multi(named_vectors) => {
                for vectors in named_vectors.values() {
                    if batch.ids.len() != vectors.len() {
                        return Err(create_error(bad_input_description(
                            batch.ids.len(),
                            vectors.len(),
                        )));
                    }
                }
            }
        }
        if let Some(payload_vector) = &batch.payloads {
            if payload_vector.len() != batch.ids.len() {
                return Err(create_error(format!(
                    "number of ids and payloads must be equal ({} != {})",
                    batch.ids.len(),
                    payload_vector.len(),
                )));
            }
        }
        Ok(())
    }
}

impl SplitByShard for PointInsertOperationsInternal {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
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

impl From<Batch> for PointInsertOperations {
    fn from(batch: Batch) -> Self {
        PointInsertOperations::PointsBatch(PointsBatch {
            batch,
            shard_key: None,
        })
    }
}

impl From<Vec<PointStruct>> for PointInsertOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperations::PointsList(PointsList {
            points,
            shard_key: None,
        })
    }
}

impl From<Batch> for PointInsertOperationsInternal {
    fn from(batch: Batch) -> Self {
        PointInsertOperationsInternal::PointsBatch(batch)
    }
}

impl From<Vec<PointStruct>> for PointInsertOperationsInternal {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperationsInternal::PointsList(points)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperationsInternal),
    /// Delete point if exists
    DeletePoints { ids: Vec<PointIdType> },
    /// Delete points by given filter criteria
    DeletePointsByFilter(Filter),
    /// Points Sync
    SyncPoints(PointSyncOperation),
}

impl PointOperations {
    pub fn is_write_operation(&self) -> bool {
        match self {
            PointOperations::UpsertPoints(_) => true,
            PointOperations::DeletePoints { .. } => false,
            PointOperations::DeletePointsByFilter(_) => false,
            PointOperations::SyncPoints(_) => true,
        }
    }
}

impl Validate for PointOperations {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points.validate(),
            PointOperations::DeletePoints { ids: _ } => Ok(()),
            PointOperations::DeletePointsByFilter(_) => Ok(()),
            PointOperations::SyncPoints(_) => Ok(()),
        }
    }
}

impl SplitByShard for Batch {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
        let batch = self;
        let mut batch_by_shard: HashMap<ShardId, Batch> = HashMap::new();
        let Batch {
            ids,
            vectors,
            payloads,
        } = batch;

        if let Some(payloads) = payloads {
            match vectors {
                BatchVectorStruct::Single(vectors) => {
                    for (id, vector, payload) in izip!(ids, vectors, payloads) {
                        let shard_id = point_to_shard(id, ring);
                        let batch = batch_by_shard.entry(shard_id).or_insert_with(|| Batch {
                            ids: vec![],
                            vectors: BatchVectorStruct::Single(vec![]),
                            payloads: Some(vec![]),
                        });
                        batch.ids.push(id);
                        match &mut batch.vectors {
                            BatchVectorStruct::Single(vectors) => vectors.push(vector),
                            _ => unreachable!(), // TODO(sparse) propagate error
                        }
                        batch.payloads.as_mut().unwrap().push(payload);
                    }
                }
                BatchVectorStruct::Multi(named_vectors) => {
                    let named_vectors_list = if !named_vectors.is_empty() {
                        transpose_map_into_named_vector(named_vectors)
                    } else {
                        vec![NamedVectors::default(); ids.len()]
                    };
                    for (id, named_vector, payload) in izip!(ids, named_vectors_list, payloads) {
                        let shard_id = point_to_shard(id, ring);
                        let batch = batch_by_shard.entry(shard_id).or_insert_with(|| Batch {
                            ids: vec![],
                            vectors: BatchVectorStruct::Multi(HashMap::new()),
                            payloads: Some(vec![]),
                        });
                        batch.ids.push(id);
                        for (name, vector) in named_vector {
                            let name = name.into_owned();
                            let vector: Vector = vector.to_owned();
                            match &mut batch.vectors {
                                BatchVectorStruct::Multi(batch_vectors) => batch_vectors
                                    .entry(name)
                                    .or_default()
                                    .push(api::rest::Vector::from(vector)),
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                        }
                        batch.payloads.as_mut().unwrap().push(payload);
                    }
                }
            }
        } else {
            match vectors {
                BatchVectorStruct::Single(vectors) => {
                    for (id, vector) in izip!(ids, vectors) {
                        let shard_id = point_to_shard(id, ring);
                        let batch = batch_by_shard.entry(shard_id).or_insert_with(|| Batch {
                            ids: vec![],
                            vectors: BatchVectorStruct::Single(vec![]),
                            payloads: None,
                        });
                        batch.ids.push(id);
                        match &mut batch.vectors {
                            BatchVectorStruct::Single(vectors) => vectors.push(vector),
                            _ => unreachable!(), // TODO(sparse) propagate error
                        }
                    }
                }
                BatchVectorStruct::Multi(named_vectors) => {
                    let named_vectors_list = if !named_vectors.is_empty() {
                        transpose_map_into_named_vector(named_vectors)
                    } else {
                        vec![NamedVectors::default(); ids.len()]
                    };
                    for (id, named_vector) in izip!(ids, named_vectors_list) {
                        let shard_id = point_to_shard(id, ring);
                        let batch = batch_by_shard.entry(shard_id).or_insert_with(|| Batch {
                            ids: vec![],
                            vectors: BatchVectorStruct::Multi(HashMap::new()),
                            payloads: None,
                        });
                        batch.ids.push(id);
                        for (name, vector) in named_vector {
                            let name = name.into_owned();
                            let vector: Vector = vector.to_owned();
                            match &mut batch.vectors {
                                BatchVectorStruct::Multi(batch_vectors) => batch_vectors
                                    .entry(name)
                                    .or_default()
                                    .push(api::rest::Vector::from(vector)),
                                _ => unreachable!(), // TODO(sparse) propagate error
                            }
                        }
                    }
                }
            }
        }

        OperationToShard::by_shard(batch_by_shard)
    }
}

impl SplitByShard for Vec<PointStruct> {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

impl SplitByShard for PointOperations {
    fn split_by_shard(self, ring: &ShardHashRing) -> OperationToShard<Self> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points
                .split_by_shard(ring)
                .map(PointOperations::UpsertPoints),
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

impl From<Batch> for PointOperations {
    fn from(batch: Batch) -> Self {
        PointOperations::UpsertPoints(batch.into())
    }
}

impl From<Vec<PointStruct>> for PointOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointOperations::UpsertPoints(points.into())
    }
}

impl PointStruct {
    pub fn get_vectors(&self) -> NamedVectors {
        let mut named_vectors = NamedVectors::default();
        match &self.vector {
            VectorStruct::Single(vector) => named_vectors.insert(
                DEFAULT_VECTOR_NAME.to_string(),
                Vector::from(vector.clone()),
            ),
            VectorStruct::Multi(vectors) => {
                for (name, vector) in vectors {
                    named_vectors.insert(name.clone(), Vector::from(vector.clone()));
                }
            }
        }
        named_vectors
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::BatchVectorStruct;

    use super::*;

    #[test]
    fn validate_batch() {
        let batch: PointInsertOperationsInternal = Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: BatchVectorStruct::from(vec![]).into(),
            payloads: None,
        }
        .into();
        assert!(batch.validate().is_err());

        let batch: PointInsertOperationsInternal = Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: BatchVectorStruct::from(vec![vec![0.1]]).into(),
            payloads: None,
        }
        .into();
        assert!(batch.validate().is_ok());

        let batch: PointInsertOperationsInternal = Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: BatchVectorStruct::from(vec![vec![0.1]]).into(),
            payloads: Some(vec![]),
        }
        .into();
        assert!(batch.validate().is_err());
    }
}
