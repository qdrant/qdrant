use crate::ShardId;
use crate::{hash_ring::HashRing, operations::types::VectorType};
use schemars::gen::SchemaGenerator;
use schemars::schema::{ObjectValidation, Schema, SchemaObject, SubschemaValidation};
use schemars::JsonSchema;
use segment::types::{Filter, Payload, PointIdType};
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
    pub payload: Option<Payload>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: Vec<VectorType>,
    pub payloads: Option<Vec<Option<Payload>>>,
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

// Structure used for deriving custom JsonSchema only
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct PointsList {
    points: Vec<PointStruct>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PointInsertOperations {
    /// Inset points from a batch.
    #[serde(rename = "batch")]
    PointsBatch(Batch),
    /// Insert points from a list
    #[serde(rename = "points")]
    PointsList(Vec<PointStruct>),
}

impl JsonSchema for PointInsertOperations {
    fn schema_name() -> String {
        "PointInsertOperations".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        let def_path = gen.settings().definitions_path.clone();
        let a_schema: Schema = Batch::json_schema(gen);
        let proxy_b_schema = PointsList::json_schema(gen);
        let definitions = gen.definitions_mut();
        definitions.insert(Batch::schema_name(), a_schema);

        let field_a_name = "batch".to_string();

        let get_obj_schema = |field: String, schema_name: String| {
            let schema_ref = Schema::Object(SchemaObject {
                reference: Some(format!("{}{}", def_path, schema_name)),
                ..Default::default()
            });
            Schema::Object(SchemaObject {
                object: Some(Box::new(ObjectValidation {
                    required: vec![field.clone()].into_iter().collect(),
                    properties: vec![(field, schema_ref)].into_iter().collect(),
                    ..Default::default()
                })),
                ..Default::default()
            })
        };

        let proxy_a_schema = get_obj_schema(field_a_name, Batch::schema_name());

        let proxy_a_name = "PointsBatch".to_string();

        definitions.insert(proxy_a_name.clone(), proxy_a_schema);
        definitions.insert(PointsList::schema_name(), proxy_b_schema);

        let proxy_a_ref = Schema::Object(SchemaObject {
            reference: Some(format!("{}{}", def_path, proxy_a_name)),
            ..Default::default()
        });

        let proxy_b_ref = Schema::Object(SchemaObject {
            reference: Some(format!("{}{}", def_path, PointsList::schema_name())),
            ..Default::default()
        });

        Schema::Object(SchemaObject {
            subschemas: Some(Box::new(SubschemaValidation {
                one_of: Some(vec![proxy_a_ref, proxy_b_ref]),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
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
            PointInsertOperations::PointsBatch(batch) => {
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

impl SplitByShard for Batch {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        let batch = self;
        let mut batch_by_shard: HashMap<ShardId, Batch> = HashMap::new();
        for i in 0..batch.ids.len() {
            let shard_id = point_to_shard(batch.ids[i], ring);
            let shard_batch = batch_by_shard
                .entry(shard_id)
                .or_insert_with(Batch::default);
            shard_batch.ids.push(batch.ids[i]);
            shard_batch.vectors.push(batch.vectors[i].clone());
            if let Some(payloads) = &batch.payloads {
                shard_batch
                    .payloads
                    .get_or_insert(Vec::new())
                    .push(payloads[i].clone())
            }
        }
        OperationToShard::by_shard(batch_by_shard)
    }
}

impl SplitByShard for Vec<PointStruct> {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        split_iter_by_shard(self, |point| point.id, ring)
    }
}

impl SplitByShard for PointOperations {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points
                .split_by_shard(ring)
                .map(PointOperations::UpsertPoints),
            PointOperations::DeletePoints { ids } => split_iter_by_shard(ids, |id| *id, ring)
                .map(|ids| PointOperations::DeletePoints { ids }),
            by_filter @ PointOperations::DeletePointsByFilter(_) => {
                OperationToShard::to_all(by_filter)
            }
        }
    }
}

impl SplitByShard for PointInsertOperations {
    fn split_by_shard(self, ring: &HashRing<ShardId>) -> OperationToShard<Self> {
        match self {
            PointInsertOperations::PointsBatch(batch) => batch
                .split_by_shard(ring)
                .map(PointInsertOperations::PointsBatch),
            PointInsertOperations::PointsList(list) => list
                .split_by_shard(ring)
                .map(PointInsertOperations::PointsList),
        }
    }
}

impl From<Batch> for PointInsertOperations {
    fn from(batch: Batch) -> Self {
        PointInsertOperations::PointsBatch(batch)
    }
}

impl From<Vec<PointStruct>> for PointInsertOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointInsertOperations::PointsList(points)
    }
}

impl From<Batch> for PointOperations {
    fn from(batch: Batch) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsBatch(batch))
    }
}

impl From<Vec<PointStruct>> for PointOperations {
    fn from(points: Vec<PointStruct>) -> Self {
        PointOperations::UpsertPoints(PointInsertOperations::PointsList(points))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_batch() {
        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: vec![],
            payloads: None,
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));

        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: vec![vec![0.1]],
            payloads: None,
        });
        assert!(matches!(batch.validate(), Ok(())));

        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: vec![vec![0.1]],
            payloads: Some(vec![]),
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));
    }
}
