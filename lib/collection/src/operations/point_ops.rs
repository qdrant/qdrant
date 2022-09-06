use std::collections::HashMap;

use schemars::gen::SchemaGenerator;
use schemars::schema::{ObjectValidation, Schema, SchemaObject, SubschemaValidation};
use schemars::JsonSchema;
use segment::common::only_default_vector;
use segment::entry::entry_point::AllVectors;
use segment::types::{Filter, Payload, PointIdType};
use serde::{Deserialize, Serialize};

use super::types::{CollectionError, CollectionResult};
use super::{point_to_shard, split_iter_by_shard, OperationToShard, SplitByShard, Validate};
use crate::hash_ring::HashRing;
use crate::operations::types::{Record, VectorType};
use crate::shard::ShardId;

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub struct PointStruct {
    /// Point id
    pub id: PointIdType,
    /// Vector
    pub vector: Option<VectorType>,
    /// Vectors
    pub vectors: Option<AllVectors>,
    /// Payload values (optional)
    pub payload: Option<Payload>,
}

/// Warn: panics if the vector is empty
impl TryFrom<Record> for PointStruct {
    type Error = String;

    fn try_from(record: Record) -> Result<Self, Self::Error> {
        match record.vectors {
            Some(vectors) => Ok(Self {
                id: record.id,
                vector: None,
                vectors: Some(vectors),
                payload: record.payload,
            }),
            None => Err("Vector is empty".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Batch {
    pub ids: Vec<PointIdType>,
    pub vectors: Option<Vec<VectorType>>,
    pub all_vectors: Option<Vec<AllVectors>>,
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
pub struct PointSyncOperation {
    /// Minimal id of the sync range
    pub from_id: Option<PointIdType>,
    /// Maximal id og
    pub to_id: Option<PointIdType>,
    pub points: Vec<PointStruct>,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperations),
    /// Delete point if exists
    DeletePoints { ids: Vec<PointIdType> },
    /// Delete points by given filter criteria
    DeletePointsByFilter(Filter),
    /// Points Sync
    SyncPoints(PointSyncOperation),
}

impl Validate for PointOperations {
    fn validate(&self) -> CollectionResult<()> {
        match self {
            PointOperations::UpsertPoints(upsert_points) => upsert_points.validate(),
            PointOperations::DeletePoints { ids: _ } => Ok(()),
            PointOperations::DeletePointsByFilter(_) => Ok(()),
            PointOperations::SyncPoints(_) => Ok(()),
        }
    }
}

impl Validate for PointInsertOperations {
    fn validate(&self) -> CollectionResult<()> {
        let bad_input_error = |ids_len: usize, vectors_len: usize| {
            Err(CollectionError::BadInput {
                description: format!(
                    "Amount of ids ({}) and vectors ({}) does not match",
                    ids_len, vectors_len,
                ),
            })
        };

        match self {
            PointInsertOperations::PointsList(_) => Ok(()),
            PointInsertOperations::PointsBatch(batch) => {
                if let Some(vectors) = &batch.vectors {
                    if batch.ids.len() != vectors.len() {
                        return bad_input_error(batch.ids.len(), vectors.len());
                    }
                } else if let Some(all_vectors) = &batch.all_vectors {
                    if batch.ids.len() != all_vectors.len() {
                        return bad_input_error(batch.ids.len(), all_vectors.len());
                    }
                } else {
                    return Err(CollectionError::BadInput {
                        description: "No vector data".to_owned(),
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
        let all_vectors = batch.get_vectors();
        for i in 0..batch.ids.len() {
            let shard_id = point_to_shard(batch.ids[i], ring);
            let shard_batch = batch_by_shard
                .entry(shard_id)
                .or_insert_with(Batch::default);
            shard_batch.ids.push(batch.ids[i]);
            if shard_batch.all_vectors.is_none() {
                shard_batch.all_vectors = Some(Vec::new());
            }
            if let Some(shard_all_vectors) = &mut shard_batch.all_vectors {
                shard_all_vectors.push(all_vectors[i].clone());
            }
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
            PointOperations::SyncPoints(_) => {
                debug_assert!(
                    false,
                    "SyncPoints operation is intended to by applied to specific shard only"
                );
                OperationToShard::by_shard(vec![])
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

impl PointStruct {
    pub fn get_vectors(&self) -> AllVectors {
        if let Some(vectors) = &self.vectors {
            vectors.clone()
        } else if let Some(vector) = &self.vector {
            only_default_vector(vector)
        } else {
            panic!("PointStruct does not have vector data")
        }
    }
}

impl Batch {
    pub fn get_vectors(&self) -> Vec<AllVectors> {
        if let Some(all_vectors) = &self.all_vectors {
            all_vectors.clone()
        } else if let Some(vectors) = &self.vectors {
            vectors
                .iter()
                .map(|vector| only_default_vector(vector))
                .collect()
        } else {
            panic!("PointStruct does not have vector data")
        }
    }
}

#[cfg(test)]
mod tests {
    use segment::common::only_default_vector;

    use super::*;

    #[test]
    fn validate_batch() {
        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: Some(vec![]),
            all_vectors: None,
            payloads: None,
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));

        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: None,
            all_vectors: Some(vec![only_default_vector(&[0.1])]),
            payloads: None,
        });
        assert!(matches!(batch.validate(), Ok(())));

        let batch = PointInsertOperations::PointsBatch(Batch {
            ids: vec![PointIdType::NumId(0)],
            vectors: None,
            all_vectors: Some(vec![only_default_vector(&[0.1])]),
            payloads: Some(vec![]),
        });
        assert!(matches!(
            batch.validate(),
            Err(CollectionError::BadInput { description: _ })
        ));
    }
}
