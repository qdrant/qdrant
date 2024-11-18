use std::collections::{HashMap, HashSet};
use std::iter;

use api::rest::{
    DenseVector, MultiDenseVector, ShardKeySelector, VectorOutput, VectorStructOutput,
};
use common::validation::validate_multi_vector;
use itertools::izip;
use schemars::JsonSchema;
use segment::common::operation_error::OperationError;
use segment::common::utils::transpose_map_into_named_vector;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    BatchVectorStructInternal, MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
    DEFAULT_VECTOR_NAME,
};
use segment::types::{Filter, Payload, PointIdType};
use serde::{Deserialize, Serialize};
use strum::{EnumDiscriminants, EnumIter};
use validator::{Validate, ValidationErrors};

use super::payload_ops::SetPayloadOp;
use super::vector_ops::{PointVectorsPersisted, UpdateVectorsOp};
use super::{
    point_to_shards, split_iter_by_shard, CollectionUpdateOperations, OperationToShard,
    SplitByShard,
};
use crate::hash_ring::HashRingRouter;
use crate::operations::{payload_ops, vector_ops};
use crate::shards::shard::ShardId;

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

/// Single vector data, as it is persisted in WAL
/// Unlike [`Vector`], this struct only stores raw vectors, inferenced or resolved.
/// Unlike [`VectorInternal`], is not optimized for search
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorPersisted {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
}

impl Validate for VectorPersisted {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            VectorPersisted::Dense(_) => Ok(()),
            VectorPersisted::Sparse(v) => v.validate(),
            VectorPersisted::MultiDense(m) => validate_multi_vector(m),
        }
    }
}

impl From<VectorInternal> for VectorPersisted {
    fn from(value: VectorInternal) -> Self {
        match value {
            VectorInternal::Dense(vector) => VectorPersisted::Dense(vector),
            VectorInternal::Sparse(vector) => VectorPersisted::Sparse(vector),
            VectorInternal::MultiDense(vector) => {
                VectorPersisted::MultiDense(vector.into_multi_vectors())
            }
        }
    }
}

impl From<VectorOutput> for VectorPersisted {
    fn from(value: VectorOutput) -> Self {
        match value {
            VectorOutput::Dense(vector) => VectorPersisted::Dense(vector),
            VectorOutput::Sparse(vector) => VectorPersisted::Sparse(vector),
            VectorOutput::MultiDense(vector) => VectorPersisted::MultiDense(vector),
        }
    }
}

impl From<VectorPersisted> for VectorInternal {
    fn from(value: VectorPersisted) -> Self {
        match value {
            VectorPersisted::Dense(vector) => VectorInternal::Dense(vector),
            VectorPersisted::Sparse(vector) => VectorInternal::Sparse(vector),
            VectorPersisted::MultiDense(vector) => {
                // the REST vectors have been validated already
                // we can use an internal constructor
                VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(vector))
            }
        }
    }
}

// General idea of having an extra layer of data structures after REST and gRPC
// is to ensure that all vectors are inferenced and validated before they are persisted.
//
// This separation allows to have a single point, enforced by the type system,
// where all Documents and other inference-able objects are resolved into raw vectors.
//
// Separation between VectorStructPersisted and VectorStructInternal is only needed
// for legacy reasons, as the previous implementations wrote VectorStruct to WAL,
// so we need an ability to read it back. VectorStructPersisted reproduces the same
// structure as VectorStruct had in the previous versions.
//
//
//        gRPC              REST API           ┌───┐              WAL
//          │                  │               │ I │               ▲
//          │                  │               │ n │               │
//          │                  │               │ f │               │
//  ┌───────▼───────┐    ┌─────▼──────┐        │ e │     ┌─────────┴───────────┐
//  │ grpc::Vectors ├───►│VectorStruct├───────►│ r ├────►│VectorStructPersisted├─────┐
//  └───────────────┘    └────────────┘        │ e │     └─────────────────────┘     │
//                        Vectors              │ n │      Only Vectors               │
//                        + Documents          │ c │                                 │
//                        + Images             │ e │                                 │
//                        + Other inference    └───┘                                 │
//                        Implement JsonSchema                                       │
//                                                       ┌─────────────────────┐     │
//                                                       │                     ◄─────┘
//                                                       │   Storage           │
//                                                       │                     │
//                        REST API Response              └────────┬────────────┘
//                             ▲                                  │
//                             │                                  │
//                      ┌──────┴──────────────┐         ┌─────────▼───────────┐
//                      │ VectorStructOutput  ◄───┬─────┤VectorStructInternal │
//                      └─────────────────────┘   │     └─────────────────────┘
//                       Only Vectors             │      Only Vectors
//                       Implement JsonSchema     │      Optimized for search
//                                                │
//                                                │
//                      ┌─────────────────────┐   │
//                      │ grpc::VectorsOutput ◄───┘
//                      └───────────┬─────────┘
//                                  │
//                                  ▼
//                              gPRC Response

/// Data structure for point vectors, as it is persisted in WAL
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStructPersisted {
    Single(DenseVector),
    MultiDense(MultiDenseVector),
    Named(HashMap<String, VectorPersisted>),
}

impl VectorStructPersisted {
    /// Check if this vector struct is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorStructPersisted::Single(vector) => vector.is_empty(),
            VectorStructPersisted::MultiDense(vector) => vector.is_empty(),
            VectorStructPersisted::Named(vectors) => vectors.values().all(|v| match v {
                VectorPersisted::Dense(vector) => vector.is_empty(),
                VectorPersisted::Sparse(vector) => vector.indices.is_empty(),
                VectorPersisted::MultiDense(vector) => vector.is_empty(),
            }),
        }
    }
}

impl Validate for VectorStructPersisted {
    fn validate(&self) -> Result<(), ValidationErrors> {
        match self {
            VectorStructPersisted::Single(_) => Ok(()),
            VectorStructPersisted::MultiDense(v) => validate_multi_vector(v),
            VectorStructPersisted::Named(v) => common::validation::validate_iter(v.values()),
        }
    }
}

impl From<DenseVector> for VectorStructPersisted {
    fn from(value: DenseVector) -> Self {
        VectorStructPersisted::Single(value)
    }
}

impl From<VectorStructInternal> for VectorStructPersisted {
    fn from(value: VectorStructInternal) -> Self {
        match value {
            VectorStructInternal::Single(vector) => VectorStructPersisted::Single(vector),
            VectorStructInternal::MultiDense(vector) => {
                VectorStructPersisted::MultiDense(vector.into_multi_vectors())
            }
            VectorStructInternal::Named(vectors) => VectorStructPersisted::Named(
                vectors
                    .into_iter()
                    .map(|(k, v)| (k, VectorPersisted::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<VectorStructOutput> for VectorStructPersisted {
    fn from(value: VectorStructOutput) -> Self {
        match value {
            VectorStructOutput::Single(vector) => VectorStructPersisted::Single(vector),
            VectorStructOutput::MultiDense(vector) => VectorStructPersisted::MultiDense(vector),
            VectorStructOutput::Named(vectors) => VectorStructPersisted::Named(
                vectors
                    .into_iter()
                    .map(|(k, v)| (k, VectorPersisted::from(v)))
                    .collect(),
            ),
        }
    }
}

impl TryFrom<VectorStructPersisted> for VectorStructInternal {
    type Error = OperationError;
    fn try_from(value: VectorStructPersisted) -> Result<Self, Self::Error> {
        let vector_struct = match value {
            VectorStructPersisted::Single(vector) => VectorStructInternal::Single(vector),
            VectorStructPersisted::MultiDense(vector) => {
                VectorStructInternal::MultiDense(MultiDenseVectorInternal::try_from(vector)?)
            }
            VectorStructPersisted::Named(vectors) => VectorStructInternal::Named(
                vectors
                    .into_iter()
                    .map(|(k, v)| (k, VectorInternal::from(v)))
                    .collect(),
            ),
        };
        Ok(vector_struct)
    }
}

impl From<VectorStructPersisted> for NamedVectors<'_> {
    fn from(value: VectorStructPersisted) -> Self {
        match value {
            VectorStructPersisted::Single(vector) => {
                NamedVectors::from_pairs([(DEFAULT_VECTOR_NAME.to_string(), vector)])
            }
            VectorStructPersisted::MultiDense(vector) => {
                let mut named_vector = NamedVectors::default();
                let multivec = MultiDenseVectorInternal::new_unchecked(vector);

                named_vector.insert(
                    DEFAULT_VECTOR_NAME.to_string(),
                    segment::data_types::vectors::VectorInternal::from(multivec),
                );
                named_vector
            }
            VectorStructPersisted::Named(vectors) => {
                let mut named_vector = NamedVectors::default();
                for (name, vector) in vectors {
                    named_vector.insert(
                        name,
                        segment::data_types::vectors::VectorInternal::from(vector),
                    );
                }
                named_vector
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate)]
#[serde(rename_all = "snake_case")]
pub struct PointStructPersisted {
    /// Point id
    pub id: PointIdType,
    /// Vectors
    pub vector: VectorStructPersisted,
    /// Payload values (optional)
    pub payload: Option<Payload>,
}

impl PointStructPersisted {
    pub fn get_vectors(&self) -> NamedVectors {
        let mut named_vectors = NamedVectors::default();
        match &self.vector {
            VectorStructPersisted::Single(vector) => named_vectors.insert(
                DEFAULT_VECTOR_NAME.to_string(),
                VectorInternal::from(vector.clone()),
            ),
            VectorStructPersisted::MultiDense(vector) => named_vectors.insert(
                DEFAULT_VECTOR_NAME.to_string(),
                VectorInternal::from(MultiDenseVectorInternal::new_unchecked(vector.clone())),
            ),
            VectorStructPersisted::Named(vectors) => {
                for (name, vector) in vectors {
                    named_vectors.insert(name.clone(), VectorInternal::from(vector.clone()));
                }
            }
        }
        named_vectors
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStructPersisted {
    Single(Vec<DenseVector>),
    MultiDense(Vec<MultiDenseVector>),
    Named(HashMap<String, Vec<VectorPersisted>>),
}

impl From<BatchVectorStructPersisted> for BatchVectorStructInternal {
    fn from(value: BatchVectorStructPersisted) -> Self {
        match value {
            BatchVectorStructPersisted::Single(vector) => BatchVectorStructInternal::Single(vector),
            BatchVectorStructPersisted::MultiDense(vectors) => {
                BatchVectorStructInternal::MultiDense(
                    vectors
                        .into_iter()
                        .map(MultiDenseVectorInternal::new_unchecked)
                        .collect(),
                )
            }
            BatchVectorStructPersisted::Named(vectors) => BatchVectorStructInternal::Named(
                vectors
                    .into_iter()
                    .map(|(k, v)| (k, v.into_iter().map(VectorInternal::from).collect()))
                    .collect(),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchPersisted {
    pub ids: Vec<PointIdType>,
    pub vectors: BatchVectorStructPersisted,
    pub payloads: Option<Vec<Option<Payload>>>,
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
    fn validate(&self) -> Result<(), ValidationErrors> {
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
    pub points: Vec<PointStructPersisted>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum PointInsertOperationsInternal {
    /// Inset points from a batch.
    #[serde(rename = "batch")]
    PointsBatch(BatchPersisted),
    /// Insert points from a list
    #[serde(rename = "points")]
    PointsList(Vec<PointStructPersisted>),
}

impl PointInsertOperationsInternal {
    pub fn point_ids(&self) -> Vec<PointIdType> {
        match self {
            Self::PointsBatch(batch) => batch.ids.clone(),
            Self::PointsList(points) => points.iter().map(|point| point.id).collect(),
        }
    }

    pub fn retain_point_ids<F>(&mut self, filter: F)
    where
        F: Fn(&PointIdType) -> bool,
    {
        match self {
            Self::PointsBatch(batch) => {
                let mut retain_indices = HashSet::new();

                retain_with_index(&mut batch.ids, |index, id| {
                    if filter(id) {
                        retain_indices.insert(index);
                        true
                    } else {
                        false
                    }
                });

                match &mut batch.vectors {
                    BatchVectorStructPersisted::Single(vectors) => {
                        retain_with_index(vectors, |index, _| retain_indices.contains(&index));
                    }

                    BatchVectorStructPersisted::MultiDense(vectors) => {
                        retain_with_index(vectors, |index, _| retain_indices.contains(&index));
                    }

                    BatchVectorStructPersisted::Named(vectors) => {
                        for (_, vectors) in vectors.iter_mut() {
                            retain_with_index(vectors, |index, _| retain_indices.contains(&index));
                        }
                    }
                }

                if let Some(payload) = &mut batch.payloads {
                    retain_with_index(payload, |index, _| retain_indices.contains(&index));
                }
            }

            Self::PointsList(points) => points.retain(|point| filter(&point.id)),
        }
    }

    pub fn into_update_only(self) -> Vec<CollectionUpdateOperations> {
        let mut operations = Vec::new();

        match self {
            Self::PointsBatch(batch) => {
                let mut update_vectors = UpdateVectorsOp { points: Vec::new() };

                match batch.vectors {
                    BatchVectorStructPersisted::Single(vectors) => {
                        let ids = batch.ids.iter().copied();
                        let vectors = vectors.into_iter().map(VectorStructPersisted::Single);

                        update_vectors.points = ids
                            .zip(vectors)
                            .map(|(id, vector)| PointVectorsPersisted { id, vector })
                            .collect();
                    }

                    BatchVectorStructPersisted::MultiDense(vectors) => {
                        let ids = batch.ids.iter().copied();
                        let vectors = vectors.into_iter().map(VectorStructPersisted::MultiDense);

                        update_vectors.points = ids
                            .zip(vectors)
                            .map(|(id, vector)| PointVectorsPersisted { id, vector })
                            .collect();
                    }

                    BatchVectorStructPersisted::Named(batch_vectors) => {
                        let ids = batch.ids.iter().copied();

                        let mut batch_vectors: HashMap<_, _> = batch_vectors
                            .into_iter()
                            .map(|(name, vectors)| (name, vectors.into_iter()))
                            .collect();

                        let vectors = iter::repeat(()).filter_map(move |_| {
                            let mut point_vectors =
                                HashMap::with_capacity(batch_vectors.capacity());

                            for (vector_name, vectors) in batch_vectors.iter_mut() {
                                point_vectors.insert(vector_name.clone(), vectors.next()?);
                            }

                            Some(VectorStructPersisted::Named(point_vectors))
                        });

                        update_vectors.points = ids
                            .zip(vectors)
                            .map(|(id, vector)| PointVectorsPersisted { id, vector })
                            .collect();
                    }
                }

                let update_vectors = vector_ops::VectorOperations::UpdateVectors(update_vectors);
                let update_vectors = CollectionUpdateOperations::VectorOperation(update_vectors);

                operations.push(update_vectors);

                if let Some(payloads) = batch.payloads {
                    let ids = batch.ids.iter().copied();

                    for (id, payload) in ids.zip(payloads) {
                        if let Some(payload) = payload {
                            let set_payload = SetPayloadOp {
                                points: Some(vec![id]),
                                payload,
                                filter: None,
                                key: None,
                            };

                            let set_payload =
                                payload_ops::PayloadOps::OverwritePayload(set_payload);
                            let set_payload =
                                CollectionUpdateOperations::PayloadOperation(set_payload);

                            operations.push(set_payload);
                        }
                    }
                }
            }

            Self::PointsList(points) => {
                let mut update_vectors = UpdateVectorsOp { points: Vec::new() };

                for point in points {
                    update_vectors.points.push(PointVectorsPersisted {
                        id: point.id,
                        vector: point.vector,
                    });

                    if let Some(payload) = point.payload {
                        let set_payload = SetPayloadOp {
                            points: Some(vec![point.id]),
                            payload,
                            filter: None,
                            key: None,
                        };

                        let set_payload = payload_ops::PayloadOps::OverwritePayload(set_payload);
                        let set_payload = CollectionUpdateOperations::PayloadOperation(set_payload);

                        operations.push(set_payload);
                    }
                }

                let update_vectors = vector_ops::VectorOperations::UpdateVectors(update_vectors);
                let update_vectors = CollectionUpdateOperations::VectorOperation(update_vectors);

                operations.insert(0, update_vectors);
            }
        }

        operations
    }
}

fn retain_with_index<T, F>(vec: &mut Vec<T>, mut filter: F)
where
    F: FnMut(usize, &T) -> bool,
{
    let mut index = 0;

    vec.retain(|item| {
        let retain = filter(index, item);
        index += 1;
        retain
    });
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

impl From<BatchPersisted> for PointInsertOperationsInternal {
    fn from(batch: BatchPersisted) -> Self {
        PointInsertOperationsInternal::PointsBatch(batch)
    }
}

impl From<Vec<PointStructPersisted>> for PointInsertOperationsInternal {
    fn from(points: Vec<PointStructPersisted>) -> Self {
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

    pub fn point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::UpsertPoints(op) => Some(op.point_ids()),
            Self::DeletePoints { ids } => Some(ids.clone()),
            Self::DeletePointsByFilter(_) => None,
            Self::SyncPoints(op) => Some(op.points.iter().map(|point| point.id).collect()),
        }
    }

    pub fn retain_point_ids<F>(&mut self, filter: F)
    where
        F: Fn(&PointIdType) -> bool,
    {
        match self {
            Self::UpsertPoints(op) => op.retain_point_ids(filter),
            Self::DeletePoints { ids } => ids.retain(filter),
            Self::DeletePointsByFilter(_) => (),
            Self::SyncPoints(op) => op.points.retain(|point| filter(&point.id)),
        }
    }
}

impl SplitByShard for BatchPersisted {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
        let batch = self;
        let mut batch_by_shard: HashMap<ShardId, BatchPersisted> = HashMap::new();
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

impl SplitByShard for PointOperations {
    fn split_by_shard(self, ring: &HashRingRouter) -> OperationToShard<Self> {
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

#[cfg(test)]
mod tests {
    use api::rest::{Batch, BatchVectorStruct, PointInsertOperations, PointsBatch};

    use super::*;

    #[test]
    fn validate_batch() {
        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![]),
                payloads: None,
            },
            shard_key: None,
        });
        assert!(batch.validate().is_err());

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![vec![0.1]]),
                payloads: None,
            },
            shard_key: None,
        });
        assert!(batch.validate().is_ok());

        let batch = PointInsertOperations::PointsBatch(PointsBatch {
            batch: Batch {
                ids: vec![PointIdType::NumId(0)],
                vectors: BatchVectorStruct::Single(vec![vec![0.1]]),
                payloads: Some(vec![]),
            },
            shard_key: None,
        });
        assert!(batch.validate().is_err());
    }
}
