use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::{iter, mem};

use api::conversions::json::payload_to_proto;
use api::rest::{
    DenseVector, MultiDenseVector, ShardKeySelector, VectorOutput, VectorStructOutput,
};
use common::validation::validate_multi_vector;
use itertools::Itertools as _;
use ordered_float::OrderedFloat;
use schemars::JsonSchema;
use segment::common::operation_error::OperationError;
use segment::common::utils::unordered_hash_unique;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::vectors::{
    BatchVectorStructInternal, DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, VectorInternal,
    VectorStructInternal,
};
use segment::types::{Filter, Payload, PointIdType, VectorNameBuf};
use serde::{Deserialize, Serialize};
use sparse::common::types::{DimId, DimWeight};
use strum::{EnumDiscriminants, EnumIter};
use tonic::Status;
use validator::{Validate, ValidationErrors};

use super::payload_ops::*;
use super::vector_ops::*;
use super::*;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema, Validate, Hash)]
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
#[strum_discriminants(derive(EnumIter))]
#[serde(rename_all = "snake_case")]
pub enum PointOperations {
    /// Insert or update points
    UpsertPoints(PointInsertOperationsInternal),
    /// Insert points, or update existing points if condition matches
    UpsertPointsConditional(ConditionalInsertOperationInternal),
    /// Delete point if exists
    DeletePoints { ids: Vec<PointIdType> },
    /// Delete points by given filter criteria
    DeletePointsByFilter(Filter),
    /// Points Sync
    SyncPoints(PointSyncOperation),
}

impl PointOperations {
    pub fn point_ids(&self) -> Option<Vec<PointIdType>> {
        match self {
            Self::UpsertPoints(op) => Some(op.point_ids()),
            Self::UpsertPointsConditional(op) => Some(op.points_op.point_ids()),
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
            Self::UpsertPointsConditional(op) => {
                op.points_op.retain_point_ids(filter);
            }
            Self::DeletePoints { ids } => ids.retain(filter),
            Self::DeletePointsByFilter(_) => (),
            Self::SyncPoints(op) => op.points.retain(|point| filter(&point.id)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, EnumDiscriminants, Hash)]
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

    pub fn into_point_vec(self) -> Vec<PointStructPersisted> {
        match self {
            PointInsertOperationsInternal::PointsBatch(batch) => {
                let batch_vectors = BatchVectorStructInternal::from(batch.vectors);
                let all_vectors = batch_vectors.into_all_vectors(batch.ids.len());
                let vectors_iter = batch.ids.into_iter().zip(all_vectors);
                match batch.payloads {
                    None => vectors_iter
                        .map(|(id, vectors)| PointStructPersisted {
                            id,
                            vector: VectorStructInternal::from(vectors).into(),
                            payload: None,
                        })
                        .collect(),
                    Some(payloads) => vectors_iter
                        .zip(payloads)
                        .map(|((id, vectors), payload)| PointStructPersisted {
                            id,
                            vector: VectorStructInternal::from(vectors).into(),
                            payload,
                        })
                        .collect(),
                }
            }
            PointInsertOperationsInternal::PointsList(points) => points,
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

    pub fn into_update_only(
        self,
        update_filter: Option<Filter>,
    ) -> Vec<CollectionUpdateOperations> {
        let mut operations = Vec::new();

        match self {
            Self::PointsBatch(batch) => {
                let mut update_vectors = UpdateVectorsOp {
                    points: Vec::new(),
                    update_filter: update_filter.clone(),
                };

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
                            let set_payload = if let Some(update_filter) = update_filter.clone() {
                                SetPayloadOp {
                                    points: None,
                                    payload,
                                    filter: Some(update_filter.with_point_ids(vec![id])),
                                    key: None,
                                }
                            } else {
                                SetPayloadOp {
                                    points: Some(vec![id]),
                                    payload,
                                    filter: None,
                                    key: None,
                                }
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
                let mut update_vectors = UpdateVectorsOp {
                    points: Vec::new(),
                    update_filter: update_filter.clone(),
                };

                for point in points {
                    update_vectors.points.push(PointVectorsPersisted {
                        id: point.id,
                        vector: point.vector,
                    });

                    if let Some(payload) = point.payload {
                        let set_payload = if let Some(update_filter) = update_filter.clone() {
                            SetPayloadOp {
                                points: None,
                                payload,
                                filter: Some(update_filter.with_point_ids(vec![point.id])),
                                key: None,
                            }
                        } else {
                            SetPayloadOp {
                                points: Some(vec![point.id]),
                                payload,
                                filter: None,
                                key: None,
                            }
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct ConditionalInsertOperationInternal {
    pub points_op: PointInsertOperationsInternal,
    /// Condition to check, if the point already exists
    pub condition: Filter,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
pub struct PointSyncOperation {
    /// Minimal id of the sync range
    pub from_id: Option<PointIdType>,
    /// Maximal id og
    pub to_id: Option<PointIdType>,
    pub points: Vec<PointStructPersisted>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Hash)]
#[serde(rename_all = "snake_case")]
pub struct BatchPersisted {
    pub ids: Vec<PointIdType>,
    pub vectors: BatchVectorStructPersisted,
    pub payloads: Option<Vec<Option<Payload>>>,
}

impl TryFrom<BatchPersisted> for Vec<api::grpc::qdrant::PointStruct> {
    type Error = Status;

    fn try_from(batch: BatchPersisted) -> Result<Self, Self::Error> {
        let BatchPersisted {
            ids,
            vectors,
            payloads,
        } = batch;
        let mut points = Vec::with_capacity(ids.len());
        let batch_vectors = BatchVectorStructInternal::from(vectors);
        let all_vectors = batch_vectors.into_all_vectors(ids.len());
        for (i, p_id) in ids.into_iter().enumerate() {
            let id = Some(p_id.into());
            let vector = all_vectors.get(i).cloned();
            let payload = payloads.as_ref().and_then(|payloads| {
                payloads.get(i).map(|payload| match payload {
                    None => HashMap::new(),
                    Some(payload) => payload_to_proto(payload.clone()),
                })
            });
            let vectors: Option<VectorStructInternal> = vector.map(|v| v.into());

            let point = api::grpc::qdrant::PointStruct {
                id,
                vectors: vectors.map(api::grpc::qdrant::Vectors::from),
                payload: payload.unwrap_or_default(),
            };
            points.push(point);
        }

        Ok(points)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum BatchVectorStructPersisted {
    Single(Vec<DenseVector>),
    MultiDense(Vec<MultiDenseVector>),
    Named(HashMap<VectorNameBuf, Vec<VectorPersisted>>),
}

impl Hash for BatchVectorStructPersisted {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            BatchVectorStructPersisted::Single(dense) => {
                for vector in dense {
                    for v in vector {
                        OrderedFloat(*v).hash(state);
                    }
                }
            }
            BatchVectorStructPersisted::MultiDense(multidense) => {
                for vector in multidense {
                    for v in vector {
                        for element in v {
                            OrderedFloat(*element).hash(state);
                        }
                    }
                }
            }
            BatchVectorStructPersisted::Named(named) => unordered_hash_unique(state, named.iter()),
        }
    }
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, Validate, Hash)]
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
    pub fn get_vectors(&self) -> NamedVectors<'_> {
        let mut named_vectors = NamedVectors::default();
        match &self.vector {
            VectorStructPersisted::Single(vector) => named_vectors.insert(
                DEFAULT_VECTOR_NAME.to_owned(),
                VectorInternal::from(vector.clone()),
            ),
            VectorStructPersisted::MultiDense(vector) => named_vectors.insert(
                DEFAULT_VECTOR_NAME.to_owned(),
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

impl TryFrom<api::rest::schema::Record> for PointStructPersisted {
    type Error = String;

    fn try_from(record: api::rest::schema::Record) -> Result<Self, Self::Error> {
        let api::rest::schema::Record {
            id,
            payload,
            vector,
            shard_key: _,
            order_value: _,
        } = record;

        if vector.is_none() {
            return Err("Vector is empty".to_string());
        }

        Ok(Self {
            id,
            payload,
            vector: VectorStructPersisted::from(vector.unwrap()),
        })
    }
}

impl TryFrom<PointStructPersisted> for api::grpc::qdrant::PointStruct {
    type Error = Status;

    fn try_from(value: PointStructPersisted) -> Result<Self, Self::Error> {
        let PointStructPersisted {
            id,
            vector,
            payload,
        } = value;

        let vectors_internal = VectorStructInternal::try_from(vector)
            .map_err(|e| Status::invalid_argument(format!("Failed to convert vectors: {e}")))?;

        let vectors = api::grpc::qdrant::Vectors::from(vectors_internal);
        let converted_payload = match payload {
            None => HashMap::new(),
            Some(payload) => payload_to_proto(payload),
        };

        Ok(Self {
            id: Some(id.into()),
            vectors: Some(vectors),
            payload: converted_payload,
        })
    }
}

/// Data structure for point vectors, as it is persisted in WAL
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorStructPersisted {
    Single(DenseVector),
    MultiDense(MultiDenseVector),
    Named(HashMap<VectorNameBuf, VectorPersisted>),
}

impl std::hash::Hash for VectorStructPersisted {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            VectorStructPersisted::Single(vec) => {
                for v in vec {
                    OrderedFloat(*v).hash(state);
                }
            }
            VectorStructPersisted::MultiDense(multi_vec) => {
                for vec in multi_vec {
                    for v in vec {
                        OrderedFloat(*v).hash(state);
                    }
                }
            }
            VectorStructPersisted::Named(map) => {
                unordered_hash_unique(state, map.iter());
            }
        }
    }
}

impl Debug for VectorStructPersisted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorStructPersisted::Single(vector) => {
                let first_elements = vector.iter().take(4).join(", ");
                write!(f, "Single([{}, ... x {}])", first_elements, vector.len())
            }
            VectorStructPersisted::MultiDense(vector) => {
                let first_vectors = vector
                    .iter()
                    .take(4)
                    .map(|v| {
                        let first_elements = v.iter().take(4).join(", ");
                        format!("[{}, ... x {}]", first_elements, v.len())
                    })
                    .join(", ");
                write!(f, "MultiDense([{}, ... x {})", first_vectors, vector.len())
            }
            VectorStructPersisted::Named(vectors) => write!(f, "Named(( ")
                .and_then(|_| {
                    for (name, vector) in vectors {
                        write!(f, "{name}: {vector:?}, ")?;
                    }
                    Ok(())
                })
                .and_then(|_| write!(f, "))")),
        }
    }
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
                NamedVectors::from_pairs([(DEFAULT_VECTOR_NAME.to_owned(), vector)])
            }
            VectorStructPersisted::MultiDense(vector) => {
                let mut named_vector = NamedVectors::default();
                let multivec = MultiDenseVectorInternal::new_unchecked(vector);

                named_vector.insert(
                    DEFAULT_VECTOR_NAME.to_owned(),
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

/// Single vector data, as it is persisted in WAL
/// Unlike [`api::rest::Vector`], this struct only stores raw vectors, inferenced or resolved.
/// Unlike [`VectorInternal`], is not optimized for search
#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(untagged, rename_all = "snake_case")]
pub enum VectorPersisted {
    Dense(DenseVector),
    Sparse(sparse::common::sparse_vector::SparseVector),
    MultiDense(MultiDenseVector),
}

impl Hash for VectorPersisted {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        match self {
            VectorPersisted::Dense(vec) => {
                for v in vec {
                    OrderedFloat(*v).hash(state);
                }
            }
            VectorPersisted::Sparse(sparse) => {
                sparse.hash(state);
            }
            VectorPersisted::MultiDense(multi_vec) => {
                for vec in multi_vec {
                    for v in vec {
                        OrderedFloat(*v).hash(state);
                    }
                }
            }
        }
    }
}

impl VectorPersisted {
    pub fn new_sparse(indices: Vec<DimId>, values: Vec<DimWeight>) -> Self {
        Self::Sparse(sparse::common::sparse_vector::SparseVector { indices, values })
    }

    pub fn empty_sparse() -> Self {
        Self::new_sparse(vec![], vec![])
    }
}

impl Debug for VectorPersisted {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            VectorPersisted::Dense(vector) => {
                let first_elements = vector.iter().take(4).join(", ");
                write!(f, "Dense([{}, ... x {}])", first_elements, vector.len())
            }
            VectorPersisted::Sparse(vector) => {
                let first_elements = vector
                    .indices
                    .iter()
                    .zip(vector.values.iter())
                    .take(4)
                    .map(|(k, v)| format!("{k}->{v}"))
                    .join(", ");
                write!(
                    f,
                    "Sparse([{}, ... x {})",
                    first_elements,
                    vector.indices.len()
                )
            }
            VectorPersisted::MultiDense(vector) => {
                let first_vectors = vector
                    .iter()
                    .take(4)
                    .map(|v| {
                        let first_elements = v.iter().take(4).join(", ");
                        format!("[{}, ... x {}]", first_elements, v.len())
                    })
                    .join(", ");
                write!(f, "MultiDense([{}, ... x {})", first_vectors, vector.len())
            }
        }
    }
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
