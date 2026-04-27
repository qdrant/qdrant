use std::collections::HashMap;

use segment::data_types::vectors::VectorStructInternal;
use segment::types::{
    Payload, PointIdType, ScoredPoint as SegmentScoredPoint,
    WithPayloadInterface, WithVector as SegmentWithVector,
};
use shard::operations::point_ops::{
    PointStructPersisted, VectorPersisted, VectorStructPersisted,
};
use shard::retrieve::record_internal::RecordInternal;
use sparse::common::sparse_vector::SparseVector as InternalSparseVector;

// ── PointId ─────────────────────────────────────────────────────────────────

/// The identifier of a point stored in a shard.
///
/// Every point has exactly one ID, which is either a 64-bit unsigned
/// integer or a UUID string. IDs must be unique within a shard.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum PointId {
    /// Numeric identifier.
    NumId { value: u64 },
    /// UUID identifier formatted as a canonical `8-4-4-4-12` hyphenated
    /// string. Must parse as a valid RFC 4122 UUID.
    Uuid { value: String },
}

impl From<PointId> for PointIdType {
    fn from(id: PointId) -> Self {
        match id {
            PointId::NumId { value } => PointIdType::NumId(value),
            PointId::Uuid { value } => {
                PointIdType::Uuid(uuid::Uuid::parse_str(&value).expect("valid UUID"))
            }
        }
    }
}

impl From<PointIdType> for PointId {
    fn from(id: PointIdType) -> Self {
        match id {
            PointIdType::NumId(value) => PointId::NumId { value },
            PointIdType::Uuid(value) => PointId::Uuid {
                value: value.to_string(),
            },
        }
    }
}

// ── SparseVector ────────────────────────────────────────────────────────────

/// A sparse vector represented as parallel arrays of indices and values.
///
/// Produced by sparse embedding models such as SPLADE or BM42. `indices` and
/// `values` must have the same length; each `indices[i]` is a feature/token
/// ID whose weight is `values[i]`. Indices are typically sorted in ascending
/// order, though Qdrant does not require it.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SparseVector {
    /// Non-zero feature indices.
    pub indices: Vec<u32>,
    /// Weights aligned 1-to-1 with `indices`.
    pub values: Vec<f32>,
}

impl From<SparseVector> for InternalSparseVector {
    fn from(v: SparseVector) -> Self {
        InternalSparseVector {
            indices: v.indices,
            values: v.values,
        }
    }
}

impl From<InternalSparseVector> for SparseVector {
    fn from(v: InternalSparseVector) -> Self {
        SparseVector {
            indices: v.indices,
            values: v.values,
        }
    }
}

// ── NamedVector ─────────────────────────────────────────────────────────────

/// A single vector value associated with a specific vector field.
///
/// The variant chosen must match the field's configuration in
/// [`EdgeConfig`](crate::config::EdgeConfig) — for example, a `Dense`
/// variant cannot be stored in a field declared as sparse.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum NamedVector {
    /// A single dense vector. Component count must match the field's
    /// `size`.
    Dense { values: Vec<f32> },
    /// A sparse vector (SPLADE-, BM42-style).
    Sparse { vector: SparseVector },
    /// A multi-vector: a variable number of dense vectors (e.g. ColBERT
    /// per-token embeddings).
    MultiDense { vectors: Vec<Vec<f32>> },
}

impl From<NamedVector> for VectorPersisted {
    fn from(v: NamedVector) -> Self {
        match v {
            NamedVector::Dense { values } => VectorPersisted::Dense(values),
            NamedVector::Sparse { vector } => {
                VectorPersisted::Sparse(InternalSparseVector::from(vector))
            }
            NamedVector::MultiDense { vectors } => VectorPersisted::MultiDense(vectors),
        }
    }
}

impl From<VectorPersisted> for NamedVector {
    fn from(v: VectorPersisted) -> Self {
        match v {
            VectorPersisted::Dense(values) => NamedVector::Dense { values },
            VectorPersisted::Sparse(vector) => NamedVector::Sparse {
                vector: SparseVector::from(vector),
            },
            VectorPersisted::MultiDense(vectors) => NamedVector::MultiDense { vectors },
        }
    }
}

// ── Vector (top-level input for point construction) ─────────────────────────

/// The vector payload attached to a [`Point`] at upsert time.
///
/// Use `Single` for shards with exactly one dense vector field, `Named` for
/// shards with multiple vector fields, and `MultiDense` for a single
/// multi-vector field.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Vector {
    /// A single dense vector, to be stored in the shard's (sole) dense
    /// vector field.
    Single { values: Vec<f32> },
    /// A variable-sized multi-vector, stored in the shard's (sole)
    /// multi-vector field.
    MultiDense { vectors: Vec<Vec<f32>> },
    /// A map of vector field names to per-field values. Use this variant
    /// when the shard has more than one vector field or mixes dense and
    /// sparse vectors.
    Named { map: HashMap<String, NamedVector> },
}

impl From<Vector> for VectorStructPersisted {
    fn from(v: Vector) -> Self {
        match v {
            Vector::Single { values } => VectorStructPersisted::Single(values),
            Vector::MultiDense { vectors } => VectorStructPersisted::MultiDense(vectors),
            Vector::Named { map } => VectorStructPersisted::Named(
                map.into_iter()
                    .map(|(k, v)| (k, VectorPersisted::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<VectorStructPersisted> for Vector {
    fn from(v: VectorStructPersisted) -> Self {
        match v {
            VectorStructPersisted::Single(values) => Vector::Single { values },
            VectorStructPersisted::MultiDense(vectors) => Vector::MultiDense { vectors },
            VectorStructPersisted::Named(map) => Vector::Named {
                map: map
                    .into_iter()
                    .map(|(k, v)| (k, NamedVector::from(v)))
                    .collect(),
            },
        }
    }
}

// ── Payload (JSON string) ───────────────────────────────────────────────────

/// Payload is represented as a JSON string across the FFI boundary.
pub fn payload_to_json(payload: &Payload) -> String {
    serde_json::to_string(&payload.0).unwrap_or_default()
}

pub fn json_to_payload(json: &str) -> std::result::Result<Payload, String> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(json).map_err(|e| e.to_string())?;
    Ok(Payload(map))
}

fn vector_struct_internal_to_json(v: &VectorStructInternal) -> String {
    match v {
        VectorStructInternal::Single(dense) => serde_json::to_string(dense).unwrap_or_default(),
        VectorStructInternal::MultiDense(multi) => {
            serde_json::to_string(multi).unwrap_or_default()
        }
        VectorStructInternal::Named(map) => serde_json::to_string(map).unwrap_or_default(),
    }
}

// ── WithPayload / WithVector ────────────────────────────────────────────────

/// Controls which payload fields are included in retrieval responses.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum WithPayload {
    /// Include all payload fields (`enable = true`) or none
    /// (`enable = false`).
    Bool { enable: bool },
    /// Include only the listed payload fields. Use JSON-path syntax to
    /// select nested keys (e.g. `"meta.author"`).
    Fields { fields: Vec<String> },
}

impl From<WithPayload> for WithPayloadInterface {
    fn from(w: WithPayload) -> Self {
        match w {
            WithPayload::Bool { enable } => WithPayloadInterface::Bool(enable),
            WithPayload::Fields { fields } => {
                WithPayloadInterface::Fields(fields.into_iter().filter_map(|f| f.parse().ok()).collect())
            }
        }
    }
}

/// Controls which vector fields are included in retrieval responses.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum WithVector {
    /// Include all vectors (`enable = true`) or none (`enable = false`).
    Bool { enable: bool },
    /// Include only the listed vector fields (by name).
    Names { names: Vec<String> },
}

impl From<WithVector> for SegmentWithVector {
    fn from(w: WithVector) -> Self {
        match w {
            WithVector::Bool { enable } => SegmentWithVector::Bool(enable),
            WithVector::Names { names } => SegmentWithVector::Selector(names),
        }
    }
}

// ── Point ───────────────────────────────────────────────────────────────────

/// A point to be upserted into a shard.
///
/// ## Example
///
/// ```swift
/// let point = Point(
///     id: .numId(value: 42),
///     vector: .single(values: [0.1, 0.2, 0.3, 0.4]),
///     payload: #"{"title":"Hello"}"#
/// )
/// ```
///
/// ```kotlin
/// val point = Point(
///     id = PointId.NumId(42u),
///     vector = Vector.Single(listOf(0.1f, 0.2f, 0.3f, 0.4f)),
///     payload = """{"title":"Hello"}""",
/// )
/// ```
#[derive(Clone, Debug, uniffi::Record)]
pub struct Point {
    /// Unique identifier within the shard.
    pub id: PointId,
    /// Vector value(s). Must match the shard's configured vector fields.
    pub vector: Vector,
    /// Optional payload, encoded as a JSON object string.
    /// `None`/`null` stores an empty payload.
    pub payload: Option<String>,
}

impl Point {
    pub fn into_internal(self) -> std::result::Result<PointStructPersisted, String> {
        let payload = match self.payload {
            Some(json) => Some(json_to_payload(&json)?),
            None => None,
        };
        Ok(PointStructPersisted {
            id: PointIdType::from(self.id),
            vector: VectorStructPersisted::from(self.vector),
            payload,
        })
    }
}

// ── ScoredPoint ─────────────────────────────────────────────────────────────

/// A single search hit returned from [`EdgeShard::search`] or
/// [`EdgeShard::query`].
///
/// [`EdgeShard::search`]: crate::EdgeShard::search
/// [`EdgeShard::query`]: crate::EdgeShard::query
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScoredPoint {
    /// The point's identifier.
    pub id: PointId,
    /// The shard-local version of the point at the time it was returned.
    pub version: u64,
    /// Similarity score against the query. Higher is better for `Cosine`
    /// and `Dot`; lower is better for `Euclid` and `Manhattan`.
    pub score: f32,
    /// Payload as a JSON object string, or `None`/`null` if the request
    /// did not ask for payload.
    pub payload: Option<String>,
    /// Vectors as a JSON string, or `None`/`null` if the request did not
    /// ask for vectors.
    pub vector: Option<String>,
}

impl From<SegmentScoredPoint> for ScoredPoint {
    fn from(p: SegmentScoredPoint) -> Self {
        ScoredPoint {
            id: PointId::from(p.id),
            version: p.version,
            score: p.score,
            payload: p.payload.map(|p| payload_to_json(&p)),
            vector: p
                .vector
                .as_ref()
                .map(|v| vector_struct_internal_to_json(v)),
        }
    }
}

// ── Record ──────────────────────────────────────────────────────────────────

/// A point returned from [`EdgeShard::retrieve`] or
/// [`EdgeShard::scroll`], without a similarity score.
///
/// [`EdgeShard::retrieve`]: crate::EdgeShard::retrieve
/// [`EdgeShard::scroll`]: crate::EdgeShard::scroll
#[derive(Clone, Debug, uniffi::Record)]
pub struct Record {
    /// The point's identifier.
    pub id: PointId,
    /// Payload as a JSON object string, or `None`/`null` if not requested.
    pub payload: Option<String>,
    /// Vectors as a JSON string, or `None`/`null` if not requested.
    pub vector: Option<String>,
}

impl From<RecordInternal> for Record {
    fn from(r: RecordInternal) -> Self {
        Record {
            id: PointId::from(r.id),
            payload: r.payload.map(|p| payload_to_json(&p)),
            vector: r
                .vector
                .as_ref()
                .map(|v| vector_struct_internal_to_json(v)),
        }
    }
}

// ── PointVectors (for update_vectors) ───────────────────────────────────────

/// Replacement vectors for an existing point, used with
/// [`UpdateOperation::update_vectors`](crate::update::UpdateOperation::update_vectors).
#[derive(Clone, Debug, uniffi::Record)]
pub struct PointVectors {
    /// The point to update. Must already exist in the shard.
    pub id: PointId,
    /// New vector value(s). Only the named fields present here are
    /// replaced; other vector fields on the point are left untouched.
    pub vector: Vector,
}
