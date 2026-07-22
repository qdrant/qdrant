use std::collections::HashMap;

use segment::data_types::vectors::{
    MultiDenseVectorInternal, VectorInternal, VectorStructInternal,
};
use segment::types::{
    Payload, PayloadSelector, PayloadSelectorExclude, PointIdType,
    ScoredPoint as SegmentScoredPoint, WithPayloadInterface, WithVector as SegmentWithVector,
};
use shard::operations::point_ops::{PointStructPersisted, VectorPersisted, VectorStructPersisted};
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

impl TryFrom<PointId> for PointIdType {
    type Error = crate::error::EdgeError;

    fn try_from(id: PointId) -> Result<Self, Self::Error> {
        match id {
            PointId::NumId { value } => Ok(PointIdType::NumId(value)),
            PointId::Uuid { value } => {
                let uuid = uuid::Uuid::parse_str(&value).map_err(|e| {
                    crate::error::EdgeError::invalid_argument(format!(
                        "invalid point UUID {value:?}: {e}"
                    ))
                })?;
                Ok(PointIdType::Uuid(uuid))
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
        let SparseVector { indices, values } = v;
        InternalSparseVector { indices, values }
    }
}

impl From<InternalSparseVector> for SparseVector {
    fn from(v: InternalSparseVector) -> Self {
        let InternalSparseVector { indices, values } = v;
        SparseVector { indices, values }
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

/// Reject non-finite (NaN / ±∞) components before a vector crosses into the
/// engine. The engine validates only dimensionality, so a poisoned component
/// would otherwise be stored and later serialized back to the host as JSON
/// `null` — a silent corruption. Mirrors the `is_finite()` guard the geo
/// conversions already apply.
fn reject_non_finite(values: &[f32], ctx: &str) -> Result<(), crate::error::EdgeError> {
    if let Some(pos) = values.iter().position(|x| !x.is_finite()) {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{ctx} contains a non-finite value (NaN or infinity) at index {pos}"
        )));
    }
    Ok(())
}

/// Validate a host-supplied multi-vector matrix with the same invariants the
/// engine assumes — non-empty, non-zero and uniform row width — plus a
/// finite-component check. The persisted form is later reconstructed via
/// `MultiDenseVectorInternal::new_unchecked`, which only `debug_assert!`s these
/// invariants: without this guard an empty matrix panics in release and a
/// ragged matrix is silently reshaped (rows flattened against `row[0].len()`),
/// storing data the host never sent.
fn validate_multi_dense(vectors: &[Vec<f32>], ctx: &str) -> Result<(), crate::error::EdgeError> {
    if vectors.is_empty() {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{ctx} multi-vector must contain at least one vector"
        )));
    }
    let dim = vectors[0].len();
    if dim == 0 {
        return Err(crate::error::EdgeError::invalid_argument(format!(
            "{ctx} multi-vector has zero dimension"
        )));
    }
    for (i, v) in vectors.iter().enumerate() {
        if v.len() != dim {
            return Err(crate::error::EdgeError::invalid_argument(format!(
                "{ctx} multi-vector has inconsistent row length: row {i} has {} \
                 elements, expected {dim}",
                v.len()
            )));
        }
        reject_non_finite(v, ctx)?;
    }
    Ok(())
}

impl TryFrom<NamedVector> for VectorPersisted {
    type Error = crate::error::EdgeError;

    fn try_from(v: NamedVector) -> Result<Self, Self::Error> {
        Ok(match v {
            NamedVector::Dense { values } => {
                reject_non_finite(&values, "dense vector")?;
                VectorPersisted::Dense(values)
            }
            NamedVector::Sparse { vector } => {
                reject_non_finite(&vector.values, "sparse vector")?;
                VectorPersisted::Sparse(InternalSparseVector::from(vector))
            }
            NamedVector::MultiDense { vectors } => {
                validate_multi_dense(&vectors, "named")?;
                VectorPersisted::MultiDense(vectors)
            }
        })
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

/// Query-side conversion: a host-supplied vector value becomes the engine's
/// query vector, with the same finite/uniform validation as the upsert path.
impl TryFrom<NamedVector> for VectorInternal {
    type Error = crate::error::EdgeError;

    fn try_from(v: NamedVector) -> Result<Self, Self::Error> {
        Ok(match v {
            NamedVector::Dense { values } => {
                reject_non_finite(&values, "dense query vector")?;
                VectorInternal::Dense(values)
            }
            NamedVector::Sparse { vector } => {
                reject_non_finite(&vector.values, "sparse query vector")?;
                VectorInternal::Sparse(InternalSparseVector::from(vector))
            }
            NamedVector::MultiDense { vectors } => {
                validate_multi_dense(&vectors, "query")?;
                let multi = MultiDenseVectorInternal::try_from_matrix(vectors).map_err(|e| {
                    crate::error::EdgeError::invalid_argument(format!(
                        "invalid query multi-vector: {e}"
                    ))
                })?;
                VectorInternal::MultiDense(multi)
            }
        })
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

impl TryFrom<Vector> for VectorStructPersisted {
    type Error = crate::error::EdgeError;

    fn try_from(v: Vector) -> Result<Self, Self::Error> {
        Ok(match v {
            Vector::Single { values } => {
                reject_non_finite(&values, "dense vector")?;
                VectorStructPersisted::Single(values)
            }
            Vector::MultiDense { vectors } => {
                validate_multi_dense(&vectors, "single-field")?;
                VectorStructPersisted::MultiDense(vectors)
            }
            Vector::Named { map } => VectorStructPersisted::Named(
                map.into_iter()
                    .map(|(k, v)| Ok((k, VectorPersisted::try_from(v)?)))
                    .collect::<Result<_, crate::error::EdgeError>>()?,
            ),
        })
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
///
/// Serialization of a `serde_json::Map` is infallible (it holds only already-
/// validated JSON), so `.expect` here can never fire — but if a future change
/// makes it fallible we want a loud panic (caught at the UniFFI boundary), not
/// a silent empty string that a host would parse as valid-but-empty JSON.
pub fn payload_to_json(payload: &Payload) -> String {
    serde_json::to_string(&payload.0).expect("payload JSON serialization is infallible")
}

pub fn json_to_payload(json: &str) -> std::result::Result<Payload, String> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(json).map_err(|e| e.to_string())?;
    Ok(Payload(map))
}

/// Serialize retrieved vectors to a JSON string for the host.
///
/// These are plain numeric containers whose `Serialize` impls do not fail
/// (non-finite floats serialize to JSON `null` rather than erroring), so
/// `.expect` cannot fire — but it fails loud instead of emitting an invalid
/// empty string should that ever change. Note: non-finite components are
/// rejected at upsert (see `reject_non_finite`), so FFI-written vectors never
/// reach here as `null`.
fn vector_struct_internal_to_json(v: &VectorStructInternal) -> String {
    match v {
        VectorStructInternal::Single(dense) => {
            serde_json::to_string(dense).expect("dense vector JSON serialization is infallible")
        }
        VectorStructInternal::MultiDense(multi) => {
            serde_json::to_string(multi).expect("multi-vector JSON serialization is infallible")
        }
        VectorStructInternal::Named(map) => {
            serde_json::to_string(map).expect("named-vector JSON serialization is infallible")
        }
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
    /// Include all payload fields except the listed ones. Use JSON-path
    /// syntax to exclude nested keys.
    Exclude { fields: Vec<String> },
}

impl TryFrom<WithPayload> for WithPayloadInterface {
    type Error = crate::error::EdgeError;

    fn try_from(w: WithPayload) -> Result<Self, Self::Error> {
        let parse_all = |fields: Vec<String>| {
            fields
                .iter()
                .map(|f| crate::error::parse_json_path(f))
                .collect::<Result<Vec<_>, _>>()
        };
        match w {
            WithPayload::Bool { enable } => Ok(WithPayloadInterface::Bool(enable)),
            WithPayload::Fields { fields } => Ok(WithPayloadInterface::Fields(parse_all(fields)?)),
            WithPayload::Exclude { fields } => Ok(WithPayloadInterface::Selector(
                PayloadSelector::Exclude(PayloadSelectorExclude::new(parse_all(fields)?)),
            )),
        }
    }
}

/// Compile-time map of the engine's payload/vector selection enums onto the
/// FFI [`WithPayload`] / [`WithVector`] surface — same contract as the maps
/// in [`crate::update`], [`crate::ops::query`], and [`crate::filter`].
///
/// Never called; it exists only for the exhaustiveness check.
#[allow(dead_code)]
fn assert_every_selection_is_mapped(payload: WithPayloadInterface, vector: SegmentWithVector) {
    match payload {
        // [`WithPayload::Bool`]
        WithPayloadInterface::Bool(_) => {}
        // [`WithPayload::Fields`]
        WithPayloadInterface::Fields(_) => {}
        WithPayloadInterface::Selector(selector) => match selector {
            // Same include semantics as [`WithPayload::Fields`], which the
            // FFI emits instead.
            PayloadSelector::Include(_) => {}
            // [`WithPayload::Exclude`]
            PayloadSelector::Exclude(_) => {}
        },
    }
    match vector {
        // [`WithVector::Bool`]
        SegmentWithVector::Bool(_) => {}
        // [`WithVector::Names`]
        SegmentWithVector::Selector(_) => {}
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
    #[uniffi(default = None)]
    pub payload: Option<String>,
}

impl Point {
    pub fn into_internal(
        self,
    ) -> std::result::Result<PointStructPersisted, crate::error::EdgeError> {
        let Self {
            id,
            vector,
            payload,
        } = self;
        let payload = match payload {
            Some(json) => Some(json_to_payload(&json).map_err(|e| {
                crate::error::EdgeError::invalid_argument(format!("invalid payload JSON: {e}"))
            })?),
            None => None,
        };
        Ok(PointStructPersisted {
            id: PointIdType::try_from(id)?,
            vector: VectorStructPersisted::try_from(vector)?,
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
        let SegmentScoredPoint {
            id,
            version,
            score,
            payload,
            vector,
            // Edge is single-shard and the FFI has no order-by-scored surface,
            // so neither field reaches the host.
            shard_key: _,
            order_value: _,
        } = p;
        ScoredPoint {
            id: PointId::from(id),
            version,
            score,
            payload: payload.map(|p| payload_to_json(&p)),
            vector: vector.as_ref().map(vector_struct_internal_to_json),
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
        let RecordInternal {
            id,
            payload,
            vector,
            // Edge is single-shard and the FFI scroll surface has no
            // order-by cursor, so neither field reaches the host.
            shard_key: _,
            order_value: _,
        } = r;
        Record {
            id: PointId::from(id),
            payload: payload.map(|p| payload_to_json(&p)),
            vector: vector.as_ref().map(vector_struct_internal_to_json),
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
