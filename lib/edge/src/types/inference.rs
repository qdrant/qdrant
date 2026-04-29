//! Inference inputs at the edge API boundary.
//!
//! [`Document`] is what the user passes when they want the edge layer to
//! produce a vector from raw text using a registered model. The shape mirrors
//! `lib/api`'s REST schema so the API stays consistent across server and edge,
//! but `api` is intentionally not a dependency here — edge owns its own copy.
//!
//! Resolution (`Document` → `Vector`) happens at the edge boundary via the
//! inference registry; segment- and shard-level types never see a `Document`.

use std::collections::HashMap;

use segment::data_types::vectors::VectorInternal;
use segment::types::{ExtendedPointId, Payload};
use serde::{Deserialize, Serialize};

use crate::bm25_embed::EdgeBm25Config;
use crate::types::Vector;

/// Text document to be embedded by a registered model.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Document {
    /// Raw text to embed.
    pub text: String,

    /// Name of the model to use. Must match a model registered in
    /// [`EdgeConfig::inference_models`](crate::EdgeConfig).
    pub model: String,

    /// Optional model-specific overrides applied on top of the registered
    /// model's defaults. Currently only [`DocumentOptions::Bm25`] is honored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<DocumentOptions>,
}

impl Document {
    pub fn new(text: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model: model.into(),
            options: None,
        }
    }

    pub fn with_options(mut self, options: DocumentOptions) -> Self {
        self.options = Some(options);
        self
    }
}

/// Per-call overrides for a model. Variants are model-specific.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DocumentOptions {
    /// Free-form options bag, passed through to the resolver.
    Common(HashMap<String, serde_json::Value>),
    /// BM25-specific overrides.
    Bm25(EdgeBm25Config),
}

/// One slot in a point's named vectors map. Either an already-resolved
/// vector or a [`Document`] that the inference layer will embed.
#[derive(Clone, Debug, PartialEq)]
pub enum NamedVectorInput {
    Vector(VectorInternal),
    Document(Document),
}

impl From<Vector> for NamedVectorInput {
    fn from(v: Vector) -> Self {
        Self::Vector(v.0)
    }
}

impl From<Document> for NamedVectorInput {
    fn from(d: Document) -> Self {
        Self::Document(d)
    }
}

impl<S: Into<String>, V: Into<NamedVectorInput>> From<Vec<(S, V)>> for PointVectorsInput {
    fn from(pairs: Vec<(S, V)>) -> Self {
        PointVectorsInput::Named(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

/// Vectors attached to a point. Either a single resolved vector (dense or
/// multi-dense) or a named map that may include unresolved documents.
#[derive(Clone, Debug, PartialEq)]
pub enum PointVectorsInput {
    /// Single dense vector — no inference needed.
    SingleDense(Vec<f32>),
    /// Single multi-dense vector — no inference needed.
    MultiDense(Vec<Vec<f32>>),
    /// Named slots, possibly mixing resolved vectors and documents.
    Named(HashMap<String, NamedVectorInput>),
}

impl From<Vec<f32>> for PointVectorsInput {
    fn from(v: Vec<f32>) -> Self {
        Self::SingleDense(v)
    }
}

impl From<Vec<Vec<f32>>> for PointVectorsInput {
    fn from(v: Vec<Vec<f32>>) -> Self {
        Self::MultiDense(v)
    }
}

impl From<HashMap<String, NamedVectorInput>> for PointVectorsInput {
    fn from(m: HashMap<String, NamedVectorInput>) -> Self {
        Self::Named(m)
    }
}

/// Pending point — may carry [`Document`] inputs that are resolved at
/// shard update time. Use [`EdgeShard::upsert`](crate::EdgeShard::upsert)
/// to insert.
#[derive(Clone, Debug)]
pub struct EdgePoint {
    pub id: ExtendedPointId,
    pub vectors: PointVectorsInput,
    pub payload: Option<Payload>,
}

impl EdgePoint {
    pub fn new(
        id: impl Into<ExtendedPointId>,
        vectors: impl Into<PointVectorsInput>,
        payload: Option<Payload>,
    ) -> Self {
        Self {
            id: id.into(),
            vectors: vectors.into(),
            payload,
        }
    }
}
