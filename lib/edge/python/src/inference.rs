//! Python bindings for edge-side inference inputs.

use bytemuck::TransparentWrapper;
use derive_more::Into;
use edge::{Document, EdgePoint, NamedVectorInput, PointVectorsInput, bm25_embed::EdgeBm25Config};
use pyo3::prelude::*;
use segment::types::{Payload, PointIdType};
use std::collections::HashMap;

use crate::bm25::PyBm25Config;
use crate::repr::*;
use crate::{PyPayload, PyPointId, PyVector};

/// Text document for an edge inference model.
///
/// Pass instances anywhere a vector is accepted in
/// [`Point`](crate::PyPoint) — they're embedded at update time using the
/// model registered under [`Document.model`].
#[pyclass(name = "Document", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyDocument(pub Document);

#[pyclass_repr]
#[pymethods]
impl PyDocument {
    #[new]
    #[pyo3(signature = (text, model, options = None))]
    pub fn new(text: String, model: String, options: Option<PyDocumentOptions>) -> Self {
        Self(Document {
            text,
            model,
            options: options.map(|o| o.into()),
        })
    }

    #[getter]
    pub fn text(&self) -> &str {
        &self.0.text
    }

    #[getter]
    pub fn model(&self) -> &str {
        &self.0.model
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

/// Per-document model overrides. Today only BM25 overrides are accepted.
#[derive(Clone, Debug)]
pub struct PyDocumentOptions(edge::DocumentOptions);

impl From<PyDocumentOptions> for edge::DocumentOptions {
    fn from(o: PyDocumentOptions) -> Self {
        o.0
    }
}

impl FromPyObject<'_, '_> for PyDocumentOptions {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        // Today the only honored variant is Bm25 — accept a Bm25Config.
        let cfg: PyBm25Config = value.extract()?;
        let inner: EdgeBm25Config = cfg.into();
        Ok(Self(edge::DocumentOptions::Bm25(inner)))
    }
}

/// One slot in a point's named vectors. A regular vector OR an unembedded document.
#[derive(Clone, Debug)]
pub struct PyNamedVectorInput(pub NamedVectorInput);

impl FromPyObject<'_, '_> for PyNamedVectorInput {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        // Try Document first — otherwise fall back to a regular vector.
        if let Ok(doc) = value.extract::<PyDocument>() {
            return Ok(Self(NamedVectorInput::Document(doc.0)));
        }

        // Reuse the existing per-slot vector parser. PyVector wraps the
        // resolved struct form; we extract the named-slot single value via
        // the `PyNamedVector` helper used inside it.
        let helper = NamedVectorHelper::extract(value)?;
        Ok(Self(NamedVectorInput::Vector(helper.into_internal())))
    }
}

/// Helper mirroring PyNamedVector's variants but converting straight to
/// [`segment::data_types::vectors::VectorInternal`] for use in
/// [`NamedVectorInput::Vector`].
struct NamedVectorHelper(segment::data_types::vectors::VectorInternal);

impl NamedVectorHelper {
    fn into_internal(self) -> segment::data_types::vectors::VectorInternal {
        self.0
    }

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        use segment::data_types::vectors::{MultiDenseVectorInternal, VectorInternal};
        use sparse::common::sparse_vector::SparseVector;

        #[derive(FromPyObject)]
        enum Variant {
            Dense(Vec<f32>),
            Sparse(crate::types::vector::PySparseVector),
            MultiDense(Vec<Vec<f32>>),
        }

        let internal = match value.extract::<Variant>()? {
            Variant::Dense(d) => VectorInternal::Dense(d),
            Variant::Sparse(s) => VectorInternal::Sparse(SparseVector::from(s)),
            Variant::MultiDense(m) => {
                VectorInternal::MultiDense(MultiDenseVectorInternal::try_from_matrix(m).map_err(
                    |e| pyo3::exceptions::PyValueError::new_err(format!("{e}")),
                )?)
            }
        };

        Ok(Self(internal))
    }
}

/// A point that may carry unembedded [`Document`] inputs. Use with
/// [`EdgeShard.upsert`](crate::PyEdgeShard) to insert.
#[pyclass(name = "EdgePoint", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyEdgePoint(pub EdgePoint);

#[pyclass_repr]
#[pymethods]
impl PyEdgePoint {
    #[new]
    #[pyo3(signature = (id, vector, payload = None))]
    pub fn new(
        id: PyPointId,
        vector: PyEdgePointVectors,
        payload: Option<PyPayload>,
    ) -> Self {
        Self(EdgePoint {
            id: PointIdType::from(id),
            vectors: vector.into(),
            payload: payload.map(Payload::from),
        })
    }

    pub fn __repr__(&self) -> String {
        format!("EdgePoint(id={:?})", self.0.id)
    }
}

/// Vectors form accepted by [`PyEdgePoint::new`]: single dense, multi-dense,
/// or a named map possibly mixing vectors and documents.
#[derive(Clone, Debug)]
pub struct PyEdgePointVectors(pub PointVectorsInput);

impl From<PyEdgePointVectors> for PointVectorsInput {
    fn from(v: PyEdgePointVectors) -> Self {
        v.0
    }
}

impl FromPyObject<'_, '_> for PyEdgePointVectors {
    type Error = PyErr;

    fn extract(value: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Variant {
            Single(Vec<f32>),
            MultiDense(Vec<Vec<f32>>),
            Named(HashMap<String, PyNamedVectorInput>),
        }

        let inner = match value.extract::<Variant>()? {
            Variant::Single(d) => PointVectorsInput::SingleDense(d),
            Variant::MultiDense(m) => PointVectorsInput::MultiDense(m),
            Variant::Named(map) => {
                let resolved: HashMap<String, NamedVectorInput> =
                    map.into_iter().map(|(k, v)| (k, v.0)).collect();
                PointVectorsInput::Named(resolved)
            }
        };

        Ok(Self(inner))
    }
}

// Suppress dead-code warnings for unused imports introduced for clarity.
#[allow(dead_code)]
fn _force_used(_v: &PyVector) {}
