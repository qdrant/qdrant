use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::ScoredPoint;

use crate::repr::*;
use crate::{PyPayload, PyPointId, PyVectorInternal};

#[pyclass(name = "ScoredPoint")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyScoredPoint(pub ScoredPoint);

#[pymethods]
impl PyScoredPoint {
    #[getter]
    pub fn id(&self) -> PyPointId {
        PyPointId(self.0.id)
    }

    #[getter]
    pub fn version(&self) -> u64 {
        self.0.version
    }

    #[getter]
    pub fn score(&self) -> f32 {
        self.0.score
    }

    #[getter]
    pub fn vector(&self) -> Option<&PyVectorInternal> {
        self.0.vector.as_ref().map(PyVectorInternal::wrap_ref)
    }

    #[getter]
    pub fn payload(&self) -> Option<&PyPayload> {
        self.0.payload.as_ref().map(PyPayload::wrap_ref)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyScoredPoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.class::<Self>(&[
            ("id", &self.id()),
            ("version", &self.version()),
            ("score", &self.score()),
            ("vector", &self.vector()),
            ("payload", &self.payload()),
        ])
    }
}
