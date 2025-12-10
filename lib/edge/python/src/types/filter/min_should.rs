use std::fmt;

use bytemuck::{TransparentWrapper as _, TransparentWrapperAlloc as _};
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::MinShould;

use crate::repr::*;
use crate::types::filter::condition::PyCondition;

#[pyclass(name = "MinShould")]
#[derive(Clone, Debug, Into)]
pub struct PyMinShould(pub MinShould);

#[pymethods]
impl PyMinShould {
    #[new]
    pub fn new(conditions: Vec<PyCondition>, min_count: usize) -> Self {
        Self(MinShould {
            conditions: PyCondition::peel_vec(conditions),
            min_count,
        })
    }

    #[getter]
    pub fn conditions(&self) -> &[PyCondition] {
        PyCondition::wrap_slice(&self.0.conditions)
    }

    #[getter]
    pub fn min_count(&self) -> usize {
        self.0.min_count
    }

    fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyMinShould {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.class::<Self>(&[
            ("conditions", &self.conditions()),
            ("min_count", &self.min_count()),
        ])
    }
}
