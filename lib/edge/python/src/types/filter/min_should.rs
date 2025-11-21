use bytemuck::TransparentWrapperAlloc as _;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::MinShould;

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
}
