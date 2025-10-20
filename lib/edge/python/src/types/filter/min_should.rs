use derive_more::Into;
use pyo3::{pyclass, pymethods};
use segment::types::{Condition, MinShould};

use crate::types::filter::condition::PyCondition;

#[pyclass(name = "MinShould")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyMinShould(pub MinShould);

#[pymethods]
impl PyMinShould {
    #[new]
    pub fn new(conditions: Vec<PyCondition>, min_count: usize) -> Self {
        let conditions = conditions.into_iter().map(Condition::from).collect();
        Self(MinShould {
            conditions,
            min_count,
        })
    }
}
