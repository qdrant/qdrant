use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::{Filter, Nested, NestedCondition};

use crate::repr::*;
use crate::types::*;

#[pyclass(name = "NestedCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyNestedCondition(pub NestedCondition);

#[pyclass_repr]
#[pymethods]
impl PyNestedCondition {
    #[new]
    pub fn new(key: PyJsonPath, filter: PyFilter) -> Self {
        Self(NestedCondition {
            nested: Nested {
                key: JsonPath::from(key),
                filter: Filter::from(filter),
            },
        })
    }

    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.nested.key)
    }

    #[getter]
    pub fn filter(&self) -> &PyFilter {
        PyFilter::wrap_ref(&self.0.nested.filter)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyNestedCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let NestedCondition {
            nested: Nested { key: _, filter: _ },
        } = self.0;
    }
}
