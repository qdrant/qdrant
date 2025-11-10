use derive_more::Into;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::{Filter, Nested, NestedCondition};

use crate::types::*;

#[pyclass(name = "NestedCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyNestedCondition(pub NestedCondition);

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
}
