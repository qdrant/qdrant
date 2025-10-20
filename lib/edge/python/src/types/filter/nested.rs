use std::str::FromStr;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyclass, pymethods};
use segment::json_path::JsonPath;
use segment::types::{Filter, Nested, NestedCondition};

use crate::types::PyFilter;

#[pyclass(name = "NestedCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyNestedCondition(pub NestedCondition);

#[pymethods]
impl PyNestedCondition {
    #[new]
    pub fn new(key: &str, filter: PyFilter) -> Result<Self, PyErr> {
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;

        let filter = Filter::from(filter);

        Ok(Self(NestedCondition {
            nested: Nested { key, filter },
        }))
    }
}
