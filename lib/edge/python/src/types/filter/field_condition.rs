use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, PyErr};
use segment::json_path::JsonPath;
use segment::types::{FieldCondition, Match};
use std::str::FromStr;
use crate::types::filter::r#match::PyMatch;

#[pyclass(name = "FieldCondition")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFieldCondition(pub FieldCondition);

#[pymethods]
impl PyFieldCondition {
    #[staticmethod]
    pub fn r#match(key: &str, r#match: PyMatch) -> Result<Self, PyErr> {
        let internal_match = Match::from(r#match);
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;
        Ok(Self(FieldCondition::new_match(key, internal_match)))
    }
}

