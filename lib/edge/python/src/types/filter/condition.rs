use std::str::FromStr;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::{FromPyObject, IntoPyObject, PyErr, pyclass, pymethods};
use segment::json_path::JsonPath;
use segment::types::{
    Condition, FieldCondition, Filter, HasIdCondition, HasVectorCondition, IsEmptyCondition,
    IsNullCondition, NestedCondition, PayloadField, PointIdType, VectorNameBuf,
};

use crate::types::filter::field_condition::PyFieldCondition;
use crate::types::nested::PyNestedCondition;
use crate::types::{PyFilter, PyPointId};

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
#[allow(clippy::large_enum_variant)]
pub enum PyCondition {
    Field(PyFieldCondition),
    IsEmpty(PyIsEmptyCondition),
    IsNull(PyIsNullCondition),
    HasId(PyHasIdCondition),
    HasVector(PyHasVectorCondition),
    Nested(PyNestedCondition),
    Filter(PyFilter),
}

impl From<PyCondition> for Condition {
    fn from(value: PyCondition) -> Self {
        match value {
            PyCondition::Field(field_condition) => {
                Condition::Field(FieldCondition::from(field_condition))
            }
            PyCondition::IsEmpty(is_empty_condition) => {
                Condition::IsEmpty(IsEmptyCondition::from(is_empty_condition))
            }
            PyCondition::IsNull(is_null_condition) => {
                Condition::IsNull(IsNullCondition::from(is_null_condition))
            }
            PyCondition::HasId(has_id_condition) => {
                Condition::HasId(HasIdCondition::from(has_id_condition))
            }
            PyCondition::HasVector(has_vector_condition) => {
                Condition::HasVector(HasVectorCondition::from(has_vector_condition))
            }
            PyCondition::Nested(nested_condition) => {
                Condition::Nested(NestedCondition::from(nested_condition))
            }
            PyCondition::Filter(filter) => Condition::Filter(Filter::from(filter)),
        }
    }
}

impl From<Condition> for PyCondition {
    fn from(value: Condition) -> Self {
        match value {
            Condition::Field(field_condition) => {
                PyCondition::Field(PyFieldCondition(field_condition))
            }
            Condition::IsEmpty(is_empty_condition) => {
                PyCondition::IsEmpty(PyIsEmptyCondition(is_empty_condition))
            }
            Condition::IsNull(is_null_condition) => {
                PyCondition::IsNull(PyIsNullCondition(is_null_condition))
            }
            Condition::HasId(has_id_condition) => {
                PyCondition::HasId(PyHasIdCondition(has_id_condition))
            }
            Condition::HasVector(has_vector_condition) => {
                PyCondition::HasVector(PyHasVectorCondition(has_vector_condition))
            }
            Condition::Nested(nested_condition) => {
                PyCondition::Nested(PyNestedCondition(nested_condition))
            }
            Condition::Filter(filter) => PyCondition::Filter(PyFilter(filter)),
            Condition::CustomIdChecker(_) => {
                unreachable!("CustomIdChecker condition is not expected in Python bindings")
            }
        }
    }
}

#[pyclass(name = "IsEmptyCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyIsEmptyCondition(pub IsEmptyCondition);

#[pymethods]
impl PyIsEmptyCondition {
    #[new]
    pub fn new(key: &str) -> Result<Self, PyErr> {
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;

        Ok(Self(IsEmptyCondition {
            is_empty: PayloadField { key },
        }))
    }
}

#[pyclass(name = "IsNullCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyIsNullCondition(pub IsNullCondition);

#[pymethods]
impl PyIsNullCondition {
    #[new]
    pub fn new(key: &str) -> Result<Self, PyErr> {
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;
        Ok(Self(IsNullCondition {
            is_null: PayloadField { key },
        }))
    }
}

#[pyclass(name = "HasIdCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyHasIdCondition(pub HasIdCondition);

#[pymethods]
impl PyHasIdCondition {
    #[new]
    pub fn new(has_id: Vec<PyPointId>) -> Result<Self, PyErr> {
        let has_id = has_id.into_iter().map(PointIdType::from).collect();
        Ok(Self(HasIdCondition { has_id }))
    }
}

#[pyclass(name = "HasVectorCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyHasVectorCondition(pub HasVectorCondition);

#[pymethods]
impl PyHasVectorCondition {
    #[new]
    pub fn new(has_vector: VectorNameBuf) -> Self {
        Self(HasVectorCondition::from(has_vector))
    }
}
