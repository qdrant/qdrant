use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;
use segment::utils::maybe_arc::MaybeArc;

use crate::types::*;

#[derive(Clone, Debug, IntoPyObject, FromPyObject)]
#[expect(clippy::large_enum_variant)]
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
    fn from(condition: PyCondition) -> Self {
        match condition {
            PyCondition::Field(field) => Condition::Field(FieldCondition::from(field)),
            PyCondition::IsEmpty(is_empty) => Condition::IsEmpty(IsEmptyCondition::from(is_empty)),
            PyCondition::IsNull(is_null) => Condition::IsNull(IsNullCondition::from(is_null)),
            PyCondition::HasId(has_id) => Condition::HasId(HasIdCondition::from(has_id)),
            PyCondition::HasVector(has_vector) => {
                Condition::HasVector(HasVectorCondition::from(has_vector))
            }
            PyCondition::Nested(nested) => Condition::Nested(NestedCondition::from(nested)),
            PyCondition::Filter(filter) => Condition::Filter(Filter::from(filter)),
        }
    }
}

impl From<Condition> for PyCondition {
    fn from(condition: Condition) -> Self {
        match condition {
            Condition::Field(field) => PyCondition::Field(PyFieldCondition(field)),
            Condition::IsEmpty(is_empty) => PyCondition::IsEmpty(PyIsEmptyCondition(is_empty)),
            Condition::IsNull(is_null) => PyCondition::IsNull(PyIsNullCondition(is_null)),
            Condition::HasId(has_id) => PyCondition::HasId(PyHasIdCondition(has_id)),
            Condition::HasVector(has_vector) => {
                PyCondition::HasVector(PyHasVectorCondition(has_vector))
            }
            Condition::Nested(nested) => PyCondition::Nested(PyNestedCondition(nested)),
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
    pub fn new(key: PyJsonPath) -> Result<Self, PyErr> {
        Ok(Self(IsEmptyCondition {
            is_empty: PayloadField { key: key.into() },
        }))
    }
}

#[pyclass(name = "IsNullCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyIsNullCondition(pub IsNullCondition);

#[pymethods]
impl PyIsNullCondition {
    #[new]
    pub fn new(key: PyJsonPath) -> Result<Self, PyErr> {
        Ok(Self(IsNullCondition {
            is_null: PayloadField { key: key.into() },
        }))
    }
}

#[pyclass(name = "HasIdCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyHasIdCondition(pub HasIdCondition);

#[pymethods]
impl PyHasIdCondition {
    #[new]
    pub fn new(point_ids: ahash::HashSet<PyPointId>) -> Result<Self, PyErr> {
        Ok(Self(HasIdCondition {
            has_id: MaybeArc::NoArc(ahash::AHashSet::from(PyPointId::into_rust_set(point_ids))),
        }))
    }
}

#[pyclass(name = "HasVectorCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyHasVectorCondition(pub HasVectorCondition);

#[pymethods]
impl PyHasVectorCondition {
    #[new]
    pub fn new(vector: VectorNameBuf) -> Self {
        Self(HasVectorCondition { has_vector: vector })
    }
}
