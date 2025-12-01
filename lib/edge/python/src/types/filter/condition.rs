use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::*;
use segment::utils::maybe_arc::MaybeArc;

use crate::types::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyCondition(pub Condition);

impl FromPyObject<'_, '_> for PyCondition {
    type Error = PyErr;

    fn extract(condition: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        #[expect(clippy::large_enum_variant)]
        enum Helper {
            Field(PyFieldCondition),
            IsEmpty(PyIsEmptyCondition),
            IsNull(PyIsNullCondition),
            HasId(PyHasIdCondition),
            HasVector(PyHasVectorCondition),
            Nested(PyNestedCondition),
            Filter(PyFilter),
        }

        let condition = match condition.extract()? {
            Helper::Field(field) => Condition::Field(field.into()),
            Helper::IsEmpty(is_empty) => Condition::IsEmpty(is_empty.into()),
            Helper::IsNull(is_null) => Condition::IsNull(is_null.into()),
            Helper::HasId(has_id) => Condition::HasId(has_id.into()),
            Helper::HasVector(has_vector) => Condition::HasVector(has_vector.into()),
            Helper::Nested(nested) => Condition::Nested(nested.into()),
            Helper::Filter(filter) => Condition::Filter(filter.into()),
        };

        Ok(Self(condition))
    }
}

impl<'py> IntoPyObject<'py> for PyCondition {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Condition::Field(field) => PyFieldCondition(field).into_bound_py_any(py),
            Condition::IsEmpty(is_empty) => PyIsEmptyCondition(is_empty).into_bound_py_any(py),
            Condition::IsNull(is_null) => PyIsNullCondition(is_null).into_bound_py_any(py),
            Condition::HasId(has_id) => PyHasIdCondition(has_id).into_bound_py_any(py),
            Condition::HasVector(has_vector) => {
                PyHasVectorCondition(has_vector).into_bound_py_any(py)
            }
            Condition::Nested(nested) => PyNestedCondition(nested).into_bound_py_any(py),
            Condition::Filter(filter) => PyFilter(filter).into_bound_py_any(py),
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
            is_empty: PayloadField {
                key: JsonPath::from(key),
            },
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
            is_null: PayloadField {
                key: JsonPath::from(key),
            },
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
            has_id: MaybeArc::NoArc(ahash::AHashSet::from(PyPointId::peel_set(point_ids))),
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
