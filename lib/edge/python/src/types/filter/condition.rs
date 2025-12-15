use std::fmt;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::*;
use segment::utils::maybe_arc::MaybeArc;

use crate::repr::*;
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

impl<'py> IntoPyObject<'py> for &PyCondition {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}

impl Repr for PyCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Condition::Field(field) => PyFieldCondition::wrap_ref(field).fmt(f),
            Condition::IsEmpty(is_empty) => PyIsEmptyCondition::wrap_ref(is_empty).fmt(f),
            Condition::IsNull(is_null) => PyIsNullCondition::wrap_ref(is_null).fmt(f),
            Condition::HasId(has_id) => PyHasIdCondition::wrap_ref(has_id).fmt(f),
            Condition::HasVector(has_vector) => PyHasVectorCondition::wrap_ref(has_vector).fmt(f),
            Condition::Nested(nested) => PyNestedCondition::wrap_ref(nested).fmt(f),
            Condition::Filter(filter) => PyFilter::wrap_ref(filter).fmt(f),
            Condition::CustomIdChecker(_) => {
                unreachable!("CustomIdChecker condition is not expected in Python bindings")
            }
        }
    }
}

#[pyclass(name = "IsEmptyCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyIsEmptyCondition(pub IsEmptyCondition);

#[pyclass_repr]
#[pymethods]
impl PyIsEmptyCondition {
    #[new]
    pub fn new(key: PyJsonPath) -> Self {
        Self(IsEmptyCondition {
            is_empty: PayloadField {
                key: JsonPath::from(key),
            },
        })
    }

    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.is_empty.key)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyIsEmptyCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let IsEmptyCondition {
            is_empty: PayloadField { key: _ },
        } = self.0;
    }
}

#[pyclass(name = "IsNullCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyIsNullCondition(pub IsNullCondition);

#[pyclass_repr]
#[pymethods]
impl PyIsNullCondition {
    #[new]
    pub fn new(key: PyJsonPath) -> Self {
        Self(IsNullCondition {
            is_null: PayloadField {
                key: JsonPath::from(key),
            },
        })
    }

    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.is_null.key)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyIsNullCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let IsNullCondition {
            is_null: PayloadField { key: _ },
        } = self.0;
    }
}

#[pyclass(name = "HasIdCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyHasIdCondition(pub HasIdCondition);

#[pyclass_repr]
#[pymethods]
impl PyHasIdCondition {
    #[new]
    pub fn new(point_ids: ahash::HashSet<PyPointId>) -> Self {
        Self(HasIdCondition {
            has_id: MaybeArc::NoArc(ahash::AHashSet::from(PyPointId::peel_set(point_ids))),
        })
    }

    #[getter]
    pub fn point_ids(&self) -> &ahash::HashSet<PyPointId> {
        PyPointId::wrap_set_ref(&self.0.has_id)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyHasIdCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let HasIdCondition { has_id: _point_ids } = self.0;
    }
}

#[pyclass(name = "HasVectorCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyHasVectorCondition(pub HasVectorCondition);

#[pyclass_repr]
#[pymethods]
impl PyHasVectorCondition {
    #[new]
    pub fn new(vector: VectorNameBuf) -> Self {
        Self(HasVectorCondition { has_vector: vector })
    }

    #[getter]
    pub fn vector(&self) -> &str {
        &self.0.has_vector
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyHasVectorCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let HasVectorCondition {
            has_vector: _vector,
        } = self.0;
    }
}
