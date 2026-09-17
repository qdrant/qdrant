use std::fmt;
use std::num::NonZeroU32;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::*;
use segment::utils::maybe_arc::MaybeArc;

use crate::repr::*;
use crate::type_hint::Alias;
use crate::types::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyCondition(pub Condition);

pub const CONDITION: Alias = Alias {
    name: "ConditionType",
    definition: ConditionHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
#[expect(clippy::large_enum_variant)]
enum ConditionHelper {
    Field(PyFieldCondition),
    IsEmpty(PyIsEmptyCondition),
    IsNull(PyIsNullCondition),
    HasId(PyHasIdCondition),
    HasVector(PyHasVectorCondition),
    Slice(PySliceCondition),
    Nested(PyNestedCondition),
    Filter(PyFilter),
}

impl FromPyObject<'_, '_> for PyCondition {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = CONDITION.hint();

    fn extract(condition: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let condition = match condition.extract()? {
            ConditionHelper::Field(field) => Condition::Field(field.into()),
            ConditionHelper::IsEmpty(is_empty) => Condition::IsEmpty(is_empty.into()),
            ConditionHelper::IsNull(is_null) => Condition::IsNull(is_null.into()),
            ConditionHelper::HasId(has_id) => Condition::HasId(has_id.into()),
            ConditionHelper::HasVector(has_vector) => Condition::HasVector(has_vector.into()),
            ConditionHelper::Slice(slice) => Condition::Slice(slice.into()),
            ConditionHelper::Nested(nested) => Condition::Nested(nested.into()),
            ConditionHelper::Filter(filter) => Condition::Filter(filter.into()),
        };

        Ok(Self(condition))
    }
}

impl<'py> IntoPyObject<'py> for PyCondition {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = CONDITION.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            Condition::Field(field) => ConditionHelper::Field(PyFieldCondition(field)),
            Condition::IsEmpty(is_empty) => ConditionHelper::IsEmpty(PyIsEmptyCondition(is_empty)),
            Condition::IsNull(is_null) => ConditionHelper::IsNull(PyIsNullCondition(is_null)),
            Condition::HasId(has_id) => ConditionHelper::HasId(PyHasIdCondition(has_id)),
            Condition::HasVector(has_vector) => {
                ConditionHelper::HasVector(PyHasVectorCondition(has_vector))
            }
            Condition::Slice(slice) => ConditionHelper::Slice(PySliceCondition(slice)),
            Condition::Nested(nested) => ConditionHelper::Nested(PyNestedCondition(nested)),
            Condition::Filter(filter) => ConditionHelper::Filter(PyFilter(filter)),
            Condition::CustomIdChecker(_) => {
                unreachable!("CustomIdChecker condition is not expected in Python bindings")
            }
        }
        .into_bound_py_any(py)
    }
}

impl<'py> IntoPyObject<'py> for &PyCondition {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = CONDITION.hint();

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
            Condition::Slice(slice) => PySliceCondition::wrap_ref(slice).fmt(f),
            Condition::Nested(nested) => PyNestedCondition::wrap_ref(nested).fmt(f),
            Condition::Filter(filter) => PyFilter::wrap_ref(filter).fmt(f),
            Condition::CustomIdChecker(_) => {
                unreachable!("CustomIdChecker condition is not expected in Python bindings")
            }
        }
    }
}

#[pyclass(name = "IsEmptyCondition", from_py_object)]
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

#[pyclass(name = "IsNullCondition", from_py_object)]
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

#[pyclass(name = "HasIdCondition", from_py_object)]
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

#[pyclass(name = "HasVectorCondition", from_py_object)]
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

#[pyclass(name = "SliceCondition", from_py_object)]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PySliceCondition(pub SliceCondition);

#[pyclass_repr]
#[pymethods]
impl PySliceCondition {
    #[new]
    pub fn new(total: u32, index: u32) -> PyResult<Self> {
        let Some(total) = NonZeroU32::new(total) else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "total must be greater than 0",
            ));
        };
        if index >= total.get() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "index must be less than total",
            ));
        }
        Ok(Self(SliceCondition {
            slice: Slice { total, index },
        }))
    }

    #[getter]
    pub fn total(&self) -> u32 {
        self.0.slice.total.get()
    }

    #[getter]
    pub fn index(&self) -> u32 {
        self.0.slice.index
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PySliceCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let Slice {
            total: _total,
            index: _index,
        } = self.0.slice;
    }
}
