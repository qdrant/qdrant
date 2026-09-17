use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::types::WithVector;

use crate::repr::*;
use crate::type_hint::Alias;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyWithVector(pub WithVector);

pub const WITH_VECTOR: Alias = Alias {
    name: "WithVectorType",
    definition: WithVectorHelper::INPUT_TYPE,
};

#[derive(FromPyObject)]
enum WithVectorHelper {
    Bool(bool),
    Selector(Vec<String>),
}

impl FromPyObject<'_, '_> for PyWithVector {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = WITH_VECTOR.hint();

    fn extract(with_vector: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(with_vector: WithVector) {
            match with_vector {
                WithVector::Bool(_) => {}
                WithVector::Selector(_) => {}
            }
        }

        let with_vector = match with_vector.extract()? {
            WithVectorHelper::Bool(bool) => WithVector::Bool(bool),
            WithVectorHelper::Selector(vectors) => WithVector::Selector(vectors),
        };

        Ok(Self(with_vector))
    }
}

impl<'py> IntoPyObject<'py> for PyWithVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?
    const OUTPUT_TYPE: PyStaticExpr = WITH_VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyWithVector {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible?
    const OUTPUT_TYPE: PyStaticExpr = WITH_VECTOR.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            WithVector::Bool(bool) => bool.into_bound_py_any(py),
            WithVector::Selector(vectors) => vectors.into_bound_py_any(py),
        }
    }
}

impl Repr for PyWithVector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            WithVector::Bool(bool) => bool.fmt(f),
            WithVector::Selector(vectors) => vectors.fmt(f),
        }
    }
}
