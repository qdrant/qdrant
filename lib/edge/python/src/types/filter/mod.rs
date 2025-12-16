pub mod condition;
pub mod field_condition;
pub mod geo;
pub mod r#match;
pub mod min_should;
pub mod nested;
pub mod range;
pub mod value_count;

use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::{Filter, MinShould};

pub use self::condition::*;
pub use self::field_condition::*;
pub use self::geo::*;
pub use self::r#match::*;
pub use self::min_should::*;
pub use self::nested::*;
pub use self::range::*;
pub use self::value_count::*;
use crate::repr::*;

#[pyclass(name = "Filter")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFilter(pub Filter);

#[pyclass_repr]
#[pymethods]
impl PyFilter {
    #[new]
    #[pyo3(signature = (must=None, should=None, must_not=None, min_should=None))]
    pub fn new(
        must: Option<Vec<PyCondition>>,
        should: Option<Vec<PyCondition>>,
        must_not: Option<Vec<PyCondition>>,
        min_should: Option<PyMinShould>,
    ) -> Self {
        Self(Filter {
            must: must.map(PyCondition::peel_vec),
            should: should.map(PyCondition::peel_vec),
            must_not: must_not.map(PyCondition::peel_vec),
            min_should: min_should.map(MinShould::from),
        })
    }

    #[getter]
    pub fn must(&self) -> Option<&[PyCondition]> {
        self.0
            .must
            .as_ref()
            .map(|must| PyCondition::wrap_slice(must))
    }

    #[getter]
    pub fn should(&self) -> Option<&[PyCondition]> {
        self.0
            .should
            .as_ref()
            .map(|should| PyCondition::wrap_slice(should))
    }

    #[getter]
    pub fn must_not(&self) -> Option<&[PyCondition]> {
        self.0
            .must_not
            .as_ref()
            .map(|must_not| PyCondition::wrap_slice(must_not))
    }

    #[getter]
    pub fn min_should(&self) -> Option<PyMinShould> {
        self.0.min_should.clone().map(PyMinShould)
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyFilter {
    fn _getters(self) {
        // Every field should have a getter method
        let Filter {
            must: _,
            should: _,
            must_not: _,
            min_should: _,
        } = self.0;
    }
}

impl<'py> IntoPyObject<'py> for &PyFilter {
    type Target = PyFilter;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(self.clone(), py)
    }
}
