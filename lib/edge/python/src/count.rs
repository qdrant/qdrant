use bytemuck::TransparentWrapper as _;
use derive_more::Into;
use edge::CountRequest;
use pyo3::prelude::*;
use segment::types::Filter;

use crate::repr::*;
use crate::types::PyFilter;

#[pyclass(name = "CountRequest", from_py_object)]
#[derive(Clone, Debug, Into)]
pub struct PyCountRequest(CountRequest);

#[pyclass_repr]
#[pymethods]
impl PyCountRequest {
    #[new]
    #[pyo3(signature = (exact = true, filter = None))]
    pub fn new(exact: bool, filter: Option<PyFilter>) -> Self {
        Self(CountRequest {
            filter: filter.map(Filter::from),
            exact,
        })
    }

    #[getter]
    pub fn filter(&self) -> Option<&PyFilter> {
        self.0.filter.as_ref().map(PyFilter::wrap_ref)
    }

    #[getter]
    pub fn exact(&self) -> bool {
        self.0.exact
    }
}
