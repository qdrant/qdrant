use bytemuck::TransparentWrapper as _;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::Filter;
use shard::count::CountRequestInternal;

use crate::repr::*;
use crate::types::PyFilter;

#[pyclass(name = "CountRequest")]
#[derive(Clone, Debug, Into)]
pub struct PyCountRequest(CountRequestInternal);

#[pyclass_repr]
#[pymethods]
impl PyCountRequest {
    #[new]
    pub fn new(filter: Option<PyFilter>, exact: bool) -> Self {
        Self(CountRequestInternal {
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
