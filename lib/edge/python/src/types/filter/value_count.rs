use std::fmt;

use derive_more::Into;
use pyo3::prelude::*;
use segment::types::ValuesCount;

use crate::repr::*;

#[pyclass(name = "ValuesCount")]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyValuesCount(pub ValuesCount);

#[pymethods]
impl PyValuesCount {
    #[new]
    #[pyo3(signature = (lt=None, gt=None, lte=None, gte=None))]
    pub fn new(
        lt: Option<usize>,
        gt: Option<usize>,
        lte: Option<usize>,
        gte: Option<usize>,
    ) -> Self {
        Self(ValuesCount { lt, gt, lte, gte })
    }

    #[getter]
    pub fn lt(&self) -> Option<usize> {
        self.0.lt
    }

    #[getter]
    pub fn gt(&self) -> Option<usize> {
        self.0.gt
    }

    #[getter]
    pub fn lte(&self) -> Option<usize> {
        self.0.lte
    }

    #[getter]
    pub fn gte(&self) -> Option<usize> {
        self.0.gte
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl Repr for PyValuesCount {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.class::<Self>(&[
            ("lt", &self.0.lt),
            ("gt", &self.0.gt),
            ("lte", &self.0.lte),
            ("gte", &self.0.gte),
        ])
    }
}
