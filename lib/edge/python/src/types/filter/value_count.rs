use derive_more::Into;
use pyo3::prelude::*;
use segment::types::ValuesCount;

use crate::repr::*;

/// Condition on count of values in array field.
///
/// Args:
///     lt: Less than.
///     gt: Greater than.
///     lte: Less than or equal.
///     gte: Greater than or equal.
#[pyclass(name = "ValuesCount", from_py_object)]
#[derive(Copy, Clone, Debug, Into)]
pub struct PyValuesCount(pub ValuesCount);

#[pyclass_repr]
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

    /// Less than.
    #[getter]
    pub fn lt(&self) -> Option<usize> {
        self.0.lt
    }

    /// Greater than.
    #[getter]
    pub fn gt(&self) -> Option<usize> {
        self.0.gt
    }

    /// Less than or equal.
    #[getter]
    pub fn lte(&self) -> Option<usize> {
        self.0.lte
    }

    /// Greater than or equal.
    #[getter]
    pub fn gte(&self) -> Option<usize> {
        self.0.gte
    }

    pub fn __repr__(&self) -> String {
        self.repr()
    }
}

impl PyValuesCount {
    fn _getters(self) {
        // Every field should have a getter method
        let ValuesCount {
            lt: _,
            gt: _,
            lte: _,
            gte: _,
        } = self.0;
    }
}
