pub mod condition;
pub mod field_condition;
pub mod r#match;
pub mod min_should;

use crate::types::filter::min_should::PyMinShould;
use condition::PyCondition;
use derive_more::Into;
use pyo3::{PyErr, pyclass, pymethods};
use segment::types::{Condition, Filter, MinShould};

#[pyclass(name = "Filter")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFilter(pub Filter);

#[pymethods]
impl PyFilter {
    #[new]
    #[pyo3(signature = (must=None, should=None, must_not=None, min_should=None))]
    pub fn new(
        must: Option<Vec<PyCondition>>,
        should: Option<Vec<PyCondition>>,
        must_not: Option<Vec<PyCondition>>,
        min_should: Option<PyMinShould>,
    ) -> Result<Self, PyErr> {
        let must: Option<Vec<_>> = match must {
            Some(must) => Some(must.into_iter().map(Condition::from).collect()),
            None => None,
        };
        let should: Option<Vec<_>> = match should {
            Some(should) => Some(should.into_iter().map(Condition::from).collect()),
            None => None,
        };

        let must_not: Option<Vec<_>> = match must_not {
            Some(must_not) => Some(must_not.into_iter().map(Condition::from).collect()),
            None => None,
        };
        let min_should: Option<MinShould> = match min_should {
            Some(min_should) => Some(min_should.0),
            None => None,
        };

        Ok(Self(Filter {
            should,
            min_should,
            must,
            must_not,
        }))
    }
}
