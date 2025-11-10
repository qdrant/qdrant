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

#[pyclass(name = "Filter")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
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
    ) -> Self {
        Self(Filter {
            must: must.map(PyCondition::peel_vec),
            should: should.map(PyCondition::peel_vec),
            must_not: must_not.map(PyCondition::peel_vec),
            min_should: min_should.map(MinShould::from),
        })
    }
}
