use std::str::FromStr;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyclass, pymethods};
use segment::json_path::JsonPath;
use segment::types::{FieldCondition, Match, RangeInterface};

use crate::types::filter::r#match::PyMatch;
use crate::types::range::PyRangeInterface;

#[pyclass(name = "FieldCondition")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFieldCondition(pub FieldCondition);

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyFieldCondition {
    #[new]
    #[pyo3(signature = (key, r#match=None, range=None))]
    pub fn new(
        key: &str,
        r#match: Option<PyMatch>,
        range: Option<PyRangeInterface>,
    ) -> Result<Self, PyErr> {
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;

        let r#match = r#match.map(Match::from);
        let range = range.map(RangeInterface::from);

        Ok(Self(FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box: None,
            geo_radius: None,
            geo_polygon: None,
            values_count: None,
            is_empty: None,
            is_null: None,
        }))
    }
}
