use std::str::FromStr;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyclass, pymethods};
use segment::json_path::JsonPath;
use segment::types::{
    FieldCondition, GeoBoundingBox, GeoPolygon, GeoRadius, Match, RangeInterface, ValuesCount,
};

use crate::types::filter::r#match::PyMatch;
use crate::types::filter::value_count::PyValuesCount;
use crate::types::geo::{PyGeoBoundingBox, PyGeoPolygon, PyGeoRadius};
use crate::types::range::PyRangeInterface;

#[pyclass(name = "FieldCondition")]
#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyFieldCondition(pub FieldCondition);

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl PyFieldCondition {
    #[new]
    #[pyo3(signature = (key, r#match=None, range=None, geo_bounding_box=None, geo_radius=None, geo_polygon=None, values_count=None, is_empty=None, is_null=None))]
    pub fn new(
        key: &str,
        r#match: Option<PyMatch>,
        range: Option<PyRangeInterface>,
        geo_bounding_box: Option<PyGeoBoundingBox>,
        geo_radius: Option<PyGeoRadius>,
        geo_polygon: Option<PyGeoPolygon>,
        values_count: Option<PyValuesCount>,
        is_empty: Option<bool>,
        is_null: Option<bool>,
    ) -> Result<Self, PyErr> {
        let key =
            JsonPath::from_str(key).map_err(|_| PyErr::new::<PyValueError, _>(key.to_string()))?;

        let r#match = r#match.map(Match::from);
        let range = range.map(RangeInterface::from);
        let geo_bounding_box = geo_bounding_box.map(GeoBoundingBox::from);
        let geo_radius = geo_radius.map(GeoRadius::from);
        let geo_polygon = geo_polygon.map(GeoPolygon::from);
        let values_count = values_count.map(ValuesCount::from);

        Ok(Self(FieldCondition {
            key,
            r#match,
            range,
            geo_bounding_box,
            geo_radius,
            geo_polygon,
            values_count,
            is_empty,
            is_null,
        }))
    }
}
