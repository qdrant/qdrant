use derive_more::Into;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::*;

use crate::types::*;

#[pyclass(name = "FieldCondition")]
#[derive(Clone, Debug, Into)]
pub struct PyFieldCondition(pub FieldCondition);

#[pymethods]
impl PyFieldCondition {
    #[new]
    #[pyo3(signature = (key, r#match=None, range=None, geo_bounding_box=None, geo_radius=None, geo_polygon=None, values_count=None, is_empty=None, is_null=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key: PyJsonPath,
        r#match: Option<PyMatch>,
        range: Option<PyRange>,
        geo_bounding_box: Option<PyGeoBoundingBox>,
        geo_radius: Option<PyGeoRadius>,
        geo_polygon: Option<PyGeoPolygon>,
        values_count: Option<PyValuesCount>,
        is_empty: Option<bool>,
        is_null: Option<bool>,
    ) -> Self {
        Self(FieldCondition {
            key: JsonPath::from(key),
            r#match: r#match.map(Match::from),
            range: range.map(RangeInterface::from),
            geo_bounding_box: geo_bounding_box.map(GeoBoundingBox::from),
            geo_radius: geo_radius.map(GeoRadius::from),
            geo_polygon: geo_polygon.map(GeoPolygon::from),
            values_count: values_count.map(ValuesCount::from),
            is_empty,
            is_null,
        })
    }
}
