use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::json_path::JsonPath;
use segment::types::*;

use crate::repr::*;
use crate::types::*;

#[pyclass(name = "FieldCondition")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyFieldCondition(pub FieldCondition);

#[pyclass_repr]
#[pymethods]
impl PyFieldCondition {
    #[new]
    #[pyo3(signature = (
        key,
        r#match=None,
        range=None,
        geo_bounding_box=None,
        geo_radius=None,
        geo_polygon=None,
        values_count=None,
        is_empty=None,
        is_null=None,
    ))]
    #[expect(clippy::too_many_arguments)]
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

    #[getter]
    pub fn key(&self) -> &PyJsonPath {
        PyJsonPath::wrap_ref(&self.0.key)
    }

    #[getter]
    pub fn r#match(&self) -> Option<PyMatch> {
        self.0.r#match.clone().map(PyMatch)
    }

    #[getter]
    pub fn range(&self) -> Option<PyRange> {
        self.0.range.map(PyRange::from)
    }

    #[getter]
    pub fn geo_bounding_box(&self) -> Option<PyGeoBoundingBox> {
        self.0.geo_bounding_box.map(PyGeoBoundingBox)
    }

    #[getter]
    pub fn geo_radius(&self) -> Option<PyGeoRadius> {
        self.0.geo_radius.map(PyGeoRadius)
    }

    #[getter]
    pub fn geo_polygon(&self) -> Option<PyGeoPolygon> {
        self.0.geo_polygon.clone().map(PyGeoPolygon)
    }

    #[getter]
    pub fn values_count(&self) -> Option<PyValuesCount> {
        self.0.values_count.map(PyValuesCount)
    }

    #[getter]
    pub fn is_empty(&self) -> Option<bool> {
        self.0.is_empty
    }

    #[getter]
    pub fn is_null(&self) -> Option<bool> {
        self.0.is_null
    }
}

impl PyFieldCondition {
    fn _getters(self) {
        // Every field should have a getter method
        let FieldCondition {
            key: _,
            r#match: _,
            range: _,
            geo_bounding_box: _,
            geo_radius: _,
            geo_polygon: _,
            values_count: _,
            is_empty: _,
            is_null: _,
        } = self.0;
    }
}
