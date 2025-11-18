use bytemuck::{TransparentWrapper, TransparentWrapperAlloc as _};
use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::types::*;

#[pyclass(name = "GeoBoundingBox")]
#[derive(Clone, Debug, Into)]
pub struct PyGeoBoundingBox(pub GeoBoundingBox);

#[pymethods]
impl PyGeoBoundingBox {
    #[new]
    pub fn new(top_left: PyGeoPoint, bottom_right: PyGeoPoint) -> Self {
        Self(GeoBoundingBox {
            top_left: GeoPoint::from(top_left),
            bottom_right: GeoPoint::from(bottom_right),
        })
    }
}

#[pyclass(name = "GeoRadius")]
#[derive(Clone, Debug, Into)]
pub struct PyGeoRadius(pub GeoRadius);

#[pymethods]
impl PyGeoRadius {
    #[new]
    pub fn new(center: PyGeoPoint, radius: f64) -> Self {
        Self(GeoRadius {
            center: GeoPoint::from(center),
            radius: OrderedFloat(radius),
        })
    }
}

#[pyclass(name = "GeoPolygon")]
#[derive(Clone, Debug, Into)]
pub struct PyGeoPolygon(pub GeoPolygon);

#[pymethods]
impl PyGeoPolygon {
    #[new]
    #[pyo3(signature = (exterior, interiors=None))]
    pub fn new(
        exterior: PyGeoLineString,
        interiors: Option<Vec<PyGeoLineString>>,
    ) -> Result<Self, PyErr> {
        let shadow = GeoPolygonShadow {
            exterior: GeoLineString::from(exterior),
            interiors: interiors.map(PyGeoLineString::peel_vec),
        };

        let polygon =
            GeoPolygon::try_from(shadow).map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(polygon))
    }
}

#[pyclass(name = "GeoPoint")]
#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyGeoPoint(pub GeoPoint);

#[pymethods]
impl PyGeoPoint {
    #[new]
    pub fn new(lon: f64, lat: f64) -> Result<Self, PyErr> {
        let point =
            GeoPoint::new(lon, lat).map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(point))
    }
}

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyGeoLineString(GeoLineString);

impl FromPyObject<'_, '_> for PyGeoLineString {
    type Error = PyErr;

    fn extract(points: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let points = points.extract()?;

        Ok(Self(GeoLineString {
            points: PyGeoPoint::peel_vec(points),
        }))
    }
}

impl<'py> IntoPyObject<'py> for PyGeoLineString {
    type Target = PyAny; // PyList
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        PyGeoPoint::wrap_vec(self.0.points).into_bound_py_any(py)
    }
}
