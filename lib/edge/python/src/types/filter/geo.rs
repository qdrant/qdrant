use derive_more::Into;
use ordered_float::OrderedFloat;
use pyo3::exceptions::PyValueError;
use pyo3::{PyErr, pyclass, pymethods};
use segment::types::{
    GeoBoundingBox, GeoLineString, GeoPoint, GeoPolygon, GeoPolygonShadow, GeoRadius,
};

#[pyclass(name = "GeoPoint")]
#[derive(Clone, Debug, Into)]
pub struct PyGeoPoint(pub GeoPoint);

#[pymethods]
impl PyGeoPoint {
    #[new]
    pub fn new(lon: f64, lat: f64) -> Result<Self, PyErr> {
        Ok(Self(GeoPoint::new(lon, lat).map_err(|err| {
            PyErr::new::<PyValueError, _>(err.to_string())
        })?))
    }
}

#[pyclass(name = "GeoBoundingBox")]
#[derive(Clone, Debug, Into)]
pub struct PyGeoBoundingBox(pub GeoBoundingBox);

#[pymethods]
impl PyGeoBoundingBox {
    #[new]
    pub fn new(top_left: PyGeoPoint, bottom_right: PyGeoPoint) -> Self {
        Self(GeoBoundingBox {
            top_left: top_left.0,
            bottom_right: bottom_right.0,
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
            center: center.0,
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
        exterior: Vec<PyGeoPoint>,
        interiors: Option<Vec<Vec<PyGeoPoint>>>,
    ) -> Result<Self, PyErr> {
        let exterior = GeoLineString {
            points: exterior.into_iter().map(GeoPoint::from).collect(),
        };

        let interiors = interiors.map(|interiors| {
            interiors
                .into_iter()
                .map(|ring| GeoLineString {
                    points: ring.into_iter().map(GeoPoint::from).collect(),
                })
                .collect()
        });

        let shadow = GeoPolygonShadow {
            exterior,
            interiors,
        };

        let polygon = GeoPolygon::try_from(shadow)
            .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;

        Ok(Self(polygon))
    }
}
