use derive_more::Into;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::types::PointIdType;

#[derive(Clone, Debug, Into)]
pub struct PyPointId(pub PointIdType);

impl<'py> FromPyObject<'py> for PyPointId {
    fn extract_bound(point_id: &Bound<'py, PyAny>) -> PyResult<Self> {
        let point_id = if let Ok(id) = point_id.extract() {
            Self(PointIdType::NumId(id))
        } else if let Ok(uuid) = point_id.extract() {
            Self(PointIdType::Uuid(uuid))
        } else {
            return Err(PyErr::new::<PyException, _>(format!(
                "failed to convert Python object {point_id} into point ID"
            )));
        };

        Ok(point_id)
    }
}

impl<'py> IntoPyObject<'py> for PyPointId {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.0 {
            PointIdType::NumId(id) => id.into_bound_py_any(py),
            PointIdType::Uuid(uuid) => uuid.into_bound_py_any(py),
        }
    }
}
