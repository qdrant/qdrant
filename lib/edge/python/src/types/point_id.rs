use std::mem;

use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::types::PointIdType;
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyPointId(pub PointIdType);

impl PyPointId {
    pub fn into_rust_vec(point_ids: Vec<PyPointId>) -> Vec<PointIdType> {
        // `PyPointId` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(point_ids) }
    }
}

impl<'py> FromPyObject<'py> for PyPointId {
    fn extract_bound(point_id: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            NumId(u64),
            Uuid(Uuid),
            UuidStr(String),
        }

        let point_id = match point_id.extract()? {
            Helper::NumId(id) => PointIdType::NumId(id),
            Helper::Uuid(uuid) => PointIdType::Uuid(uuid),
            Helper::UuidStr(uuid_str) => {
                let uuid = Uuid::parse_str(&uuid_str).map_err(|err| {
                    PyValueError::new_err(format!("failed to parse {uuid_str} as UUID: {err}"))
                })?;

                PointIdType::Uuid(uuid)
            }
        };

        Ok(Self(point_id))
    }
}

impl<'py> IntoPyObject<'py> for PyPointId {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPointId {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match &self.0 {
            PointIdType::NumId(id) => id.into_bound_py_any(py),
            PointIdType::Uuid(uuid) => uuid.into_bound_py_any(py),
        }
    }
}
