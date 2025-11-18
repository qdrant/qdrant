use std::mem;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use segment::types::PointIdType;
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPointId(pub PointIdType);

impl PyPointId {
    pub fn peel_set(set: ahash::HashSet<Self>) -> ahash::HashSet<PointIdType>
    where
        Self: TransparentWrapper<PointIdType>,
    {
        unsafe { mem::transmute(set) }
    }
}

impl FromPyObject<'_, '_> for PyPointId {
    type Error = PyErr;

    fn extract(point_id: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            NumId(u64),
            Uuid(Uuid),
            UuidStr(String),
        }

        fn _variants(point_id: PointIdType) {
            match point_id {
                PointIdType::NumId(_) => {}
                PointIdType::Uuid(_) => {}
            }
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

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPointId {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match &self.0 {
            PointIdType::NumId(id) => id.into_bound_py_any(py),
            PointIdType::Uuid(uuid) => uuid.into_bound_py_any(py),
        }
    }
}
