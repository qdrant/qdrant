use std::{fmt, mem};

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use segment::types::PointIdType;
use uuid::Uuid;

use crate::repr::*;
use crate::type_hint::Alias;

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

    pub fn wrap_set_ref(set: &ahash::HashSet<PointIdType>) -> &ahash::HashSet<Self>
    where
        Self: TransparentWrapper<PointIdType>,
    {
        unsafe { mem::transmute(set) }
    }
}

pub const POINT_ID: Alias = Alias {
    name: "PointId",
    definition: PointIdHelper::INPUT_TYPE,
};

#[derive(FromPyObject, IntoPyObject)]
enum PointIdHelper {
    NumId(u64),
    Uuid(Uuid),
    UuidStr(String),
}

impl FromPyObject<'_, '_> for PyPointId {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = POINT_ID.hint();

    fn extract(point_id: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        fn _variants(point_id: PointIdType) {
            match point_id {
                PointIdType::NumId(_) => {}
                PointIdType::Uuid(_) => {}
            }
        }

        let point_id = match point_id.extract()? {
            PointIdHelper::NumId(id) => PointIdType::NumId(id),
            PointIdHelper::Uuid(uuid) => PointIdType::Uuid(uuid),
            PointIdHelper::UuidStr(uuid_str) => {
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
    const OUTPUT_TYPE: PyStaticExpr = POINT_ID.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPointId {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr; // Infallible
    const OUTPUT_TYPE: PyStaticExpr = POINT_ID.hint();

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        match self.0 {
            PointIdType::NumId(id) => PointIdHelper::NumId(id),
            PointIdType::Uuid(uuid) => PointIdHelper::Uuid(uuid),
        }
        .into_bound_py_any(py)
    }
}

impl Repr for PyPointId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            PointIdType::NumId(id) => id.fmt(f),
            PointIdType::Uuid(uuid) => uuid.fmt(f),
        }
    }
}
