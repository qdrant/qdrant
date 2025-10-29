use std::mem;

use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

use super::value::*;

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyPayload(pub Payload);

impl PyPayload {
    pub fn from_ref(payload: &Payload) -> &Self {
        // `PyPayload` has transparent representation, so we can safely transmute references
        unsafe { mem::transmute(payload) }
    }
}

impl<'py> FromPyObject<'py> for PyPayload {
    fn extract_bound(payload: &Bound<'py, PyAny>) -> PyResult<Self> {
        let payload = value_map_from_py(payload)?;
        Ok(Self(Payload(payload)))
    }
}

impl<'py> IntoPyObject<'py> for PyPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPayload {
    type Target = PyAny; // PyDict
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        value_map_into_py(&self.0.0, py)
    }
}
