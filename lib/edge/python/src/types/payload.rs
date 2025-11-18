use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::prelude::*;
use segment::types::*;

use super::value::*;

#[derive(Clone, Debug, Into, TransparentWrapper)]
#[repr(transparent)]
pub struct PyPayload(pub Payload);

impl FromPyObject<'_, '_> for PyPayload {
    type Error = PyErr;

    fn extract(payload: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
        let payload = value_map_from_py(&payload)?;
        Ok(Self(Payload(payload)))
    }
}

impl<'py> IntoPyObject<'py> for PyPayload {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPayload {
    type Target = PyAny; // PyDict
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Self::Output> {
        value_map_into_py(&self.0.0, py)
    }
}
