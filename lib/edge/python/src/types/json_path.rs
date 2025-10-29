use std::convert::Infallible;
use std::str::FromStr as _;

use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use segment::json_path::JsonPath;

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyJsonPath(pub JsonPath);

impl<'py> FromPyObject<'py> for PyJsonPath {
    fn extract_bound(json_path: &Bound<'py, PyAny>) -> PyResult<Self> {
        let json_path: String = json_path.extract()?;
        let json_path = JsonPath::from_str(&json_path)
            .map_err(|_| PyValueError::new_err(format!("invalid JSON path {json_path}")))?;

        Ok(PyJsonPath(json_path))
    }
}

impl<'py> IntoPyObject<'py> for PyJsonPath {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyJsonPath {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &self.0.to_string()))
    }
}
