use std::convert::Infallible;
use std::fmt;
use std::str::FromStr as _;

use bytemuck::TransparentWrapper;
use derive_more::Into;
use pyo3::exceptions::PyValueError;
use pyo3::inspect::PyStaticExpr;
use pyo3::prelude::*;
use pyo3::types::PyString;
use segment::json_path::JsonPath;

use crate::repr::*;
use crate::type_hint::Alias;

#[derive(Clone, Debug, Into, Eq, PartialEq, Hash, TransparentWrapper)]
#[repr(transparent)]
pub struct PyJsonPath(pub JsonPath);

pub const JSON_PATH: Alias = Alias {
    name: "JsonPath",
    definition: String::INPUT_TYPE,
};

impl FromPyObject<'_, '_> for PyJsonPath {
    type Error = PyErr;
    const INPUT_TYPE: PyStaticExpr = JSON_PATH.hint();

    fn extract(json_path: Borrowed<'_, '_, PyAny>) -> PyResult<Self> {
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
    const OUTPUT_TYPE: PyStaticExpr = JSON_PATH.hint();

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyJsonPath {
    type Target = PyString;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;
    const OUTPUT_TYPE: PyStaticExpr = JSON_PATH.hint();

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyString::new(py, &self.0.to_string()))
    }
}

impl Repr for PyJsonPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.to_string().fmt(f)
    }
}

impl ReprStr for PyJsonPath {}
