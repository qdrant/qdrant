use std::collections::HashMap;
use std::mem;

use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

#[derive(Clone, Debug, Into)]
#[repr(transparent)]
pub struct PyValue(serde_json::Value);

impl PyValue {
    pub fn from_ref(value: &serde_json::Value) -> &Self {
        // `PyValue` has transparent representation, so we can safely transmute references
        unsafe { mem::transmute(value) }
    }

    pub fn from_slice(values: &[serde_json::Value]) -> &[Self] {
        // `PyValue` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(values) }
    }

    pub fn into_rust_vec(values: Vec<Self>) -> Vec<serde_json::Value> {
        // `PyValue` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(values) }
    }

    pub fn into_rust_map(values: HashMap<String, Self>) -> HashMap<String, serde_json::Value> {
        // `PyValue` has transparent representation, so transmuting is safe
        unsafe { mem::transmute(values) }
    }
}

impl<'py> FromPyObject<'py> for PyValue {
    fn extract_bound(value: &Bound<'py, PyAny>) -> PyResult<Self> {
        #[derive(FromPyObject)]
        enum Helper {
            Bool(bool),
            Uint(u64),
            Int(i64),
            Float(f64),
            String(String),
            Array(Vec<PyValue>),
            Object(#[pyo3(from_py_with = value_map_from_py)] ValueMap),
        }

        if value.is_none() {
            return Ok(Self(serde_json::Value::Null));
        }

        let value = match value.extract()? {
            Helper::Bool(bool) => serde_json::Value::Bool(bool),
            Helper::Uint(uint) => serde_json::Value::Number(serde_json::Number::from(uint)),
            Helper::Int(int) => serde_json::Value::Number(serde_json::Number::from(int)),
            Helper::Float(float) => {
                let num = serde_json::Number::from_f64(float).ok_or_else(|| {
                    PyValueError::new_err(format!(
                        "failed to convert {float} into payload number type"
                    ))
                })?;

                serde_json::Value::Number(num)
            }
            Helper::String(str) => serde_json::Value::String(str),
            Helper::Array(arr) => serde_json::Value::Array(PyValue::into_rust_vec(arr)),
            Helper::Object(map) => serde_json::Value::Object(map),
        };

        Ok(Self(value))
    }
}

impl<'py> IntoPyObject<'py> for PyValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match &self.0 {
            serde_json::Value::Null => Ok(py.None().into_bound(py)),
            serde_json::Value::Bool(bool) => bool.into_bound_py_any(py),
            serde_json::Value::Number(num) => {
                if let Some(uint) = num.as_u64() {
                    uint.into_bound_py_any(py)
                } else if let Some(int) = num.as_i64() {
                    int.into_bound_py_any(py)
                } else if let Some(float) = num.as_f64() {
                    float.into_bound_py_any(py)
                } else {
                    unreachable!("`serde_json::Number` is always `u64`, `i64` or `f64`")
                }
            }
            serde_json::Value::String(str) => str.into_bound_py_any(py),
            serde_json::Value::Array(arr) => PyValue::from_slice(arr).into_bound_py_any(py),
            serde_json::Value::Object(map) => value_map_into_py(map, py),
        }
    }
}

pub type ValueMap = serde_json::Map<String, serde_json::Value>;

pub fn value_map_from_py(dict: &Bound<'_, PyAny>) -> PyResult<ValueMap> {
    let dict = dict.cast::<PyDict>()?;

    let mut map = serde_json::Map::with_capacity(dict.len());

    for (key, value) in dict {
        let key = key.extract()?;
        let value: PyValue = value.extract()?;
        map.insert(key, value.into());
    }

    Ok(map)
}

pub fn value_map_into_py<'py>(map: &ValueMap, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let dict = PyDict::new(py);

    for (key, value) in map {
        let key = PyString::new(py, key);
        let value = PyValue::from_ref(value).into_bound_py_any(py)?;
        dict.set_item(key, value)?;
    }

    Ok(dict.into_any())
}
