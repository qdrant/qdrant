use std::mem;

use derive_more::Into;
use pyo3::IntoPyObjectExt as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyFloat, PyInt, PyList};
use segment::types::*;

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
    fn extract_bound(any: &Bound<'py, PyAny>) -> PyResult<Self> {
        let dict = any.cast()?;
        let object = object_from_py(dict)?;
        Ok(Self(Payload(object)))
    }
}

impl<'py> IntoPyObject<'py> for PyPayload {
    type Target = pyo3::types::PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        IntoPyObject::into_pyobject(&self, py)
    }
}

impl<'py> IntoPyObject<'py> for &PyPayload {
    type Target = pyo3::types::PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        object_into_py(&self.0.0, py)
    }
}

fn value_from_py(val: &Bound<PyAny>) -> PyResult<serde_json::Value> {
    let value = if val.is_none() {
        serde_json::Value::Null
    } else if let Ok(bool) = val.extract() {
        serde_json::Value::Bool(bool)
    } else if let Ok(num) = number_from_py(val) {
        serde_json::Value::Number(num)
    } else if let Ok(str) = val.extract() {
        serde_json::Value::String(str)
    } else if let Ok(list) = val.cast() {
        let arr = array_from_py(list)?;
        serde_json::Value::Array(arr)
    } else if let Ok(dict) = val.cast() {
        let obj = object_from_py(dict)?;
        serde_json::Value::Object(obj)
    } else {
        return Err(PyValueError::new_err(format!(
            "failed to convert Python object {val} into payload value type"
        )));
    };

    Ok(value)
}

fn value_into_py<'py>(val: &serde_json::Value, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    match val {
        serde_json::Value::Null => Ok(py.None().into_bound(py)),
        serde_json::Value::Bool(bool) => bool.into_bound_py_any(py),
        serde_json::Value::Number(num) => Ok(number_into_py(num, py)),
        serde_json::Value::String(str) => str.into_bound_py_any(py),
        serde_json::Value::Array(arr) => array_into_py(arr, py).map(Bound::into_any),
        serde_json::Value::Object(obj) => object_into_py(obj, py).map(Bound::into_any),
    }
}

fn number_from_py(num: &Bound<PyAny>) -> PyResult<serde_json::Number> {
    let number = if let Ok(uint) = num.extract::<u64>() {
        serde_json::Number::from(uint)
    } else if let Ok(int) = num.extract::<i64>() {
        serde_json::Number::from(int)
    } else if let Ok(float) = num.extract() {
        serde_json::Number::from_f64(float).ok_or_else(|| {
            PyValueError::new_err(format!(
                "failed to convert {float} into payload number type"
            ))
        })?
    } else {
        return Err(PyValueError::new_err(format!(
            "failed to convert Python object {num} into payload number type"
        )));
    };

    Ok(number)
}

fn number_into_py<'py>(num: &serde_json::Number, py: Python<'py>) -> Bound<'py, PyAny> {
    if let Some(uint) = num.as_u64() {
        PyInt::new(py, uint).into_any()
    } else if let Some(int) = num.as_i64() {
        PyInt::new(py, int).into_any()
    } else if let Some(float) = num.as_f64() {
        PyFloat::new(py, float).into_any()
    } else {
        unreachable!("`serde_json::Number` is always `u64`, `i64` or `f64`")
    }
}

type Array = Vec<serde_json::Value>;

fn array_from_py(list: &Bound<PyList>) -> PyResult<Array> {
    let mut array = Vec::with_capacity(list.len());

    for value in list {
        let value = value_from_py(&value)?;
        array.push(value);
    }

    Ok(array)
}

fn array_into_py<'py>(array: &Array, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);

    for value in array {
        let value = value_into_py(value, py)?;
        list.append(value)?;
    }

    Ok(list)
}

type Object = serde_json::Map<String, serde_json::Value>;

fn object_from_py(dict: &Bound<PyDict>) -> PyResult<Object> {
    let mut object = serde_json::Map::with_capacity(dict.len());

    for (key, value) in dict {
        let key = key.extract()?;
        let value = value_from_py(&value)?;
        object.insert(key, value);
    }

    Ok(object)
}

fn object_into_py<'py>(object: &Object, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);

    for (key, value) in object {
        dict.set_item(key, value_into_py(value, py)?)?;
    }

    Ok(dict)
}
