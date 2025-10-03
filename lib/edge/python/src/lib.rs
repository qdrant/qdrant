pub mod config;
pub mod interface;
pub mod search;
pub mod update;

use std::path::PathBuf;

use derive_more::Into;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::common::operation_error::OperationError;
use segment::types::*;
use uuid::Uuid;

use self::config::*;
use self::search::*;
use self::update::*;
use crate::interface::py_record::PyRecord;

#[pymodule]
mod qdrant_edge {
    #[pymodule_export]
    use super::config::{
        PyDistance, PyIndexes, PyMultiVectorComparator, PyMultiVectorConfig, PyPayloadStorageType,
        PyQuantizationConfig, PySegmentConfig, PySparseVectorDataConfig, PyVectorDataConfig,
        PyVectorStorageDatatype, PyVectorStorageType,
    };
    #[pymodule_export]
    use super::interface::py_record::PyRecord;
    #[pymodule_export]
    use super::interface::py_vector::PyVector;
    #[pymodule_export]
    use super::search::{
        PyFilter, PyQuery, PyQueryVector, PyScoredPoint, PySearchParams, PySearchRequest,
        PyWithPayload, PyWithVector,
    };
    #[pymodule_export]
    use super::update::{PyPoint, PyUpdateOperation};
    #[pymodule_export]
    use super::{PyPayload, PyPointId, PyShard};
}

#[pyclass(name = "Shard")]
#[derive(Debug)]
pub struct PyShard(edge::Shard);

#[pymethods]
impl PyShard {
    #[new]
    pub fn load(path: PathBuf, config: Option<PySegmentConfig>) -> PyResult<Self> {
        let shard = edge::Shard::load(&path, config.map(Into::into))?;
        Ok(Self(shard))
    }

    pub fn update(&self, operation: PyUpdateOperation) -> PyResult<()> {
        self.0.update(operation.into())?;
        Ok(())
    }

    pub fn search(&self, search: PySearchRequest) -> PyResult<Vec<PyScoredPoint>> {
        let points = self.0.search(search.into())?;
        let points = points.into_iter().map(PyScoredPoint).collect();
        Ok(points)
    }

    pub fn retrieve(
        &self,
        ids: Vec<PyPointId>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
    ) -> PyResult<Vec<PyRecord>> {
        let ids: Vec<_> = ids.into_iter().map(PointIdType::from).collect();
        let records = self.0.retrieve(
            &ids,
            with_payload.map(WithPayloadInterface::from),
            with_vector.map(WithVector::from),
        )?;

        let points = records.into_iter().map(PyRecord).collect();
        Ok(points)
    }
}

#[pyclass(name = "PointId")]
#[derive(Clone, Debug, Into)]
pub struct PyPointId(PointIdType);

#[pymethods]
impl PyPointId {
    #[staticmethod]
    pub fn num(id: u64) -> Self {
        Self(PointIdType::NumId(id))
    }

    #[staticmethod]
    pub fn uuid(uuid: Uuid) -> Self {
        Self(PointIdType::Uuid(uuid))
    }
}

#[pyclass(name = "Payload")]
#[derive(Clone, Debug, Into)]
pub struct PyPayload(Payload);

#[pymethods]
impl PyPayload {
    #[new]
    fn new(dict: Bound<'_, pyo3::types::PyDict>) -> pyo3::PyResult<Self> {
        let obj = payload_object_from_py(&dict)?;
        Ok(Self(Payload(obj)))
    }
}

fn payload_object_from_py(
    dict: &Bound<'_, pyo3::types::PyDict>,
) -> pyo3::PyResult<serde_json::Map<String, serde_json::Value>> {
    let mut object = serde_json::Map::with_capacity(dict.len());

    for (key, value) in dict {
        let key = key.extract()?;
        let value = payload_value_from_py(&value)?;
        object.insert(key, value);
    }

    Ok(object)
}

fn payload_array_from_py(
    list: &Bound<'_, pyo3::types::PyList>,
) -> pyo3::PyResult<Vec<serde_json::Value>> {
    let mut array = Vec::with_capacity(list.len());

    for value in list {
        let value = payload_value_from_py(&value)?;
        array.push(value);
    }

    Ok(array)
}

fn payload_value_from_py(val: &Bound<'_, PyAny>) -> pyo3::PyResult<serde_json::Value> {
    if val.is_none() {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(dict) = val.cast() {
        let obj = payload_object_from_py(dict)?;
        return Ok(serde_json::Value::Object(obj));
    }

    if let Ok(list) = val.cast() {
        let arr = payload_array_from_py(list)?;
        return Ok(serde_json::Value::Array(arr));
    }

    if let Ok(str) = val.extract() {
        return Ok(serde_json::Value::String(str));
    }

    if let Ok(uint) = val.extract() {
        let num = serde_json::Number::from_u128(uint).unwrap(); // TODO?
        return Ok(serde_json::Value::Number(num));
    }

    if let Ok(int) = val.extract() {
        let num = serde_json::Number::from_i128(int).unwrap(); // TODO?
        return Ok(serde_json::Value::Number(num));
    }

    if let Ok(float) = val.extract() {
        let num = serde_json::Number::from_f64(float).unwrap(); // TODO?
        return Ok(serde_json::Value::Number(num));
    }

    if let Ok(bool) = val.extract() {
        return Ok(serde_json::Value::Bool(bool));
    }

    Err(PyErr::new::<PyException, _>(format!(
        "failed to convert Python object {val} into payload value"
    )))
}

pub type PyResult<T, E = PyError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct PyError(OperationError);

impl From<OperationError> for PyError {
    fn from(err: OperationError) -> Self {
        Self(err)
    }
}

impl From<PyError> for PyErr {
    fn from(err: PyError) -> Self {
        PyErr::new::<PyException, _>(err.0.to_string())
    }
}
