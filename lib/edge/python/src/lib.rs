pub mod config;
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

#[pymodule]
mod qdrant_edge {
    #[pymodule_export]
    use super::config::{
        PyDistance, PyIndexes, PyMultiVectorComparator, PyMultiVectorConfig, PyPayloadStorageType,
        PyQuantizationConfig, PySegmentConfig, PySparseVectorDataConfig, PyVectorDataConfig,
        PyVectorStorageDatatype, PyVectorStorageType,
    };
    #[pymodule_export]
    use super::search::{
        PyFilter, PyQuery, PyQueryVector, PyScoredPoint, PySearchParams, PySearchRequest,
        PyWithPayload, PyWithVector,
    };
    #[pymodule_export]
    use super::update::{PyPoint, PyUpdateOperation, PyVector};
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
    fn new(_dict: Py<pyo3::types::PyDict>) -> Self {
        Self(Payload::default()) // TODO!
    }
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
