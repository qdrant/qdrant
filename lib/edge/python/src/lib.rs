pub mod config;
pub mod search;
pub mod types;
pub mod update;

use std::path::PathBuf;

use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::common::operation_error::OperationError;
use segment::types::*;

use self::config::*;
use self::search::*;
use self::types::*;
use self::update::*;

#[pymodule]
mod qdrant_edge {
    #[pymodule_export]
    use super::PyShard;
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
    use super::types::{PyRecord, PyVector};
    #[pymodule_export]
    use super::update::{PyPoint, PyUpdateOperation};
}

#[pyclass(name = "Shard")]
#[derive(Debug)]
pub struct PyShard(edge::Shard);

#[pymethods]
impl PyShard {
    #[new]
    pub fn load(path: PathBuf, config: Option<PySegmentConfig>) -> Result<Self> {
        let shard = edge::Shard::load(&path, config.map(Into::into))?;
        Ok(Self(shard))
    }

    pub fn update(&self, operation: PyUpdateOperation) -> Result<()> {
        self.0.update(operation.into())?;
        Ok(())
    }

    pub fn search(&self, search: PySearchRequest) -> Result<Vec<PyScoredPoint>> {
        let points = self.0.search(search.into())?;
        let points = points.into_iter().map(PyScoredPoint).collect();
        Ok(points)
    }

    pub fn retrieve(
        &self,
        ids: Vec<PyPointId>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
    ) -> Result<Vec<PyRecord>, PyErr> {
        let ids_res: Result<Vec<_>, _> = ids.into_iter().map(PointIdType::try_from).collect();
        let ids = ids_res?;

        let records = self
            .0
            .retrieve(
                &ids,
                with_payload.map(WithPayloadInterface::from),
                with_vector.map(WithVector::from),
            )
            .map_err(PyError::from)?;

        let points = records.into_iter().map(PyRecord).collect();
        Ok(points)
    }
}

pub type Result<T, E = PyError> = std::result::Result<T, E>;

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
