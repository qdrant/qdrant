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
    use super::search::{PyScoredPoint, PySearchParams, PySearchRequest};
    #[pymodule_export]
    use super::types::filter::condition::{
        PyHasIdCondition, PyHasVectorCondition, PyIsEmptyCondition, PyIsNullCondition,
    };
    #[pymodule_export]
    use super::types::filter::geo::{PyGeoBoundingBox, PyGeoPoint, PyGeoPolygon, PyGeoRadius};
    #[pymodule_export]
    use super::types::filter::r#match::{
        PyMatchAny, PyMatchExcept, PyMatchPhrase, PyMatchText, PyMatchTextAny, PyMatchValue,
    };
    #[pymodule_export]
    use super::types::filter::min_should::PyMinShould;
    #[pymodule_export]
    use super::types::filter::nested::PyNestedCondition;
    #[pymodule_export]
    use super::types::filter::range::{PyRangeDateTime, PyRangeFloat};
    #[pymodule_export]
    use super::types::filter::value_count::PyValuesCount;
    #[pymodule_export]
    use super::types::filter::{PyFilter, field_condition::PyFieldCondition};
    #[pymodule_export]
    use super::types::{PyPoint, PyPointVectors, PyRecord, PySparseVector};
    #[pymodule_export]
    use super::update::PyUpdateOperation;
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
        let points = PyScoredPoint::from_rust_vec(points);
        Ok(points)
    }

    pub fn retrieve(
        &self,
        point_ids: Vec<PyPointId>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
    ) -> Result<Vec<PyRecord>> {
        let point_ids = PyPointId::into_rust_vec(point_ids);
        let points = self.0.retrieve(
            &point_ids,
            with_payload.map(WithPayloadInterface::from),
            with_vector.map(WithVector::from),
        )?;
        let points = PyRecord::from_rust_vec(points);
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
        PyException::new_err(err.0.to_string())
    }
}
