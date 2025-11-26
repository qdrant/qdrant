pub mod config;
pub mod query;
pub mod search;
pub mod types;
pub mod update;

use std::path::PathBuf;

use bytemuck::TransparentWrapperAlloc as _;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::common::operation_error::OperationError;
use segment::types::*;

use self::config::*;
use self::query::*;
use self::search::*;
use self::types::*;
use self::update::*;

#[pymodule]
mod qdrant_edge {
    #[pymodule_export]
    use super::PyShard;
    #[pymodule_export]
    use super::config::quantization::{
        PyBinaryQuantizationConfig, PyBinaryQuantizationEncoding,
        PyBinaryQuantizationQueryEncoding, PyCompressionRatio, PyProductQuantizationConfig,
        PyScalarQuantizationConfig, PyScalarType,
    };
    #[pymodule_export]
    use super::config::sparse_vector_data::{
        PyModifier, PySparseIndexConfig, PySparseIndexType, PySparseVectorDataConfig,
        PySparseVectorStorageType,
    };
    #[pymodule_export]
    use super::config::vector_data::{
        PyDistance, PyHnswIndexConfig, PyMultiVectorComparator, PyMultiVectorConfig,
        PyPlainIndexConfig, PyVectorDataConfig, PyVectorStorageDatatype, PyVectorStorageType,
    };
    #[pymodule_export]
    use super::config::{PyPayloadStorageType, PySegmentConfig};
    #[pymodule_export]
    use super::query::{
        PyDirection, PyFusion, PyMmr, PyOrderBy, PyPrefetch, PyQueryRequest, PySample,
    };
    #[pymodule_export]
    use super::search::{
        PyAcornSearchParams, PyQuantizationSearchParams, PyScoredPoint, PySearchParams,
        PySearchRequest,
    };
    #[pymodule_export]
    use super::types::filter::{
        PyFieldCondition, PyFilter, PyGeoBoundingBox, PyGeoPoint, PyGeoPolygon, PyGeoRadius,
        PyHasIdCondition, PyHasVectorCondition, PyIsEmptyCondition, PyIsNullCondition, PyMatchAny,
        PyMatchExcept, PyMatchPhrase, PyMatchText, PyMatchTextAny, PyMatchValue, PyMinShould,
        PyNestedCondition, PyRangeDateTime, PyRangeFloat, PyValuesCount,
    };
    #[pymodule_export]
    use super::types::formula::{PyDecayKind, PyExpressionInterface, PyFormula};
    #[pymodule_export]
    use super::types::query::{
        PyContextPair, PyContextQuery, PyDiscoverQuery, PyFeedbackItem, PyFeedbackSimpleQuery,
        PyQueryInterface, PyRecommendQuery, PySimpleFeedbackStrategy,
    };
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
        let shard = edge::Shard::load(&path, config.map(SegmentConfig::from))?;
        Ok(Self(shard))
    }

    pub fn update(&self, operation: PyUpdateOperation) -> Result<()> {
        self.0.update(operation.into())?;
        Ok(())
    }

    pub fn query(&self, query: PyQueryRequest) -> Result<Vec<PyScoredPoint>> {
        let points = self.0.query(query.into())?;
        let points = PyScoredPoint::wrap_vec(points);
        Ok(points)
    }

    pub fn search(&self, search: PySearchRequest) -> Result<Vec<PyScoredPoint>> {
        let points = self.0.search(search.into())?;
        let points = PyScoredPoint::wrap_vec(points);
        Ok(points)
    }

    pub fn retrieve(
        &self,
        point_ids: Vec<PyPointId>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
    ) -> Result<Vec<PyRecord>> {
        let point_ids = PyPointId::peel_vec(point_ids);
        let points = self.0.retrieve(
            &point_ids,
            with_payload.map(WithPayloadInterface::from),
            with_vector.map(WithVector::from),
        )?;
        let points = PyRecord::wrap_vec(points);
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
