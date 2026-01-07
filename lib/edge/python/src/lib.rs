pub mod config;
pub mod query;
pub mod repr;
pub mod scroll;
pub mod search;
pub mod snapshots;
pub mod types;
pub mod update;
pub mod utils;

use std::path::PathBuf;

use bytemuck::TransparentWrapperAlloc as _;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use segment::common::operation_error::OperationError;
use segment::types::*;

use self::config::*;
use self::query::*;
use self::scroll::*;
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
    use super::scroll::PyScrollRequest;
    #[pymodule_export]
    use super::search::{
        PyAcornSearchParams, PyQuantizationSearchParams, PySearchParams, PySearchRequest,
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
        PyContextPair, PyContextQuery, PyDiscoverQuery, PyFeedbackItem, PyFeedbackNaiveQuery,
        PyNaiveFeedbackCoefficients, PyPayloadSelectorInterface, PyQueryInterface,
        PyRecommendQuery,
    };
    #[pymodule_export]
    use super::types::{PyPoint, PyPointVectors, PyRecord, PyScoredPoint, PySparseVector};
    #[pymodule_export]
    use super::update::PyUpdateOperation;
}

#[pyclass(name = "Shard")]
#[derive(Debug)]
pub struct PyShard(Option<edge::Shard>);

#[pymethods]
impl PyShard {
    #[new]
    pub fn load(path: PathBuf, config: Option<PySegmentConfig>) -> Result<Self> {
        let shard = edge::Shard::load(&path, config.map(SegmentConfig::from))?;
        Ok(Self(Some(shard)))
    }

    pub fn update(&self, operation: PyUpdateOperation) -> Result<()> {
        self.get_shard()?.update(operation.into())?;
        Ok(())
    }

    pub fn query(&self, query: PyQueryRequest) -> Result<Vec<PyScoredPoint>> {
        let points = self.get_shard()?.query(query.into())?;
        let points = PyScoredPoint::wrap_vec(points);
        Ok(points)
    }

    pub fn search(&self, search: PySearchRequest) -> Result<Vec<PyScoredPoint>> {
        let points = self.get_shard()?.search(search.into())?;
        let points = PyScoredPoint::wrap_vec(points);
        Ok(points)
    }

    pub fn scroll(&self, scroll: PyScrollRequest) -> Result<Vec<PyRecord>> {
        let points = self.get_shard()?.scroll(scroll.into())?;
        let points = PyRecord::wrap_vec(points);
        Ok(points)
    }

    pub fn retrieve(
        &self,
        point_ids: Vec<PyPointId>,
        with_payload: Option<PyWithPayload>,
        with_vector: Option<PyWithVector>,
    ) -> Result<Vec<PyRecord>> {
        let point_ids = PyPointId::peel_vec(point_ids);
        let points = self.get_shard()?.retrieve(
            &point_ids,
            with_payload.map(WithPayloadInterface::from),
            with_vector.map(WithVector::from),
        )?;
        let points = PyRecord::wrap_vec(points);
        Ok(points)
    }

    // ------- Snapshot related methods -------

    #[staticmethod]
    pub fn unpack_snapshot(snapshot_path: PathBuf, target_path: PathBuf) -> Result<()> {
        edge::Shard::unpack_snapshot(&snapshot_path, &target_path)?;
        Ok(())
    }

    pub fn snapshot_manifest(&self) -> Result<PyValue> {
        let manifest = self.get_shard()?.snapshot_manifest()?;
        Ok(PyValue::new(serde_json::to_value(&manifest).unwrap()))
    }

    #[pyo3(signature = (snapshot_path, tmp_dir=None))]
    pub fn update_from_snapshot(
        &mut self,
        snapshot_path: PathBuf,
        tmp_dir: Option<PathBuf>,
    ) -> Result<()> {
        self._update_from_snapshot(snapshot_path, tmp_dir)?;
        Ok(())
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
