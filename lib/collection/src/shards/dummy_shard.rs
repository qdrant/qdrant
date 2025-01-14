use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::tar_ext;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::order_by::OrderBy;
use segment::index::field_index::CardinalityEstimation;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, SnapshotFormat, WithPayload, WithPayloadInterface,
    WithVector,
};
use tokio::runtime::Handle;

use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CoreSearchRequestBatch,
    CountRequestInternal, CountResult, PointRequestInternal, RecordInternal, ShardStatus,
    UpdateResult,
};
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::OperationWithClockTag;
use crate::shards::shard_trait::ShardOperation;
use crate::shards::telemetry::LocalShardTelemetry;

#[derive(Clone, Debug)]
pub struct DummyShard {
    message: String,
}

impl DummyShard {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub async fn create_snapshot(
        &self,
        _temp_path: &Path,
        _tar: &tar_ext::BuilderExt,
        _format: SnapshotFormat,
        _save_wal: bool,
    ) -> CollectionResult<()> {
        self.dummy()
    }

    pub async fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        self.dummy()
    }

    pub async fn on_strict_mode_config_update(&mut self) {}

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        LocalShardTelemetry {
            variant_name: Some("dummy shard".into()),
            status: Some(ShardStatus::Green),
            total_optimized_points: 0,
            segments: vec![],
            optimizations: Default::default(),
            async_scorer: None,
        }
    }

    pub fn estimate_cardinality(
        &self,
        _: Option<&Filter>,
    ) -> CollectionResult<CardinalityEstimation> {
        self.dummy()
    }

    fn dummy<T>(&self) -> CollectionResult<T> {
        Err(CollectionError::service_error(self.message.to_string()))
    }
}

#[async_trait]
impl ShardOperation for DummyShard {
    async fn update(&self, _: OperationWithClockTag, _: bool) -> CollectionResult<UpdateResult> {
        self.dummy()
    }

    /// Forward read-only `scroll_by` to `wrapped_shard`
    async fn scroll_by(
        &self,
        _: Option<ExtendedPointId>,
        _: usize,
        _: &WithPayloadInterface,
        _: &WithVector,
        _: Option<&Filter>,
        _: &Handle,
        _: Option<&OrderBy>,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.dummy()
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        self.dummy()
    }

    async fn core_search(
        &self,
        _: Arc<CoreSearchRequestBatch>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.dummy()
    }

    async fn count(
        &self,
        _: Arc<CountRequestInternal>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        self.dummy()
    }

    async fn retrieve(
        &self,
        _: Arc<PointRequestInternal>,
        _: &WithPayload,
        _: &WithVector,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.dummy()
    }

    async fn query_batch(
        &self,
        _requests: Arc<Vec<ShardQueryRequest>>,
        _search_runtime_handle: &Handle,
        _timeout: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        self.dummy()
    }

    async fn facet(
        &self,
        _: Arc<FacetParams>,
        _search_runtime_handle: &Handle,
        _: Option<Duration>,
    ) -> CollectionResult<FacetResponse> {
        self.dummy()
    }
}
