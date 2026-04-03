use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::DeferredBehavior;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::index::field_index::CardinalityEstimation;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, SizeStats, WithPayload, WithPayloadInterface, WithVector,
};
use shard::count::CountRequestInternal;
use shard::operations::CollectionUpdateOperations;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequestBatch;
use shard::snapshots::snapshot_manifest::SnapshotManifest;
use tokio::runtime::Handle;

use crate::operations::OperationWithClockTag;
use crate::operations::types::{
    CollectionError, CollectionInfo, CollectionResult, CountResult, OptimizersStatus,
    PointRequestInternal, ShardStatus, UpdateResult, UpdateStatus,
};
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
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

    pub fn snapshot_manifest(&self) -> CollectionResult<SnapshotManifest> {
        Ok(SnapshotManifest::default())
    }

    pub fn on_optimizer_config_update(&self) -> CollectionResult<()> {
        let error = self.dummy_error("Update optimizer config");
        log::error!("{error}");
        // We can't fail this operation because this operation is part of consensus loop
        Ok(())
    }

    pub fn on_strict_mode_config_update(&mut self) {}

    pub fn get_telemetry_data(&self) -> LocalShardTelemetry {
        LocalShardTelemetry {
            variant_name: Some("dummy shard".into()),
            status: Some(ShardStatus::Green),
            total_optimized_points: 0,
            vectors_size_bytes: None,
            payloads_size_bytes: None,
            num_points: None,
            num_vectors: None,
            num_vectors_by_name: None,
            segments: None,
            optimizations: Default::default(),
            async_scorer: None,
            indexed_only_excluded_vectors: None,
            update_queue: None,
        }
    }

    pub fn get_optimization_status(&self) -> OptimizersStatus {
        OptimizersStatus::Ok
    }

    pub fn get_size_stats(&self) -> SizeStats {
        SizeStats::default()
    }

    pub fn estimate_cardinality(
        &self,
        _: Option<&Filter>,
    ) -> CollectionResult<CardinalityEstimation> {
        self.dummy("estimate_cardinality")
    }

    pub fn dummy_error(&self, action: &str) -> CollectionError {
        CollectionError::service_error(format!("Failed to {action},  {}", self.message))
    }

    fn dummy<T>(&self, action: &str) -> CollectionResult<T> {
        Err(self.dummy_error(action))
    }
}

#[async_trait]
impl ShardOperation for DummyShard {
    async fn update(
        &self,
        op: OperationWithClockTag,
        _: WaitUntil,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult> {
        match &op.operation {
            CollectionUpdateOperations::PointOperation(_) => self.dummy("Update Points"),
            CollectionUpdateOperations::VectorOperation(_) => self.dummy("Update Vectors"),
            CollectionUpdateOperations::PayloadOperation(_) => self.dummy("Update Payloads"),

            // Allow (and ignore) field index operations. Field index schema is stored in collection
            // config, and indices will be created (if needed) when dummy shard is recovered.
            CollectionUpdateOperations::FieldIndexOperation(_) => Ok(UpdateResult {
                operation_id: None,
                status: UpdateStatus::Acknowledged,
                clock_tag: None,
            }),
            // Allow (and ignore) staging operations on dummy shards
            #[cfg(feature = "staging")]
            CollectionUpdateOperations::StagingOperation(_) => Ok(UpdateResult {
                operation_id: None,
                status: UpdateStatus::Acknowledged,
                clock_tag: None,
            }),
        }
    }

    /// Forward read-only `scroll_by` to `wrapped_shard`
    async fn scroll_by(
        &self,
        _: Arc<ScrollRequestInternal>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.dummy("Scroll")
    }

    async fn local_scroll_by_id(
        &self,
        _: Option<ExtendedPointId>,
        _: usize,
        _: &WithPayloadInterface,
        _: &WithVector,
        _: Option<&Filter>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
        _: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.dummy("Scroll by ID")
    }

    async fn info(&self) -> CollectionResult<CollectionInfo> {
        self.dummy("Get Info")
    }

    async fn core_search(
        &self,
        _: Arc<CoreSearchRequestBatch>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.dummy("search")
    }

    async fn count(
        &self,
        _: Arc<CountRequestInternal>,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
        _: DeferredBehavior,
    ) -> CollectionResult<CountResult> {
        self.dummy("count")
    }

    async fn retrieve(
        &self,
        _: Arc<PointRequestInternal>,
        _: &WithPayload,
        _: &WithVector,
        _: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
        _: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>> {
        self.dummy("retrieve")
    }

    async fn query_batch(
        &self,
        _requests: Arc<Vec<ShardQueryRequest>>,
        _search_runtime_handle: &Handle,
        _timeout: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        self.dummy("query")
    }

    async fn facet(
        &self,
        _: Arc<FacetParams>,
        _search_runtime_handle: &Handle,
        _: Option<Duration>,
        _: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse> {
        self.dummy("facet")
    }

    async fn stop_gracefully(self) {}
}
