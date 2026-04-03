use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::types::DeferredBehavior;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::types::*;
use shard::count::CountRequestInternal;
use shard::retrieve::record_internal::RecordInternal;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequestBatch;
use tokio::runtime::Handle;

use crate::operations::OperationWithClockTag;
use crate::operations::types::*;
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};

/// Controls how an update operation waits for completion.
///
/// Internal enum derived from `wait=true/false` as specified by a user request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitUntil {
    /// Wait until the operation is written in WAL.
    ///
    /// Corresponds with wait=false, acknowledging immediately.
    Wal,
    /// Wait until the operation is written in a segment.
    ///
    /// This does not mean the operation will be visible. If deferred points are enabled it may
    /// take some time for the change to appear.
    Segment,
    /// Wait until the operation is visible in search results.
    ///
    /// Corresponds with wait=true, acknowledging only when the change is fully applied and visible.
    Visible,
}

impl WaitUntil {
    /// Whether this behavior requires creating a callback to wait for the operation.
    pub fn needs_callback(&self) -> bool {
        match self {
            WaitUntil::Segment | WaitUntil::Visible => true,
            WaitUntil::Wal => false,
        }
    }

    /// Whether this behavior requires waiting for deferred points to be optimized.
    pub fn wait_for_deferred(&self) -> bool {
        match self {
            WaitUntil::Visible => true,
            WaitUntil::Wal | WaitUntil::Segment => false,
        }
    }
}

impl From<bool> for WaitUntil {
    fn from(wait: bool) -> Self {
        if wait {
            WaitUntil::Visible
        } else {
            WaitUntil::Wal
        }
    }
}

impl From<api::grpc::qdrant::WaitUntil> for WaitUntil {
    fn from(value: api::grpc::qdrant::WaitUntil) -> Self {
        match value {
            api::grpc::qdrant::WaitUntil::Wal => WaitUntil::Wal,
            api::grpc::qdrant::WaitUntil::Segment => WaitUntil::Segment,
            api::grpc::qdrant::WaitUntil::Visible => WaitUntil::Visible,
        }
    }
}

impl From<WaitUntil> for api::grpc::qdrant::WaitUntil {
    fn from(value: WaitUntil) -> Self {
        match value {
            WaitUntil::Wal => api::grpc::qdrant::WaitUntil::Wal,
            WaitUntil::Segment => api::grpc::qdrant::WaitUntil::Segment,
            WaitUntil::Visible => api::grpc::qdrant::WaitUntil::Visible,
        }
    }
}

#[async_trait]
pub trait ShardOperation {
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: WaitUntil,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<UpdateResult>;

    async fn scroll_by(
        &self,
        request: Arc<ScrollRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<RecordInternal>>;

    /// Scroll points ordered by their IDs.
    /// Intended for internal use only.
    /// This API is excluded from the rate limits and logging.
    #[allow(clippy::too_many_arguments)]
    async fn local_scroll_by_id(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>>;

    async fn info(&self) -> CollectionResult<CollectionInfo>;

    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>;

    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<CountResult>;

    #[allow(clippy::too_many_arguments)]
    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hardware_accumulator: HwMeasurementAcc,
        deferred_behavior: DeferredBehavior,
    ) -> CollectionResult<Vec<RecordInternal>>;

    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>>;

    async fn facet(
        &self,
        request: Arc<FacetParams>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<FacetResponse>;

    /// Signal `Stop` to all background operations gracefully
    /// and wait till they are finished.
    async fn stop_gracefully(self);
}

pub type ShardOperationSS = dyn ShardOperation + Send + Sync;
