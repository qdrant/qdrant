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
/// This enum is internal and `Segment` must not cross the API boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitBehavior {
    /// Wait for the operation to be fully applied, including deferred points optimization.
    Wait,
    /// Don't wait at all, return immediately after queuing.
    NoWait,
    /// Wait for the operation to land in a segment (via operation callback), but don't wait for
    /// deferred points to be optimized. This variant is internal only and must not cross the API
    /// boundary.
    Segment,
}

impl WaitBehavior {
    /// Whether this behavior requires creating a callback to wait for the operation.
    pub fn needs_callback(&self) -> bool {
        matches!(self, WaitBehavior::Wait | WaitBehavior::Segment)
    }

    /// Whether this behavior requires waiting for deferred points to be optimized.
    pub fn wait_for_deferred(&self) -> bool {
        matches!(self, WaitBehavior::Wait)
    }
}

impl From<bool> for WaitBehavior {
    fn from(wait: bool) -> Self {
        if wait {
            WaitBehavior::Wait
        } else {
            WaitBehavior::NoWait
        }
    }
}

#[async_trait]
pub trait ShardOperation {
    async fn update(
        &self,
        operation: OperationWithClockTag,
        wait: WaitBehavior,
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
