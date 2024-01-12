use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use segment::types::{
    ExtendedPointId, Filter, ScoredPoint, WithPayload, WithPayloadInterface, WithVector,
};
use tokio::runtime::Handle;

use crate::operations::types::{
    CollectionInfo, CollectionResult, CoreSearchRequestBatch, CountRequestInternal, CountResult,
    PointRequestInternal, Record, UpdateResult,
};
use crate::operations::CollectionUpdateOperations;

#[async_trait]
pub trait ShardOperation {
    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult>;

    #[allow(clippy::too_many_arguments)]
    async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        search_runtime_handle: &Handle,
    ) -> CollectionResult<Vec<Record>>;

    async fn info(&self) -> CollectionResult<CollectionInfo>;

    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>;

    async fn count(&self, request: Arc<CountRequestInternal>) -> CollectionResult<CountResult>;

    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
    ) -> CollectionResult<Vec<Record>>;
}

pub type ShardOperationSS = dyn ShardOperation + Send + Sync;
