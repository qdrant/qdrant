use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use segment::data_types::facets::{FacetRequestInternal, FacetResponse};
use segment::data_types::order_by::OrderBy;
use segment::types::*;
use tokio::runtime::Handle;

use crate::operations::types::*;
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};
use crate::operations::OperationWithClockTag;

#[async_trait]
pub trait ShardOperation {
    async fn update(
        &self,
        operation: OperationWithClockTag,
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
        order_by: Option<&OrderBy>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Record>>;

    async fn info(&self) -> CollectionResult<CollectionInfo>;

    async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>>;

    async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<CountResult>;

    async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Record>>;

    async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ShardQueryResponse>>;

    async fn facet(
        &self,
        request: Arc<FacetRequestInternal>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<FacetResponse>;
}

pub type ShardOperationSS = dyn ShardOperation + Send + Sync;
