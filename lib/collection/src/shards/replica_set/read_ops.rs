use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use segment::data_types::order_by::OrderBy;
use segment::types::*;

use super::ShardReplicaSet;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::*;
use crate::operations::universal_query::shard_query::ShardQueryRequest;

impl ShardReplicaSet {
    #[allow(clippy::too_many_arguments)]
    pub async fn scroll_by(
        &self,
        offset: Option<ExtendedPointId>,
        limit: usize,
        with_payload_interface: &WithPayloadInterface,
        with_vector: &WithVector,
        filter: Option<&Filter>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
        order_by: Option<&OrderBy>,
    ) -> CollectionResult<Vec<Record>> {
        let with_payload_interface = Arc::new(with_payload_interface.clone());
        let with_vector = Arc::new(with_vector.clone());
        let filter = filter.map(|filter| Arc::new(filter.clone()));
        let order_by = order_by.map(|order_by| Arc::new(order_by.clone()));

        self.execute_and_resolve_read_operation(
            |shard| {
                let with_payload_interface = with_payload_interface.clone();
                let with_vector = with_vector.clone();
                let filter = filter.clone();
                let search_runtime = self.search_runtime.clone();
                let order_by = order_by.clone();

                async move {
                    shard
                        .scroll_by(
                            offset,
                            limit,
                            &with_payload_interface,
                            &with_vector,
                            filter.as_deref(),
                            &search_runtime,
                            order_by.as_deref(),
                        )
                        .await
                }
                .boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }

    pub async fn core_search(
        &self,
        request: Arc<CoreSearchRequestBatch>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = Arc::clone(&request);
                let search_runtime = self.search_runtime.clone();

                async move { shard.core_search(request, &search_runtime, timeout).await }.boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }

    pub async fn count(
        &self,
        request: Arc<CountRequestInternal>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
    ) -> CollectionResult<CountResult> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = request.clone();
                async move { shard.count(request).await }.boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }

    pub async fn retrieve(
        &self,
        request: Arc<PointRequestInternal>,
        with_payload: &WithPayload,
        with_vector: &WithVector,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
    ) -> CollectionResult<Vec<Record>> {
        let with_payload = Arc::new(with_payload.clone());
        let with_vector = Arc::new(with_vector.clone());

        self.execute_and_resolve_read_operation(
            |shard| {
                let request = request.clone();
                let with_payload = with_payload.clone();
                let with_vector = with_vector.clone();

                async move { shard.retrieve(request, &with_payload, &with_vector).await }.boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }

    pub async fn info(&self, local_only: bool) -> CollectionResult<CollectionInfo> {
        self.execute_read_operation(
            |shard| async move { shard.info().await }.boxed(),
            local_only,
        )
        .await
    }

    pub async fn count_local(
        &self,
        request: Arc<CountRequestInternal>,
    ) -> CollectionResult<Option<CountResult>> {
        let local = self.local.read().await;
        match &*local {
            None => Ok(None),
            Some(shard) => Ok(Some(shard.get().count(request).await?)),
        }
    }

    pub async fn query(
        &self,
        request: Arc<ShardQueryRequest>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = Arc::clone(&request);
                let search_runtime = self.search_runtime.clone();

                async move { shard.query(request, &search_runtime).await }.boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }
}
