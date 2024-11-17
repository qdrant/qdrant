use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use futures::FutureExt as _;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::data_types::order_by::OrderBy;
use segment::types::*;

use super::ShardReplicaSet;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::*;
use crate::operations::universal_query::shard_query::{ShardQueryRequest, ShardQueryResponse};

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
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<RecordInternal>> {
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
                            timeout,
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
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = Arc::clone(&request);
                let search_runtime = self.search_runtime.clone();
                let hardware_collector = hw_measurement_acc.new_collector();
                async move {
                    shard
                        .core_search(request, &search_runtime, timeout, &hardware_collector)
                        .await
                }
                .boxed()
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
        timeout: Option<Duration>,
        local_only: bool,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<CountResult> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = request.clone();
                let search_runtime = self.search_runtime.clone();

                let hw_collector = hw_measurement_acc.new_collector();
                async move {
                    shard
                        .count(request, &search_runtime, timeout, &hw_collector)
                        .await
                }
                .boxed()
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
        timeout: Option<Duration>,
        local_only: bool,
    ) -> CollectionResult<Vec<RecordInternal>> {
        let with_payload = Arc::new(with_payload.clone());
        let with_vector = Arc::new(with_vector.clone());

        self.execute_and_resolve_read_operation(
            |shard| {
                let request = request.clone();
                let with_payload = with_payload.clone();
                let with_vector = with_vector.clone();
                let search_runtime = self.search_runtime.clone();

                async move {
                    shard
                        .retrieve(
                            request,
                            &with_payload,
                            &with_vector,
                            &search_runtime,
                            timeout,
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
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<Option<CountResult>> {
        let local = self.local.read().await;
        match &*local {
            None => Ok(None),
            Some(shard) => {
                let search_runtime = self.search_runtime.clone();
                Ok(Some(
                    shard
                        .get()
                        .count(request, &search_runtime, timeout, hw_measurement_acc)
                        .await?,
                ))
            }
        }
    }

    pub async fn query_batch(
        &self,
        requests: Arc<Vec<ShardQueryRequest>>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
        timeout: Option<Duration>,
        hw_measurement_acc: &HwMeasurementAcc,
    ) -> CollectionResult<Vec<ShardQueryResponse>> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let requests = Arc::clone(&requests);
                let search_runtime = self.search_runtime.clone();
                let hw_collector = hw_measurement_acc.new_collector();
                async move {
                    shard
                        .query_batch(requests, &search_runtime, timeout, &hw_collector)
                        .await
                }
                .boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }

    pub async fn facet(
        &self,
        request: Arc<FacetParams>,
        read_consistency: Option<ReadConsistency>,
        local_only: bool,
        timeout: Option<Duration>,
    ) -> CollectionResult<FacetResponse> {
        self.execute_and_resolve_read_operation(
            |shard| {
                let request = request.clone();
                let search_runtime = self.search_runtime.clone();

                async move { shard.facet(request, &search_runtime, timeout).await }.boxed()
            },
            read_consistency,
            local_only,
        )
        .await
    }
}
