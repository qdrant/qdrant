use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{WithPayloadInterface, WithVector};
use shard::common::stopping_guard::StoppingGuard;
use shard::scroll::ScrollRequestInternal;
use tokio::time::Instant;
use tokio_util::task::AbortOnDropHandle;
use validator::Validate;

use super::Collection;
use crate::operations::block_hashes::{
    BlockHashAccumulator, BlockHashesRequest, BlockHashesResponse,
};
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::routing::RoutingToken;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{CollectionError, CollectionResult};

impl Collection {
    /// Live, read-only audit. No state survives cancellation or failure, and only a
    /// completed scan can produce a response. Existing scroll provides the global
    /// ID order and logical point/version resolution across segments and shards.
    pub async fn block_hashes(
        &self,
        request: BlockHashesRequest,
        read_consistency: Option<ReadConsistency>,
        routing_token: Option<RoutingToken>,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<BlockHashesResponse> {
        request
            .validate()
            .map_err(|err| CollectionError::bad_request(err.to_string()))?;
        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);
        let deadline = Instant::now() + timeout;
        let stopping_guard = StoppingGuard::new();
        let scan = async {
            let mut accumulator = BlockHashAccumulator::new(request.block_count);
            let mut offset = None;
            loop {
                let page = self
                    .scroll_by(
                        ScrollRequestInternal {
                            offset,
                            limit: Some(1024),
                            filter: request.filter.clone(),
                            with_payload: Some(WithPayloadInterface::Fields(vec![
                                request.payload_key.clone(),
                            ])),
                            with_vector: WithVector::Bool(false),
                            order_by: None,
                        },
                        read_consistency,
                        routing_token,
                        &ShardSelectorInternal::All,
                        Some(deadline.saturating_duration_since(Instant::now())),
                        hw_measurement_acc.clone(),
                    )
                    .await?;
                offset = page.next_page_offset;
                let key = request.payload_key.clone();
                let stopped = stopping_guard.get_is_stopped();
                let task = self.search_runtime.spawn_blocking(move || {
                    for point in page.points {
                        accumulator.add(point.id, point.payload.as_ref(), &key, &stopped)?;
                    }
                    CollectionResult::Ok(accumulator)
                });
                accumulator = AbortOnDropHandle::new(task).await??;
                if offset.is_none() {
                    break;
                }
            }
            let stopped = stopping_guard.get_is_stopped();
            let task = self
                .search_runtime
                .spawn_blocking(move || accumulator.finish(&stopped));
            AbortOnDropHandle::new(task).await?
        };
        let response = tokio::time::timeout_at(deadline, scan)
            .await
            .map_err(|_| CollectionError::timeout(timeout, "block hashes"))??;
        // timeout_at can finish an immediately-ready future after its deadline.
        if Instant::now() >= deadline {
            return Err(CollectionError::timeout(timeout, "block hashes"));
        }
        Ok(response)
    }
}
