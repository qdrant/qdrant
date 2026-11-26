use std::sync::Arc;
use std::time::Duration;

use common::ambient;
use common::ambient::hw;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::common::operation_error::OperationError;
use segment::data_types::query_context::FormulaContext;
use segment::types::ScoredPoint;
use shard::search_result_aggregator::BatchResultAggregator;
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio_util::task::AbortOnDropHandle;

use super::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{CollectionError, CollectionResult};

impl SegmentsSearcher {
    /// Rescore results with a formula that can reference payload values.
    ///
    /// Aggregates rescores from the segments.
    pub async fn rescore_with_formula(
        segments: LockedSegmentHolder,
        arc_ctx: Arc<FormulaContext>,
        runtime_handle: &AdaptiveSearchHandle,
        timeout: Duration,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let limit = arc_ctx.limit;

        let mut futures = {
            let segments: Vec<_> = {
                let Some(segments_guard) = segments.try_read_for(timeout) else {
                    return Err(CollectionError::timeout(timeout, "rescore_with_formula"));
                };
                // Collect the segments first so we don't lock the segment holder during the operations.
                segments_guard
                    .non_appendable_then_appendable_segments()
                    .collect()
            };

            segments
                .into_iter()
                .map(|segment| {
                    let handle = runtime_handle.spawn_blocking({
                        let arc_ctx = arc_ctx.clone();
                        let handoff = ambient::current();
                        let cpu_utilization = hw::cpu_utilization();
                        move || {
                            let _scope = handoff.enter_guard();
                            cpu_utilization
                                .measure(|| segment.get().read().rescore_with_formula(arc_ctx))
                        }
                    });
                    AbortOnDropHandle::new(handle)
                })
                .collect::<FuturesUnordered<_>>()
        };

        let mut segments_results = Vec::with_capacity(futures.len());
        while let Some(result) = futures.try_next().await? {
            segments_results.push(result?)
        }

        // use aggregator with only one "batch"
        let mut aggregator = BatchResultAggregator::new(std::iter::once(limit));
        aggregator.update_point_versions(segments_results.iter().flatten());
        aggregator.update_batch_results(0, segments_results.into_iter().flatten());
        let top =
            aggregator.into_topk().into_iter().next().ok_or_else(|| {
                OperationError::service_error("expected first result of aggregator")
            })?;

        Ok(top)
    }
}
