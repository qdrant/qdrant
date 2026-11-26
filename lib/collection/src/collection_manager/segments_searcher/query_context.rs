use std::time::{Duration, Instant};

use common::ambient::hw;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::common::operation_error::OperationError;
use segment::data_types::modifier::Modifier;
use segment::data_types::query_context::QueryContext;
use shard::common::stopping_guard::StoppingGuard;
use shard::optimizers::config::DEFAULT_INDEXING_THRESHOLD_KB;
use shard::query::query_context::{fill_segment_query_context, init_query_context};
use shard::search::CoreSearchRequestBatch;
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio_util::task::AbortOnDropHandle;

use super::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::config::CollectionConfigInternal;
use crate::operations::types::{CollectionError, CollectionResult};

impl SegmentsSearcher {
    pub async fn prepare_query_context(
        segments: LockedSegmentHolder,
        batch_request: &CoreSearchRequestBatch,
        collection_config: &CollectionConfigInternal,
        timeout: Duration,
        search_runtime_handle: &AdaptiveSearchHandle,
        is_stopped_guard: &StoppingGuard,
    ) -> CollectionResult<Option<QueryContext>> {
        let indexing_threshold_kb = collection_config
            .optimizer_config
            .indexing_threshold
            .unwrap_or(DEFAULT_INDEXING_THRESHOLD_KB);
        let full_scan_threshold_kb = collection_config.hnsw_config.full_scan_threshold;
        let search_optimized_threshold_kb = indexing_threshold_kb.max(full_scan_threshold_kb);

        let query_context = init_query_context(
            &batch_request.searches,
            search_optimized_threshold_kb,
            is_stopped_guard,
            |vector_name| {
                collection_config
                    .params
                    .get_sparse_vector_params_opt(vector_name)
                    .map(|params| params.modifier == Some(Modifier::Idf))
                    .unwrap_or(false)
            },
        )?;
        Self::fill_query_context(
            query_context,
            segments,
            search_runtime_handle,
            is_stopped_guard,
            timeout,
        )
        .await
    }

    /// Fill `query_context` with the statistics of every segment, one blocking
    /// task per segment when there are IDF or text statistics to collect, a
    /// single sequential task otherwise. Returns `None` when there are no segments.
    pub(super) async fn fill_query_context(
        mut query_context: QueryContext,
        segments: LockedSegmentHolder,
        runtime_handle: &AdaptiveSearchHandle,
        is_stopped_guard: &StoppingGuard,
        timeout: Duration,
    ) -> CollectionResult<Option<QueryContext>> {
        let start = Instant::now();

        let segments: Vec<_> = {
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "fill query context"));
            };
            // Collect the segments first so we don't lock the segment holder during the operations.
            segments_guard
                .non_appendable_then_appendable_segments()
                .collect()
        };

        if segments.is_empty() {
            return Ok(None);
        }

        let cpu_utilization = hw::cpu_utilization();

        // Only the available point count is gathered, too little work per
        // segment to pay for a task each.
        if !query_context.has_statistics_to_collect() {
            let is_stopped = is_stopped_guard.get_is_stopped();
            // Do blocking calls in a blocking task: `segment.get().read()` calls might block async runtime
            let handle = runtime_handle.spawn_blocking(move || {
                cpu_utilization.measure(|| {
                    for segment in &segments {
                        let timeout = timeout.saturating_sub(start.elapsed());
                        fill_segment_query_context(
                            &mut query_context,
                            segment,
                            timeout,
                            &is_stopped,
                        )?;
                    }
                    Ok::<_, OperationError>(query_context)
                })
            });
            return Ok(Some(AbortOnDropHandle::new(handle).await??));
        }

        // Every count is still zero, so each clone carries only its own segment.
        debug_assert_eq!(query_context.available_point_count(), 0);
        let mut forks = segments
            .into_iter()
            .map(|segment| {
                let mut fork = query_context.clone();
                let is_stopped = is_stopped_guard.get_is_stopped();
                let timeout = timeout.saturating_sub(start.elapsed());
                let cpu_utilization = cpu_utilization.clone();
                // Do blocking calls in a blocking task: `segment.get().read()` calls might block async runtime
                let handle = runtime_handle.spawn_blocking(move || {
                    cpu_utilization.measure(|| {
                        fill_segment_query_context(&mut fork, &segment, timeout, &is_stopped)?;
                        Ok::<_, OperationError>(fork)
                    })
                });
                AbortOnDropHandle::new(handle)
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(fork) = forks.try_next().await? {
            query_context.merge_statistics(fork?);
        }
        Ok(Some(query_context))
    }
}
