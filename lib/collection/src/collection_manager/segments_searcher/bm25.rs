use std::sync::Arc;
use std::time::{Duration, Instant};

use common::ambient::hw;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use segment::common::operation_error::OperationError;
use segment::data_types::query_context::QueryContext;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::json_path::JsonPath;
use segment::types::{Filter, ScoredPoint, WithPayload, WithVector};
use shard::common::stopping_guard::StoppingGuard;
use shard::query::text::init_text_query_context;
use shard::search_result_aggregator::BatchResultAggregator;
use shard::segment_holder::locked::LockedSegmentHolder;
use tokio_util::task::AbortOnDropHandle;

use super::SegmentsSearcher;
use crate::common::adaptive_handle::AdaptiveSearchHandle;
use crate::operations::types::{CollectionError, CollectionResult};

impl SegmentsSearcher {
    /// Rank the shard's points by BM25 over the text index of `field` and
    /// return the `limit` best, highest first.
    ///
    /// `terms` must already be tokenized by the field's tokenizer. The text
    /// statistics are gathered over every segment first, among the points
    /// matching `idf_corpus` (all of them when `None`), so a point scores the
    /// same whichever segment holds it; then each segment returns its own
    /// `limit` best, and a point held by several segments keeps only its
    /// highest version. No sampling: every segment is asked for the full
    /// `limit`.
    #[allow(clippy::too_many_arguments)]
    pub async fn score_bm25(
        segments: LockedSegmentHolder,
        field: JsonPath,
        terms: Vec<String>,
        params: Bm25Params,
        filter: Option<Filter>,
        idf_corpus: Option<&Filter>,
        limit: usize,
        with_payload: WithPayload,
        with_vector: WithVector,
        runtime_handle: &AdaptiveSearchHandle,
        is_stopped_guard: &StoppingGuard,
        timeout: Duration,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let start = Instant::now();

        let query_context = init_text_query_context(
            &field,
            idf_corpus,
            &terms,
            is_stopped_guard.get_is_stopped(),
        );
        let query_context = Self::fill_query_context(
            query_context,
            segments.clone(),
            runtime_handle,
            is_stopped_guard,
            timeout,
        )
        .await?;
        let Some(query_context) = query_context else {
            return Ok(Vec::new());
        };

        let segments: Vec<_> = {
            let timeout = timeout.saturating_sub(start.elapsed());
            let Some(segments_guard) = segments.try_read_for(timeout) else {
                return Err(CollectionError::timeout(timeout, "score_bm25"));
            };
            // Collect the segments first so we don't lock the segment holder during the operations.
            segments_guard
                .non_appendable_then_appendable_segments()
                .collect()
        };

        let query = Arc::new(SegmentBm25Query {
            query_context,
            field,
            terms,
            filter,
            with_payload,
            with_vector,
        });
        let mut futures = segments
            .into_iter()
            .map(|segment| {
                let query = query.clone();
                let timeout = timeout.saturating_sub(start.elapsed());
                let cpu_utilization = hw::cpu_utilization();
                let handle = runtime_handle.spawn_blocking(move || {
                    cpu_utilization.measure(|| {
                        let SegmentBm25Query {
                            query_context,
                            field,
                            terms,
                            filter,
                            with_payload,
                            with_vector,
                        } = &*query;
                        let segment = segment.get();
                        let Some(read_segment) = segment.try_read_for(timeout) else {
                            return Err(CollectionError::timeout(timeout, "score_bm25"));
                        };
                        let segment_query_context = query_context.get_segment_query_context();
                        Ok(read_segment.score_bm25(
                            field,
                            terms,
                            params,
                            with_payload,
                            with_vector,
                            filter.as_ref(),
                            limit,
                            &segment_query_context,
                        )?)
                    })
                });
                AbortOnDropHandle::new(handle)
            })
            .collect::<FuturesUnordered<_>>();

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

/// What every segment's BM25 task shares, behind one `Arc`.
struct SegmentBm25Query {
    query_context: QueryContext,
    field: JsonPath,
    terms: Vec<String>,
    filter: Option<Filter>,
    with_payload: WithPayload,
    with_vector: WithVector,
}
