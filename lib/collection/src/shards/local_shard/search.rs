use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::ScoredPoint;
use shard::common::stopping_guard::StoppingGuard;
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequestBatch;
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{CollectionError, CollectionResult};

// Chunk requests for parallelism in certain scenarios
//
// Deeper down, each segment gets its own dedicated search thread. If this shard has just
// one segment, all requests will be executed on a single thread.
//
// To prevent this from being a bottleneck if we have a lot of requests, we can chunk the
// requests into multiple searches to allow more parallelism.
//
// For simplicity, we use a fixed chunk size. Using chunks helps to ensure our 'filter
// reuse optimization' is still properly utilized.
// See: <https://github.com/qdrant/qdrant/pull/813>
// See: <https://github.com/qdrant/qdrant/pull/6326>
const CHUNK_SIZE: usize = 16;

impl LocalShard {
    pub async fn do_search(
        &self,
        core_request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_counter_acc: HwMeasurementAcc,
        spike_handle: Option<common::spike_profiler::SpikeProfilerHandle>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        if core_request.searches.is_empty() {
            return Ok(vec![]);
        }

        let skip_batching = if core_request.searches.len() <= CHUNK_SIZE {
            // Don't batch if we have few searches, prevents cloning request
            true
        } else if self.segments.read().len() > self.shared_storage_config.search_thread_count {
            // Don't batch if we have more segments than search threads
            // Not a perfect condition, but it helps to prevent consuming a lot of search threads
            // if the number of segments is large
            // Note: search threads are shared with all other search threads on this Qdrant
            // instance, and other shards also have segments. For simplicity this only considers
            // the global search thread count and local segment count.
            // See: <https://github.com/qdrant/qdrant/pull/6478>
            true
        } else {
            false
        };

        let is_stopped_guard = StoppingGuard::new();

        if skip_batching {
            let _await_guard = spike_handle
                .as_ref()
                .map(|h| h.await_section("do_search_impl"));
            let result = self
                .do_search_impl(
                    core_request,
                    search_runtime_handle,
                    timeout,
                    hw_counter_acc,
                    &is_stopped_guard,
                    spike_handle.clone(),
                )
                .await;
            drop(_await_guard);
            return result;
        }

        // Batch if we have many searches, allows for more parallelism
        let CoreSearchRequestBatch { searches } = core_request.as_ref();

        let chunk_futures = searches
            .chunks(CHUNK_SIZE)
            .map(|chunk| {
                let core_request = CoreSearchRequestBatch {
                    searches: chunk.to_vec(),
                };
                self.do_search_impl(
                    Arc::new(core_request),
                    search_runtime_handle,
                    timeout,
                    hw_counter_acc.clone(),
                    &is_stopped_guard,
                    spike_handle.clone(),
                )
            })
            .collect::<Vec<_>>();

        let _await_guard = spike_handle
            .as_ref()
            .map(|h| h.await_section("try_join_search_chunks"));
        let results = futures::future::try_join_all(chunk_futures)
            .await?
            .into_iter()
            .flatten()
            .collect();
        drop(_await_guard);

        Ok(results)
    }

    async fn do_search_impl(
        &self,
        core_request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Duration,
        hw_counter_acc: HwMeasurementAcc,
        is_stopped_guard: &StoppingGuard,
        spike_handle: Option<common::spike_profiler::SpikeProfilerHandle>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let start = std::time::Instant::now();
        let (query_context, collection_params) = {
            let _await_guard = spike_handle
                .as_ref()
                .map(|h| h.await_section("collection_config_read"));
            let collection_config = self.collection_config.read().await;
            drop(_await_guard);

            let _await_guard = spike_handle
                .as_ref()
                .map(|h| h.await_section("prepare_query_context"));
            let query_context_opt = SegmentsSearcher::prepare_query_context(
                self.segments.clone(),
                &core_request,
                &collection_config,
                timeout,
                search_runtime_handle,
                is_stopped_guard,
                hw_counter_acc.clone(),
                spike_handle.clone(),
            )
            .await?;
            drop(_await_guard);

            let Some(query_context) = query_context_opt else {
                // No segments to search
                return Ok(vec![]);
            };

            (query_context, collection_config.params.clone())
        };

        // update timeout
        let timeout = timeout.saturating_sub(start.elapsed());

        let spike_handle_for_postprocess = spike_handle.clone();
        let _await_guard = spike_handle
            .as_ref()
            .map(|h| h.await_section("segments_searcher_search"));
        let search_request = SegmentsSearcher::search(
            self.segments.clone(),
            core_request.clone(),
            search_runtime_handle,
            true,
            query_context,
            timeout,
            spike_handle,
        );
        let res = tokio::time::timeout(timeout, search_request)
            .await
            .map_err(|_| {
                log::debug!("Search timeout reached: {timeout:?}");
                // StoppingGuard takes care of setting is_stopped to true
                CollectionError::timeout(timeout, "Search")
            })??;
        drop(_await_guard);

        let _sync_guard = spike_handle_for_postprocess
            .as_ref()
            .map(|h| h.sync_section("post_process_results"));
        let top_results = res
            .into_iter()
            .zip(core_request.searches.iter())
            .map(|(vector_res, req)| {
                let vector_name = req.query.get_vector_name();
                let distance = collection_params.get_distance(vector_name).unwrap();
                let processed_res = vector_res.into_iter().map(|mut scored_point| {
                    match req.query {
                        QueryEnum::Nearest(_) => {
                            scored_point.score = distance.postprocess_score(scored_point.score);
                        }
                        // Don't post-process if we are dealing with custom scoring
                        QueryEnum::RecommendBestScore(_)
                        | QueryEnum::RecommendSumScores(_)
                        | QueryEnum::Discover(_)
                        | QueryEnum::Context(_)
                        | QueryEnum::FeedbackNaive(_) => {}
                    };
                    scored_point
                });

                if let Some(threshold) = req.score_threshold {
                    processed_res
                        .take_while(|scored_point| {
                            distance.check_threshold(scored_point.score, threshold)
                        })
                        .collect()
                } else {
                    processed_res.collect()
                }
            })
            .collect();
        drop(_sync_guard);
        Ok(top_results)
    }
}
