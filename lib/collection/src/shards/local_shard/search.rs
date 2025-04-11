use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::ScoredPoint;
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::query_enum::QueryEnum;
use crate::operations::types::{CollectionError, CollectionResult, CoreSearchRequestBatch};

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
        timeout: Option<Duration>,
        hw_counter_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        if core_request.searches.is_empty() {
            return Ok(vec![]);
        }

        let is_stopped_guard = StoppingGuard::new();

        // Don't batch if we have few searches, prevents cloning request
        if core_request.searches.len() <= CHUNK_SIZE {
            return self
                .do_search_impl(
                    core_request,
                    search_runtime_handle,
                    timeout,
                    hw_counter_acc,
                    &is_stopped_guard,
                )
                .await;
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
                )
            })
            .collect::<Vec<_>>();

        let results = futures::future::try_join_all(chunk_futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(results)
    }

    async fn do_search_impl(
        &self,
        core_request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
        hw_counter_acc: HwMeasurementAcc,
        is_stopped_guard: &StoppingGuard,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let (query_context, collection_params) = {
            let collection_config = self.collection_config.read().await;

            let query_context_opt = SegmentsSearcher::prepare_query_context(
                self.segments.clone(),
                &core_request,
                &collection_config,
                is_stopped_guard,
                hw_counter_acc.clone(),
            )
            .await?;

            let Some(query_context) = query_context_opt else {
                // No segments to search
                return Ok(vec![]);
            };

            (query_context, collection_config.params.clone())
        };

        let search_request = SegmentsSearcher::search(
            Arc::clone(&self.segments),
            Arc::clone(&core_request),
            search_runtime_handle,
            true,
            query_context,
        );

        let timeout = timeout.unwrap_or(self.shared_storage_config.search_timeout);

        let res = tokio::time::timeout(timeout, search_request)
            .await
            .map_err(|_| {
                log::debug!("Search timeout reached: {} seconds", timeout.as_secs());
                // StoppingGuard takes care of setting is_stopped to true
                CollectionError::timeout(timeout.as_secs() as usize, "Search")
            })??;

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
                        | QueryEnum::Context(_) => {}
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
        Ok(top_results)
    }
}
