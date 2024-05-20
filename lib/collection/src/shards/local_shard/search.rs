use std::sync::Arc;
use std::time::Duration;

use segment::types::{Score, ScoredPoint};
use tokio::runtime::Handle;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::common::stopping_guard::StoppingGuard;
use crate::operations::types::{CollectionError, CollectionResult, CoreSearchRequestBatch};

impl LocalShard {
    pub async fn do_search(
        &self,
        core_request: Arc<CoreSearchRequestBatch>,
        search_runtime_handle: &Handle,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let is_stopped_guard = StoppingGuard::new();

        let (query_context, collection_params) = {
            let collection_config = self.collection_config.read().await;

            let query_context_opt = SegmentsSearcher::prepare_query_context(
                self.segments.clone(),
                &core_request,
                &collection_config,
                &is_stopped_guard,
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
                    if req.query.has_custom_scoring() {
                        if let Some(Score::Float(score)) = &mut scored_point.score {
                            *score = distance.postprocess_score(*score as f32) as f64;
                        }
                    };
                    scored_point
                });

                if let Some(threshold) = req.score_threshold {
                    let threshold = Some(Score::Float(threshold as f64));
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
