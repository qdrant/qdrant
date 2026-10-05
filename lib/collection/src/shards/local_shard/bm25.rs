use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::{Filter, ScoredPoint, WithPayload, WithVector};
use shard::common::stopping_guard::StoppingGuard;
use shard::query::text::{TextScoringQuery, TextSearchRequestInternal};

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{CollectionError, CollectionResult};

impl LocalShard {
    /// Rank this shard's points by BM25 over the text index of `query.field`
    /// and return the `limit` best, highest first.
    ///
    /// The text is tokenized here, with the tokenizer of the field's index in
    /// this shard's payload schema, and scored against statistics gathered
    /// over this shard's segments only.
    #[allow(clippy::too_many_arguments)]
    pub async fn score_bm25(
        &self,
        query: &TextScoringQuery,
        filter: Option<Filter>,
        limit: usize,
        with_payload: WithPayload,
        with_vector: WithVector,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let terms = query.tokenize(&self.payload_index_schema.read())?;
        let stopping_guard = StoppingGuard::new();

        let future = SegmentsSearcher::score_bm25(
            self.segments.clone(),
            query.field.clone(),
            terms,
            query.params,
            filter,
            limit,
            with_payload,
            with_vector,
            &self.search_runtime,
            &stopping_guard,
            hw_measurement_acc,
            timeout,
        );

        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_elapsed| CollectionError::timeout(timeout, "score_bm25"))?
    }

    /// Run the BM25 leaves of a planned query concurrently, one result list
    /// per leaf, each cut at its score threshold.
    pub(super) async fn do_text_searches(
        &self,
        texts: Vec<TextSearchRequestInternal>,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<Vec<ScoredPoint>>> {
        let searches = texts.into_iter().map(|text| {
            let TextSearchRequestInternal {
                query,
                filter,
                limit,
                score_threshold,
                with_vector,
                with_payload,
            } = text;
            let hw_measurement_acc = hw_measurement_acc.clone();
            async move {
                let mut points = self
                    .score_bm25(
                        &query,
                        filter,
                        limit,
                        WithPayload::from(with_payload),
                        with_vector,
                        timeout,
                        hw_measurement_acc,
                    )
                    .await?;
                // Best first, so the threshold cuts a suffix. Strict, as the
                // threshold of a search over a larger-is-better distance.
                if let Some(threshold) = score_threshold {
                    let keep = points.partition_point(|point| point.score > threshold);
                    points.truncate(keep);
                }
                Ok(points)
            }
        });
        futures::future::try_join_all(searches).await
    }
}
