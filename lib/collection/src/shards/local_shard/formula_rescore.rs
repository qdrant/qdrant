use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::query_context::FormulaContext;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::ScoredPoint;
use shard::common::stopping_guard::StoppingGuard;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::{CollectionError, CollectionResult};

impl LocalShard {
    pub async fn rescore_with_formula(
        &self,
        formula: ParsedFormula,
        prefetches_results: Vec<Vec<ScoredPoint>>,
        limit: usize,
        timeout: Duration,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let stopping_guard = StoppingGuard::new();

        let ctx = FormulaContext {
            formula,
            prefetches_results,
            limit,
            is_stopped: stopping_guard.get_is_stopped(),
        };

        let arc_ctx = Arc::new(ctx);

        let future = SegmentsSearcher::rescore_with_formula(
            self.segments.clone(),
            arc_ctx,
            &self.search_runtime,
            hw_measurement_acc,
        );

        let res = tokio::time::timeout(timeout, future)
            .await
            .map_err(|_elapsed| {
                CollectionError::timeout(timeout.as_secs() as usize, "rescore_with_formula")
            })??;

        Ok(res)
    }
}
