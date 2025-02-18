use std::sync::Arc;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::query_context::FormulaContext;
use segment::types::ScoredPoint;

use super::LocalShard;
use crate::collection_manager::segments_searcher::SegmentsSearcher;
use crate::operations::types::CollectionResult;

impl LocalShard {
    pub async fn rescore_with_formula(
        &self,
        ctx: FormulaContext,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let arc_ctx = Arc::new(ctx);

        let res = SegmentsSearcher::rescore_with_formula(
            self.segments.clone(),
            arc_ctx,
            &self.search_runtime,
            hw_measurement_acc,
        )
        .await?;

        Ok(res)
    }
}
