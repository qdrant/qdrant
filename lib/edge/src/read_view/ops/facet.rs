use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::common::operation_error::OperationResult;
use segment::data_types::facets::{FacetParams, FacetResponse};
use segment::entry::ReadSegmentEntry;
use shard::facet::FacetRequestInternal;

use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// Returns facet hits for the given facet request.
    ///
    /// Counts the number of points for each unique value of the specified payload key,
    /// optionally filtering by the given conditions.
    pub(crate) fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse> {
        let FacetRequestInternal {
            key,
            limit,
            filter,
            exact,
        } = request;

        let hw_acc = HwMeasurementAcc::disposable_edge();
        let is_stopped = AtomicBool::new(false);

        let facet_params = FacetParams {
            key,
            limit,
            filter,
            exact,
        };

        // Facet every segment in parallel, then merge the per-segment counts sequentially.
        let per_segment = self.par_map_segments(|segment| {
            segment
                .read_segment()
                .facet(&facet_params, &is_stopped, &hw_acc.get_counter_cell())
        })?;

        let mut merged_counts = HashMap::new();
        for segment_result in per_segment {
            for (value, count) in segment_result {
                *merged_counts.entry(value).or_insert(0) += count;
            }
        }

        Ok(FacetResponse::top_hits(merged_counts, limit))
    }
}
