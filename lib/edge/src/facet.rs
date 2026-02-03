use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::common::operation_error::OperationResult;
use segment::data_types::facets::{FacetParams, FacetResponse, FacetValue, FacetValueHit};
use shard::facet::FacetRequestInternal;

use super::EdgeShard;

impl EdgeShard {
    /// Returns facet hits for the given facet request.
    ///
    /// Counts the number of points for each unique value of the specified payload key,
    /// optionally filtering by the given conditions.
    pub fn facet(&self, request: FacetRequestInternal) -> OperationResult<FacetResponse> {
        let FacetRequestInternal {
            key,
            limit,
            filter,
            exact,
        } = request;

        if limit == 0 {
            return Ok(FacetResponse { hits: vec![] });
        }

        let facet_params = FacetParams {
            key,
            limit,
            filter,
            exact,
        };

        let (non_appendable, appendable) = self.segments.read().split_segments();
        let segments = non_appendable.into_iter().chain(appendable);

        let hw_counter = HwMeasurementAcc::disposable_edge().get_counter_cell();
        let is_stopped = AtomicBool::new(false);

        // Collect facet results from all segments
        let segment_results: Vec<HashMap<FacetValue, usize>> = segments
            .map(|segment| {
                segment
                    .get()
                    .read()
                    .facet(&facet_params, &is_stopped, &hw_counter)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Merge results by summing counts for each value
        let mut merged_counts: HashMap<FacetValue, usize> = HashMap::new();
        for segment_result in segment_results {
            for (value, count) in segment_result {
                *merged_counts.entry(value).or_insert(0) += count;
            }
        }

        // Sort by count descending, then by value ascending, and take top `limit`
        let hits: Vec<FacetValueHit> = merged_counts
            .into_iter()
            .map(|(value, count)| FacetValueHit { value, count })
            .k_largest(limit)
            .collect();

        Ok(FacetResponse { hits })
    }
}
