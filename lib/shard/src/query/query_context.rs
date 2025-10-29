use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::data_types::query_context::QueryContext;
use segment::types::VectorName;

use crate::common::stopping_guard::StoppingGuard;
use crate::search::CoreSearchRequest;
use crate::segment_holder::LockedSegmentHolder;

pub fn init_query_context(
    batch_request: &[CoreSearchRequest],
    // How many KBs segment should have to be considered requiring indexing for search
    search_optimized_threshold_kb: usize,
    is_stopped_guard: &StoppingGuard,
    hw_measurement_acc: HwMeasurementAcc,
    check_idf_required: impl Fn(&VectorName) -> bool,
) -> QueryContext {
    let mut query_context = QueryContext::new(search_optimized_threshold_kb, hw_measurement_acc)
        .with_is_stopped(is_stopped_guard.get_is_stopped());

    for search_request in batch_request {
        search_request
            .query
            .iterate_sparse(|vector_name, sparse_vector| {
                if check_idf_required(vector_name) {
                    query_context.init_idf(vector_name, &sparse_vector.indices);
                }
            })
    }

    query_context
}

pub fn fill_query_context(
    mut query_context: QueryContext,
    segments: LockedSegmentHolder,
) -> Option<QueryContext> {
    let segments = segments.read();

    if segments.is_empty() {
        return None;
    }

    let segments = segments.non_appendable_then_appendable_segments();
    for locked_segment in segments {
        let segment = locked_segment.get();
        let segment_guard = segment.read();
        segment_guard.fill_query_context(&mut query_context);
    }
    Some(query_context)
}
