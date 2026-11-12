use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use segment::data_types::query_context::SegmentQueryContext;
use segment::data_types::vectors::QueryVector;
use segment::entry::entry_point::ReadSegmentEntry;
use segment::types::{Indexes, ScoredPoint, SegmentConfig, VectorName};
use shard::search::{
    BatchSearchParams, CoreSearchRequestBatch, SearchBatchGroup, group_search_batches,
};
use shard::search_shared_filter::share_group_filters;

use super::sampling::sampling_limit;
use crate::collection_manager::holders::segment_holder::LockedSegment;
use crate::operations::types::{CollectionError, CollectionResult};

/// Process sequentially contiguous batches
///
/// # Arguments
///
/// * `segment` - Locked segment to search in
/// * `request` - Batch of search requests
/// * `use_sampling` - If true, try to use probabilistic sampling
/// * `segment_query_context` - Additional context for the search
///
/// # Returns
///
/// Collection Result of:
/// * Vector of ScoredPoints for each request in the batch
/// * Vector of boolean indicating if the segment have further points to search
pub(super) fn search_in_segment(
    segment: LockedSegment,
    request: Arc<CoreSearchRequestBatch>,
    use_sampling: bool,
    segment_query_context: &SegmentQueryContext,
    timeout: Duration,
) -> CollectionResult<(Vec<Vec<ScoredPoint>>, Vec<bool>)> {
    if segment_query_context.is_stopped() {
        return Err(CollectionError::cancelled(
            "Search in segment was cancelled",
        ));
    }

    let batch_size = request.searches.len();

    let mut result: Vec<Vec<ScoredPoint>> = Vec::with_capacity(batch_size);
    let mut further_results: Vec<bool> = Vec::with_capacity(batch_size); // if segment have more points to return

    let groups = group_search_batches(&request.searches);

    // One read lock for the whole batch: filters shared between groups are resolved to the
    // segment's points once, and those must stay valid for every group that uses them.
    let locked_segment = segment.get();
    let Some(read_segment) = locked_segment.try_read_for(timeout) else {
        return Err(CollectionError::timeout(timeout, "batch search"));
    };

    let shared_filters = share_group_filters(&groups, &*read_segment, segment_query_context)?;

    for (group, shared_filter) in groups.into_iter().zip(&shared_filters) {
        let SearchBatchGroup {
            mut params,
            query_vectors,
        } = group;
        // In this segment, a shared filter stands for the group's own
        params.filter = shared_filter.as_ref().or(params.filter);

        let (mut res, mut further) = execute_batch_search(
            &*read_segment,
            &query_vectors,
            &params,
            use_sampling,
            segment_query_context,
        )?;
        further_results.append(&mut further);
        result.append(&mut res);
    }

    Ok((result, further_results))
}

fn execute_batch_search(
    read_segment: &dyn ReadSegmentEntry,
    vectors_batch: &[QueryVector],
    search_params: &BatchSearchParams,
    use_sampling: bool,
    segment_query_context: &SegmentQueryContext,
) -> CollectionResult<(Vec<Vec<ScoredPoint>>, Vec<bool>)> {
    let segment_points = read_segment.available_point_count_without_deferred();
    let segment_config = read_segment.config();

    let top = if use_sampling {
        let ef_limit = search_params
            .params
            .and_then(|p| p.hnsw_ef)
            .or_else(|| get_hnsw_ef_construct(segment_config, search_params.vector_name));
        sampling_limit(
            search_params.top,
            ef_limit,
            segment_points,
            segment_query_context.available_point_count(),
        )
    } else {
        search_params.top
    };

    let vectors_batch = &vectors_batch.iter().collect_vec();
    let res = read_segment.search_batch(
        search_params.vector_name,
        vectors_batch,
        &search_params.with_payload,
        &search_params.with_vector,
        search_params.filter,
        top,
        search_params.params,
        segment_query_context,
    )?;

    let further_results = res
        .iter()
        .map(|batch_result| batch_result.len() == top)
        .collect();

    Ok((res, further_results))
}

/// Find the HNSW ef_construct for a named vector
///
/// If the given named vector has no HNSW index, `None` is returned.
fn get_hnsw_ef_construct(config: &SegmentConfig, vector_name: &VectorName) -> Option<usize> {
    config
        .vector_data
        .get(vector_name)
        .and_then(|config| match &config.index {
            Indexes::Plain {} => None,
            Indexes::Hnsw(hnsw) => Some(hnsw),
        })
        .map(|hnsw| hnsw.ef_construct)
}
