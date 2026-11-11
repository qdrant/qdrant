use std::sync::Arc;
use std::time::Duration;

use common::types::DeferredBehavior;
use itertools::Itertools;
use segment::data_types::query_context::SegmentQueryContext;
use segment::data_types::vectors::QueryVector;
use segment::entry::entry_point::ReadSegmentEntry;
use segment::types::{
    Condition, Filter, HasIdCondition, Indexes, ScoredPoint, SegmentConfig, VectorName,
};
use shard::search::{
    BatchSearchParams, CoreSearchRequestBatch, SearchBatchGroup, group_search_batches,
};

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

    // One read lock for the whole batch: filters shared between groups are resolved to ids
    // once, and those ids must stay valid for every group that uses them.
    let locked_segment = segment.get();
    let Some(read_segment) = locked_segment.try_read_for(timeout) else {
        return Err(CollectionError::timeout(timeout, "batch search"));
    };

    let shared_filters = resolve_shared_filters(&groups, &*read_segment, segment_query_context)?;

    for (group, shared_filter) in groups.into_iter().zip(&shared_filters) {
        let SearchBatchGroup {
            params,
            query_vectors,
        } = group;
        let params = BatchSearchParams {
            filter: shared_filter.as_ref().or(params.filter),
            ..params
        };

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

/// Above this many matching points in a segment, a filter is not shared between groups.
/// Every group then resolves and hashes all the ids, which can cost more than a graph search
/// that checks the original filter only on the points it visits.
///
// ponytail: fixed cap; pass a segment-internal bitmask down to the vector index instead of
// `HasId` to drop the per-group id resolution and this cap.
const MAX_SHARED_FILTER_POINTS: usize = 50_000;

/// For each group, a `HasId` filter equivalent to its filter in this segment, if the same
/// filter is used by more than one group of the batch.
///
/// A batch over several named vectors is split into one group per vector, and each group
/// evaluates its filter on its own. Evaluating a filter shared by several groups once and
/// handing the matching ids to all of them avoids repeating an expensive filter (e.g.
/// `nested`) for every vector.
///
/// The ids are read with [`DeferredBehavior::VisibleOnly`], the visibility vector searches use,
/// and the caller must keep the segment read-locked while the groups use them.
fn resolve_shared_filters(
    groups: &[SearchBatchGroup],
    read_segment: &dyn ReadSegmentEntry,
    segment_query_context: &SegmentQueryContext,
) -> CollectionResult<Vec<Option<Filter>>> {
    // Account the filter evaluation to the query, like `search_batch` does
    let _scope = segment_query_context.handoff().enter_guard();
    let mut shared: Vec<Option<Filter>> = vec![None; groups.len()];

    for (i, group) in groups.iter().enumerate() {
        let Some(filter) = group.params.filter else {
            continue;
        };
        // Already resolved for an earlier group with the same filter
        if shared[i].is_some() {
            continue;
        }
        let users: Vec<usize> = (i..groups.len())
            .filter(|&j| groups[j].params.filter == Some(filter))
            .collect();
        if users.len() < 2 {
            continue;
        }
        if read_segment.estimate_point_count(Some(filter))?.max > MAX_SHARED_FILTER_POINTS {
            continue;
        }

        let ids = read_segment.read_filtered(
            None,
            None,
            Some(filter),
            segment_query_context.is_stopped_flag(),
            DeferredBehavior::VisibleOnly,
        )?;
        let ids_filter = Filter::new_must(Condition::HasId(HasIdCondition::from(
            ids.into_iter().collect::<ahash::AHashSet<_>>(),
        )));
        for j in users {
            shared[j] = Some(ids_filter.clone());
        }
    }

    Ok(shared)
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
