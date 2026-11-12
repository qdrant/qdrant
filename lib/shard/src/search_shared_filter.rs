//! Filters shared between the groups of a batch search, resolved once per segment.

use segment::common::operation_error::OperationResult;
use segment::data_types::filter_user::FilterUser;
use segment::data_types::query_context::SegmentQueryContext;
use segment::entry::ReadSegmentEntry;
use segment::types::Filter;

use crate::search::SearchBatchGroup;

/// For each group of [`group_search_batches`], the filter to search `segment` with instead of
/// the group's own, if any.
///
/// Groups only hold consecutive searches with identical parameters, so a batch that runs one
/// filter against several named vectors becomes one group per vector, each evaluating the
/// filter again. Here the groups sharing a filter hand it to the segment once, which resolves
/// it when that is cheaper than each group evaluating it (see
/// [`ReadSegmentEntry::share_filter`]).
///
/// The returned filters hold offsets of `segment`: the caller must keep it read-locked until
/// the groups have searched with them.
///
/// [`group_search_batches`]: crate::search::group_search_batches
pub fn share_group_filters(
    groups: &[SearchBatchGroup<'_>],
    segment: &(impl ReadSegmentEntry + ?Sized),
    query_context: &SegmentQueryContext,
) -> OperationResult<Vec<Option<Filter>>> {
    let mut shared: Vec<Option<Filter>> = vec![None; groups.len()];
    let mut visited = vec![false; groups.len()];

    for (first, group) in groups.iter().enumerate() {
        if visited[first] {
            continue;
        }
        let Some(filter) = group.params.filter else {
            continue;
        };
        // Comparing filters is expensive on large filters, but far cheaper than evaluating one
        let members: Vec<usize> = (first..groups.len())
            .filter(|&other| groups[other].params.filter == Some(filter))
            .collect();
        for &member in &members {
            visited[member] = true;
        }
        if members.len() < 2 {
            continue;
        }

        let users: Vec<FilterUser<'_>> = members
            .iter()
            .map(|&member| {
                let SearchBatchGroup {
                    params,
                    query_vectors,
                } = &groups[member];
                FilterUser {
                    vector_name: params.vector_name,
                    query_vectors,
                    top: params.top,
                    params: params.params,
                }
            })
            .collect();
        let Some(resolved) = segment.share_filter(filter, &users, query_context)? else {
            continue;
        };
        for &member in &members {
            // Cheap: the resolved points are behind an `Arc`
            shared[member] = Some(resolved.clone());
        }
    }

    Ok(shared)
}
