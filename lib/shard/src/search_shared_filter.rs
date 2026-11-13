//! Filters shared between the groups of a batch search, resolved once per segment.

use indexmap::IndexMap;
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
/// filter again, and a group with several query vectors can evaluate it once per vector. Here
/// the groups sharing a filter hand it to the segment once, which resolves it when that is
/// cheaper than each search evaluating it (see
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

    // Groups by filter, in the order each filter first appears. Hashing every filter once keeps
    // this linear in the number of groups, instead of comparing every pair of filters.
    let mut members_by_filter: IndexMap<&Filter, Vec<usize>, ahash::RandomState> =
        IndexMap::default();
    for (index, group) in groups.iter().enumerate() {
        if let Some(filter) = group.params.filter {
            members_by_filter.entry(filter).or_default().push(index);
        }
    }

    for (filter, members) in members_by_filter {
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
        // A group with several query vectors is several searches of the same filter: a graph
        // search evaluates it once per query vector, so a single group can share it too.
        if FilterUser::count_searches(&users) < 2 {
            continue;
        }
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
