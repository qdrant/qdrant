//! Building blocks for group-by queries, shared between the full server implementation
//! (multi-request group filling in `collection`) and the single-request edge implementation,
//! so both interpret group keys and shape candidate queries identically.

pub mod aggregator;

use ahash::AHashMap;
use fnv::FnvBuildHasher;
use indexmap::IndexSet;
use segment::data_types::groups::GroupId;
use segment::json_path::JsonPath;
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, PointIdType, ScoredPoint,
    WithPayloadInterface,
};
use serde_json::Value;

pub use self::aggregator::GroupsAggregator;
use crate::query::{ShardPrefetch, ShardQueryRequest};

#[derive(PartialEq, Debug)]
pub enum AggregatorError {
    BadKeyType,
    KeyNotFound,
}

/// A group of points that share the same value of the `group_by` field.
#[derive(Debug, Clone)]
pub struct Group {
    pub hits: Vec<ScoredPoint>,
    pub key: GroupId,
}

impl Group {
    pub fn hydrate_from(&mut self, map: &AHashMap<PointIdType, ScoredPoint>) {
        self.hits.iter_mut().for_each(|hit| {
            if let Some(point) = map.get(&hit.id) {
                hit.payload.clone_from(&point.payload);
                hit.vector.clone_from(&point.vector);
            }
        });
    }
}

/// Make `group_by` field selector work with as `with_payload`.
pub fn group_by_to_payload_selector(group_by: &JsonPath) -> WithPayloadInterface {
    WithPayloadInterface::Fields(vec![group_by.strip_wildcard_suffix()])
}

/// Rewrite a base scoring query into the query used to fetch group candidates: restrict it to
/// points that carry the `group_by` field, fetch `limit` candidates with only the `group_by`
/// payload, and scale nested prefetch limits by `group_size` so enough candidates survive
/// every rescoring stage.
pub fn shape_candidates_query(
    query: &mut ShardQueryRequest,
    group_by: &JsonPath,
    limit: usize,
    group_size: usize,
) {
    query.limit = limit;
    query.offset = 0;
    query
        .prefetches
        .iter_mut()
        .for_each(|prefetch| increase_limit_for_group(prefetch, group_size));

    let key_not_empty = Filter::new_must_not(Condition::IsEmpty(group_by.clone().into()));
    query.filter = Some(match query.filter.take() {
        Some(filter) => filter.merge_owned(key_not_empty),
        None => key_not_empty,
    });

    query.with_payload = group_by_to_payload_selector(group_by);
}

fn increase_limit_for_group(shard_prefetch: &mut ShardPrefetch, group_size: usize) {
    shard_prefetch.limit *= group_size;
    shard_prefetch.prefetches.iter_mut().for_each(|prefetch| {
        increase_limit_for_group(prefetch, group_size);
    });
}

/// Uses the set of values to create Match::Except's, if possible
pub fn except_on(path: &JsonPath, values: &[Value]) -> Vec<Condition> {
    values_to_any_variants(values)
        .into_iter()
        .map(|v| {
            Condition::Field(FieldCondition::new_match(
                path.clone(),
                Match::new_except(v),
            ))
        })
        .collect()
}

/// Uses the set of values to create Match::Any's, if possible
pub fn match_on(path: &JsonPath, values: &[Value]) -> Vec<Condition> {
    values_to_any_variants(values)
        .into_iter()
        .map(|any_variants| {
            Condition::Field(FieldCondition::new_match(
                path.clone(),
                Match::new_any(any_variants),
            ))
        })
        .collect()
}

fn values_to_any_variants(values: &[Value]) -> Vec<AnyVariants> {
    let mut any_variants = Vec::new();

    // gather int values
    let ints: IndexSet<_, FnvBuildHasher> = values.iter().filter_map(|v| v.as_i64()).collect();

    if !ints.is_empty() {
        any_variants.push(AnyVariants::Integers(ints));
    }

    // gather string values
    let strs: IndexSet<_, FnvBuildHasher> = values
        .iter()
        .filter_map(|v| v.as_str().map(Into::into))
        .collect();

    if !strs.is_empty() {
        any_variants.push(AnyVariants::Strings(strs));
    }

    any_variants
}
