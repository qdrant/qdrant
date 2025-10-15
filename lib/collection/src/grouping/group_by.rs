use std::future::Future;
use std::time::Duration;

use ahash::AHashMap;
use api::rest::{BaseGroupRequest, SearchGroupsRequestInternal, SearchRequestInternal};
use common::counter::hardware_accumulator::HwMeasurementAcc;
use fnv::FnvBuildHasher;
use indexmap::IndexSet;
use segment::json_path::JsonPath;
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, ScoredPoint, WithPayloadInterface,
    WithVector,
};
use serde_json::Value;
use tokio::sync::RwLockReadGuard;

use super::aggregator::GroupsAggregator;
use super::types::QueryGroupRequest;
use crate::collection::Collection;
use crate::common::fetch_vectors;
use crate::common::fetch_vectors::build_vector_resolver_query;
use crate::lookup::WithLookup;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionResult, PointGroup, RecommendGroupsRequestInternal, RecommendRequestInternal,
};
use crate::operations::universal_query::collection_query::{
    CollectionQueryGroupsRequest, CollectionQueryRequest,
};
use crate::operations::universal_query::shard_query::{self, ShardPrefetch, ShardQueryRequest};
use crate::recommendations::recommend_into_core_search;

const MAX_GET_GROUPS_REQUESTS: usize = 5;
const MAX_GROUP_FILLING_REQUESTS: usize = 5;

#[derive(Clone, Debug, PartialEq)]
pub enum SourceRequest {
    Search(SearchRequestInternal),
    Recommend(RecommendRequestInternal),
    Query(CollectionQueryRequest),
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupRequest {
    /// Request to use (search or recommend)
    pub source: SourceRequest,

    /// Path to the field to group by
    pub group_by: JsonPath,

    /// Limit of points to return per group
    pub group_size: usize,

    /// Limit of groups to return
    pub limit: usize,

    /// Options for specifying how to use the group id to lookup points in another collection
    pub with_lookup: Option<WithLookup>,
}

impl GroupRequest {
    pub fn with_limit_from_request(
        source: SourceRequest,
        group_by: JsonPath,
        group_size: usize,
    ) -> Self {
        let limit = match &source {
            SourceRequest::Search(request) => request.limit,
            SourceRequest::Recommend(request) => request.limit,
            SourceRequest::Query(request) => request.limit,
        };
        Self {
            source,
            group_by,
            group_size,
            limit,
            with_lookup: None,
        }
    }

    pub async fn into_query_group_request<'a, F, Fut>(
        self,
        collection: &Collection,
        collection_by_name: F,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<QueryGroupRequest>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        let query_search = match self.source {
            SourceRequest::Search(search_req) => ShardQueryRequest::from(search_req),
            SourceRequest::Recommend(recommend_req) => {
                let referenced_vectors = fetch_vectors::resolve_referenced_vectors_batch(
                    &[(recommend_req.clone(), shard_selection)],
                    collection,
                    collection_by_name,
                    read_consistency,
                    timeout,
                    hw_measurement_acc.clone(),
                )
                .await?;

                let core_search =
                    recommend_into_core_search(&collection.id, recommend_req, &referenced_vectors)?;
                ShardQueryRequest::from(core_search)
            }
            SourceRequest::Query(query_req) => {
                // Lift nested prefetches to root queries for vector resolution
                let resolver_requests = build_vector_resolver_query(&query_req, &shard_selection);

                let referenced_vectors = fetch_vectors::resolve_referenced_vectors_batch(
                    &resolver_requests,
                    collection,
                    collection_by_name,
                    read_consistency,
                    timeout,
                    hw_measurement_acc.clone(),
                )
                .await?;
                query_req.try_into_shard_request(&collection.id, &referenced_vectors)?
            }
        };

        Ok(QueryGroupRequest {
            source: query_search,
            group_by: self.group_by,
            group_size: self.group_size,
            groups: self.limit,
        })
    }
}

impl QueryGroupRequest {
    /// Make `group_by` field selector work with as `with_payload`.
    fn group_by_to_payload_selector(group_by: &JsonPath) -> WithPayloadInterface {
        WithPayloadInterface::Fields(vec![group_by.strip_wildcard_suffix()])
    }

    async fn r#do(
        &self,
        collection: &Collection,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
        hw_measurement_acc: HwMeasurementAcc,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let mut request = self.source.clone();

        // Adjust limit to fetch enough points to fill groups
        request.limit = self.groups * self.group_size;
        request.prefetches.iter_mut().for_each(|prefetch| {
            increase_limit_for_group(prefetch, self.group_size);
        });

        let key_not_empty = Filter::new_must_not(Condition::IsEmpty(self.group_by.clone().into()));
        request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_empty));

        let with_group_by_payload = Self::group_by_to_payload_selector(&self.group_by);

        // We're enriching the final results at the end, so we'll keep this minimal
        request.with_payload = with_group_by_payload;
        request.with_vector = WithVector::Bool(false);

        collection
            .query(
                request,
                read_consistency,
                shard_selection,
                timeout,
                hw_measurement_acc,
            )
            .await
    }
}

impl From<SearchGroupsRequestInternal> for GroupRequest {
    fn from(request: SearchGroupsRequestInternal) -> Self {
        let SearchGroupsRequestInternal {
            vector,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            group_request:
                BaseGroupRequest {
                    group_by,
                    group_size,
                    limit,
                    with_lookup: with_lookup_interface,
                },
        } = request;

        let search = SearchRequestInternal {
            vector,
            filter,
            params,
            limit: 0,
            offset: Some(0),
            with_payload,
            with_vector,
            score_threshold,
        };

        GroupRequest {
            source: SourceRequest::Search(search),
            group_by,
            group_size: group_size as usize,
            limit: limit as usize,
            with_lookup: with_lookup_interface.map(Into::into),
        }
    }
}

impl From<RecommendGroupsRequestInternal> for GroupRequest {
    fn from(request: RecommendGroupsRequestInternal) -> Self {
        let RecommendGroupsRequestInternal {
            positive,
            negative,
            strategy,
            filter,
            params,
            with_payload,
            with_vector,
            score_threshold,
            using,
            lookup_from,
            group_request:
                BaseGroupRequest {
                    group_by,
                    group_size,
                    limit,
                    with_lookup: with_lookup_interface,
                },
        } = request;

        let recommend = RecommendRequestInternal {
            positive,
            negative,
            strategy,
            filter,
            params,
            limit: 0,
            offset: None,
            with_payload,
            with_vector,
            score_threshold,
            using,
            lookup_from,
        };

        GroupRequest {
            source: SourceRequest::Recommend(recommend),
            group_by,
            group_size: group_size as usize,
            limit: limit as usize,
            with_lookup: with_lookup_interface.map(Into::into),
        }
    }
}

impl From<CollectionQueryGroupsRequest> for GroupRequest {
    fn from(request: CollectionQueryGroupsRequest) -> Self {
        let CollectionQueryGroupsRequest {
            prefetch,
            query,
            using,
            filter,
            params,
            score_threshold,
            with_vector,
            with_payload,
            lookup_from,
            group_by,
            group_size,
            limit,
            with_lookup: with_lookup_interface,
        } = request;

        let collection_query_request = CollectionQueryRequest {
            prefetch: prefetch.into_iter().collect(),
            query,
            using,
            filter,
            score_threshold,
            limit,
            offset: 0,
            params,
            with_vector,
            with_payload,
            lookup_from,
        };

        GroupRequest {
            source: SourceRequest::Query(collection_query_request),
            group_by,
            group_size,
            limit,
            with_lookup: with_lookup_interface,
        }
    }
}

/// Uses the request to fill up groups of points.
pub async fn group_by(
    request: QueryGroupRequest,
    collection: &Collection,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<PointGroup>> {
    let start = std::time::Instant::now();
    let collection_params = collection.collection_config.read().await.params.clone();
    let score_ordering =
        shard_query::query_result_order(request.source.query.as_ref(), &collection_params)?;

    let mut aggregator = GroupsAggregator::new(
        request.groups,
        request.group_size,
        request.group_by.clone(),
        score_ordering,
    );

    // Try to complete amount of groups
    let mut needs_filling = true;
    for _ in 0..MAX_GET_GROUPS_REQUESTS {
        // update timeout
        let timeout = timeout.map(|t| t.saturating_sub(start.elapsed()));
        let mut request = request.clone();

        let source = &mut request.source;

        // Construct filter to exclude already found groups
        let full_groups = aggregator.keys_of_filled_groups();
        if !full_groups.is_empty() {
            let except_any = except_on(&request.group_by, &full_groups);
            if !except_any.is_empty() {
                let exclude_groups = Filter {
                    must: Some(except_any),
                    ..Default::default()
                };
                source.filter = Some(
                    source
                        .filter
                        .as_ref()
                        .map(|filter| filter.merge(&exclude_groups))
                        .unwrap_or(exclude_groups),
                );
            }
        }

        // Exclude already aggregated points
        let ids = aggregator.ids().clone();
        if !ids.is_empty() {
            let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
            source.filter = Some(
                source
                    .filter
                    .as_ref()
                    .map(|filter| filter.merge(&exclude_ids))
                    .unwrap_or(exclude_ids),
            );
        }

        // Make request
        let points = request
            .r#do(
                collection,
                read_consistency,
                shard_selection.clone(),
                timeout,
                hw_measurement_acc.clone(),
            )
            .await?;

        if points.is_empty() {
            break;
        }

        aggregator.add_points(&points);

        // TODO: should we break early if we have some amount of "enough" groups?
        if aggregator.len_of_filled_best_groups() >= request.groups {
            needs_filling = false;
            break;
        }
    }

    // Try to fill up groups
    if needs_filling {
        for _ in 0..MAX_GROUP_FILLING_REQUESTS {
            // update timeout
            let timeout = timeout.map(|t| t.saturating_sub(start.elapsed()));
            let mut request = request.clone();

            let source = &mut request.source;

            // Construct filter to only include unsatisfied groups
            let unsatisfied_groups = aggregator.keys_of_unfilled_best_groups();
            let match_any = match_on(&request.group_by, &unsatisfied_groups);
            if !match_any.is_empty() {
                let include_groups = Filter {
                    must: Some(match_any),
                    ..Default::default()
                };
                source.filter = Some(
                    source
                        .filter
                        .as_ref()
                        .map(|filter| filter.merge(&include_groups))
                        .unwrap_or(include_groups),
                );
            }

            // Exclude already aggregated points
            let ids = aggregator.ids().clone();
            if !ids.is_empty() {
                let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
                source.filter = Some(
                    source
                        .filter
                        .as_ref()
                        .map(|filter| filter.merge(&exclude_ids))
                        .unwrap_or(exclude_ids),
                );
            }

            // Make request
            let points = request
                .r#do(
                    collection,
                    read_consistency,
                    shard_selection.clone(),
                    timeout,
                    hw_measurement_acc.clone(),
                )
                .await?;

            if points.is_empty() {
                break;
            }

            aggregator.add_points(&points);

            if aggregator.len_of_filled_best_groups() >= request.groups {
                break;
            }
        }
    }

    // extract best results
    let mut groups = aggregator.distill();

    // flatten results
    let bare_points = groups
        .iter()
        .cloned()
        .flat_map(|group| group.hits)
        .collect();

    // update timeout
    let timeout = timeout.map(|t| t.saturating_sub(start.elapsed()));

    // enrich with payload and vector
    let enriched_points: AHashMap<_, _> = collection
        .fill_search_result_with_payload(
            bare_points,
            Some(request.source.with_payload),
            request.source.with_vector,
            read_consistency,
            &shard_selection,
            timeout,
            hw_measurement_acc.clone(),
        )
        .await?
        .into_iter()
        .map(|point| (point.id, point))
        .collect();

    // hydrate groups with enriched points
    groups
        .iter_mut()
        .for_each(|group| group.hydrate_from(&enriched_points));

    // turn into output form
    let groups = groups.into_iter().map(PointGroup::from).collect();

    Ok(groups)
}

/// Uses the set of values to create Match::Except's, if possible
fn except_on(path: &JsonPath, values: &[Value]) -> Vec<Condition> {
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
fn match_on(path: &JsonPath, values: &[Value]) -> Vec<Condition> {
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
        .filter_map(|v| v.as_str().map(|s| s.into()))
        .collect();

    if !strs.is_empty() {
        any_variants.push(AnyVariants::Strings(strs));
    }

    any_variants
}

fn increase_limit_for_group(shard_prefetch: &mut ShardPrefetch, group_size: usize) {
    shard_prefetch.limit *= group_size;
    shard_prefetch.prefetches.iter_mut().for_each(|prefetch| {
        increase_limit_for_group(prefetch, group_size);
    });
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use segment::data_types::groups::GroupId;
    use segment::payload_json;
    use segment::types::{Payload, ScoredPoint};

    use crate::grouping::types::Group;

    fn make_scored_point(id: u64, score: f32, payload: Option<Payload>) -> ScoredPoint {
        ScoredPoint {
            id: id.into(),
            version: 0,
            score,
            payload,
            vector: None,
            shard_key: None,
            order_value: None,
        }
    }

    #[test]
    fn test_hydrated_from() {
        // arrange
        let mut groups: Vec<Group> = Vec::new();
        [
            (
                "a",
                [
                    make_scored_point(1, 1.0, None),
                    make_scored_point(2, 1.0, None),
                ],
            ),
            (
                "b",
                [
                    make_scored_point(3, 1.0, None),
                    make_scored_point(4, 1.0, None),
                ],
            ),
        ]
        .into_iter()
        .for_each(|(key, points)| {
            let group = Group {
                key: GroupId::from(key),
                hits: points.into_iter().collect(),
            };
            groups.push(group);
        });

        let payload_a = payload_json! {"some_key": "some value a"};
        let payload_b = payload_json! {"some_key": "some value b"};

        let hydrated = vec![
            make_scored_point(1, 1.0, Some(payload_a.clone())),
            make_scored_point(2, 1.0, Some(payload_a.clone())),
            make_scored_point(3, 1.0, Some(payload_b.clone())),
            make_scored_point(4, 1.0, Some(payload_b.clone())),
        ];

        let set: AHashMap<_, _> = hydrated.into_iter().map(|p| (p.id, p)).collect();

        // act
        groups.iter_mut().for_each(|group| group.hydrate_from(&set));

        // assert
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.first().unwrap().hits.len(), 2);
        assert_eq!(groups.get(1).unwrap().hits.len(), 2);

        let a = groups.first().unwrap();
        let b = groups.get(1).unwrap();

        assert!(
            a.hits
                .iter()
                .all(|x| x.payload.as_ref() == Some(&payload_a)),
        );
        assert!(
            b.hits
                .iter()
                .all(|x| x.payload.as_ref() == Some(&payload_b)),
        );
    }
}
