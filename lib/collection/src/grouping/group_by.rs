use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, ScoredPoint, WithPayloadInterface,
};
use serde_json::Value;
use tokio::sync::RwLockReadGuard;

use super::aggregator::GroupsAggregator;
use super::types::CoreGroupRequest;
use crate::collection::Collection;
use crate::common::fetch_vectors;
use crate::lookup::WithLookup;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    BaseGroupRequest, CollectionResult, PointGroup, RecommendGroupsRequestInternal,
    RecommendRequestInternal, SearchGroupsRequestInternal, SearchRequestInternal,
};
use crate::recommendations::recommend_into_core_search;

const MAX_GET_GROUPS_REQUESTS: usize = 5;
const MAX_GROUP_FILLING_REQUESTS: usize = 5;

#[derive(Clone, Debug)]
pub enum SourceRequest {
    Search(SearchRequestInternal),
    Recommend(RecommendRequestInternal),
}

#[derive(Clone)]
pub struct GroupRequest {
    /// Request to use (search or recommend)
    pub source: SourceRequest,

    /// Path to the field to group by
    pub group_by: String,

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
        group_by: String,
        group_size: usize,
    ) -> Self {
        let limit = match &source {
            SourceRequest::Search(request) => request.limit,
            SourceRequest::Recommend(request) => request.limit,
        };
        Self {
            source,
            group_by,
            group_size,
            limit,
            with_lookup: None,
        }
    }

    pub async fn into_core_group_request<'a, F, Fut>(
        self,
        collection: &Collection,
        collection_by_name: F,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
    ) -> CollectionResult<CoreGroupRequest>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        let core_search = match self.source {
            SourceRequest::Search(search_req) => search_req.into(),
            SourceRequest::Recommend(recommend_req) => {
                let referenced_vectors = fetch_vectors::resolve_referenced_vectors_batch(
                    &[(recommend_req.clone(), shard_selection)],
                    collection,
                    collection_by_name,
                    read_consistency,
                )
                .await?;

                recommend_into_core_search(recommend_req, &referenced_vectors)?
            }
        };

        Ok(CoreGroupRequest {
            source: core_search,
            group_by: self.group_by,
            group_size: self.group_size,
            limit: self.limit,
            with_lookup: self.with_lookup,
        })
    }
}

impl CoreGroupRequest {
    /// Make `group_by` field selector work with as `with_payload`.
    fn group_by_to_payload_selector(&self, group_by: &str) -> WithPayloadInterface {
        let group_by = group_by.strip_suffix("[]").unwrap_or(group_by).to_owned();
        WithPayloadInterface::Fields(vec![group_by])
    }

    async fn r#do(
        &self,
        collection: &Collection,
        read_consistency: Option<ReadConsistency>,
        shard_selection: ShardSelectorInternal,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>> {
        let mut request = self.source.clone();

        request.limit = self.limit * self.group_size;

        let key_not_empty = Filter::new_must_not(Condition::IsEmpty(self.group_by.clone().into()));
        request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_empty));

        let with_group_by_payload = self.group_by_to_payload_selector(&self.group_by);

        // We're enriching the final results at the end, so we'll keep this minimal
        request.with_payload = Some(with_group_by_payload);
        request.with_vector = None;

        collection
            .search(request, read_consistency, &shard_selection, timeout)
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

/// Uses the request to fill up groups of points.
pub async fn group_by(
    request: CoreGroupRequest,
    collection: &Collection,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> CollectionResult<Vec<PointGroup>> {
    let score_ordering = {
        let vector_name = request.source.query.get_vector_name();
        let collection_params = collection.collection_config.read().await;
        let distance = collection_params.params.get_distance(vector_name)?;
        distance.distance_order()
    };

    let mut aggregator = GroupsAggregator::new(
        request.limit,
        request.group_size,
        request.group_by.clone(),
        score_ordering,
    );

    // Try to complete amount of groups
    let mut needs_filling = true;
    for _ in 0..MAX_GET_GROUPS_REQUESTS {
        let mut request = request.clone();

        let source = &mut request.source;

        // Construct filter to exclude already found groups
        let full_groups = aggregator.keys_of_filled_groups();
        if !full_groups.is_empty() {
            let except_any = except_on(&request.group_by, full_groups);
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
            )
            .await?;

        if points.is_empty() {
            break;
        }

        aggregator.add_points(&points);

        // TODO: should we break early if we have some amount of "enough" groups?
        if aggregator.len_of_filled_best_groups() >= request.limit {
            needs_filling = false;
            break;
        }
    }

    // Try to fill up groups
    if needs_filling {
        for _ in 0..MAX_GROUP_FILLING_REQUESTS {
            let mut request = request.clone();

            let source = &mut request.source;

            // Construct filter to only include unsatisfied groups
            let unsatisfied_groups = aggregator.keys_of_unfilled_best_groups();
            let match_any = match_on(&request.group_by, unsatisfied_groups);
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
                )
                .await?;

            if points.is_empty() {
                break;
            }

            aggregator.add_points(&points);

            if aggregator.len_of_filled_best_groups() >= request.limit {
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

    // enrich with payload and vector
    let enriched_points: HashMap<_, _> = collection
        .fill_search_result_with_payload(
            bare_points,
            request.source.with_payload,
            request.source.with_vector.unwrap_or_default(),
            read_consistency,
            &shard_selection,
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
fn except_on(path: &str, values: Vec<Value>) -> Vec<Condition> {
    values_to_any_variants(values)
        .into_iter()
        .map(|v| Condition::Field(FieldCondition::new_match(path, Match::new_except(v))))
        .collect()
}

/// Uses the set of values to create Match::Any's, if possible
fn match_on(path: &str, values: Vec<Value>) -> Vec<Condition> {
    values_to_any_variants(values)
        .into_iter()
        .map(|any_variants| {
            Condition::Field(FieldCondition::new_match(
                path,
                Match::new_any(any_variants),
            ))
        })
        .collect()
}

fn values_to_any_variants(values: Vec<Value>) -> Vec<AnyVariants> {
    let mut any_variants = Vec::new();

    // gather int values
    let ints: HashSet<i64> = values.iter().filter_map(|v| v.as_i64()).collect();

    if !ints.is_empty() {
        any_variants.push(AnyVariants::Integers(ints));
    }

    // gather string values
    let strs: HashSet<_> = values
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.into()))
        .collect();

    if !strs.is_empty() {
        any_variants.push(AnyVariants::Keywords(strs));
    }

    any_variants
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use segment::data_types::groups::GroupId;
    use segment::types::{Payload, ScoredPoint};

    use crate::grouping::types::Group;

    #[test]
    fn test_hydrated_from() {
        // arrange
        let mut groups: Vec<Group> = Vec::new();
        [
            (
                "a",
                [
                    ScoredPoint {
                        id: 1.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                        shard_key: None,
                    },
                    ScoredPoint {
                        id: 2.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                        shard_key: None,
                    },
                ],
            ),
            (
                "b",
                [
                    ScoredPoint {
                        id: 3.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                        shard_key: None,
                    },
                    ScoredPoint {
                        id: 4.into(),
                        version: 0,
                        score: 1.0,
                        payload: None,
                        vector: None,
                        shard_key: None,
                    },
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

        let payload_a = Payload::from(serde_json::json!({"some_key": "some value a"}));
        let payload_b = Payload::from(serde_json::json!({"some_key": "some value b"}));

        let hydrated = vec![
            ScoredPoint {
                id: 1.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
                shard_key: None,
            },
            ScoredPoint {
                id: 2.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
                shard_key: None,
            },
            ScoredPoint {
                id: 3.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
                shard_key: None,
            },
            ScoredPoint {
                id: 4.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
                shard_key: None,
            },
        ];

        let set: HashMap<_, _> = hydrated.into_iter().map(|p| (p.id, p)).collect();

        // act
        groups.iter_mut().for_each(|group| group.hydrate_from(&set));

        // assert
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.first().unwrap().hits.len(), 2);
        assert_eq!(groups.get(1).unwrap().hits.len(), 2);

        let a = groups.first().unwrap();
        let b = groups.get(1).unwrap();

        assert!(a
            .hits
            .iter()
            .all(|x| x.payload.as_ref() == Some(&payload_a)));
        assert!(b
            .hits
            .iter()
            .all(|x| x.payload.as_ref() == Some(&payload_b)));
    }
}
