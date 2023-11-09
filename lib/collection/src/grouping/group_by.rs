use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use itertools::Itertools;
use segment::data_types::vectors::{Named, DEFAULT_VECTOR_NAME};
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, Match, ScoredPoint, WithPayloadInterface,
    WithVector,
};
use serde_json::Value;
use tokio::sync::RwLockReadGuard;

use super::aggregator::GroupsAggregator;
use crate::collection::Collection;
use crate::lookup::WithLookup;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    BaseGroupRequest, CollectionError, CollectionResult, PointGroup, RecommendGroupsRequest,
    RecommendRequest, SearchGroupsRequest, SearchRequest, UsingVector,
};
use crate::recommendations::recommend_by;
use crate::shards::shard::ShardId;

const MAX_GET_GROUPS_REQUESTS: usize = 5;
const MAX_GROUP_FILLING_REQUESTS: usize = 5;

#[derive(Clone, Debug)]
pub enum SourceRequest {
    Search(SearchRequest),
    Recommend(RecommendRequest),
}

impl SourceRequest {
    fn vector_field_name(&self) -> &str {
        match self {
            SourceRequest::Search(request) => request.vector.get_name(),
            SourceRequest::Recommend(request) => {
                if let Some(UsingVector::Name(name)) = &request.using {
                    name
                } else {
                    DEFAULT_VECTOR_NAME
                }
            }
        }
    }

    fn merge_filter(&mut self, filter: &Filter) {
        match self {
            SourceRequest::Search(request) => {
                request.filter = Some(request.filter.clone().unwrap_or_default().merge(filter))
            }
            SourceRequest::Recommend(request) => {
                request.filter = Some(request.filter.clone().unwrap_or_default().merge(filter))
            }
        }
    }

    fn with_payload(&self) -> Option<WithPayloadInterface> {
        match self {
            SourceRequest::Search(request) => request.with_payload.clone(),
            SourceRequest::Recommend(request) => request.with_payload.clone(),
        }
    }

    fn with_vector(&self) -> Option<WithVector> {
        match self {
            SourceRequest::Search(request) => request.with_vector.clone(),
            SourceRequest::Recommend(request) => request.with_vector.clone(),
        }
    }
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

    /// Apply a bunch of hacks to make `group_by` field selector work with as `with_payload`.
    fn _group_by_to_payload_selector(&self, group_by: &str) -> CollectionResult<String> {
        // Hack 1: `with_payload` only works with top-level fields. (ToDo: maybe fix this?)
        group_by.split('.').next().map_or_else(
            || {
                Err(CollectionError::bad_request(format!(
                    "Malformed group_by parameter which uses unsupported nested path: {}",
                    group_by
                )))
            },
            |field| {
                // Hack 2: `with_payload` doesn't work with `[]` at the end of the field name.
                // Remove the ending `[]`.
                Ok(field.strip_suffix("[]").unwrap_or(field).to_owned())
            },
        )
    }

    async fn r#do<'a, F, Fut>(
        &self,
        collection: &Collection,
        // only used for recommend
        collection_by_name: F,
        read_consistency: Option<ReadConsistency>,
        shard_selection: Option<ShardId>,
        timeout: Option<Duration>,
    ) -> CollectionResult<Vec<ScoredPoint>>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        let include_group_by = self._group_by_to_payload_selector(&self.group_by)?;

        let only_group_by_key = Some(WithPayloadInterface::Fields(vec![include_group_by]));

        let key_not_empty = Filter::new_must_not(Condition::IsEmpty(self.group_by.clone().into()));

        match self.source.clone() {
            SourceRequest::Search(mut request) => {
                request.limit = self.limit * self.group_size;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_empty));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                collection
                    .search(request, read_consistency, shard_selection, timeout)
                    .await
            }
            SourceRequest::Recommend(mut request) => {
                request.limit = self.limit * self.group_size;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_empty));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                recommend_by(
                    request,
                    collection,
                    collection_by_name,
                    read_consistency,
                    timeout,
                )
                .await
            }
        }
    }
}

impl From<SearchGroupsRequest> for GroupRequest {
    fn from(request: SearchGroupsRequest) -> Self {
        let SearchGroupsRequest {
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

        let search = SearchRequest {
            vector,
            filter,
            params,
            limit: 0,
            offset: 0,
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

impl From<RecommendGroupsRequest> for GroupRequest {
    fn from(request: RecommendGroupsRequest) -> Self {
        let RecommendGroupsRequest {
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

        let recommend = RecommendRequest {
            positive,
            negative,
            strategy,
            filter,
            params,
            limit: 0,
            offset: 0,
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
pub async fn group_by<'a, F, Fut>(
    request: GroupRequest,
    collection: &Collection,
    // Obligatory for recommend
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selection: Option<ShardId>,
    timeout: Option<Duration>,
) -> CollectionResult<Vec<PointGroup>>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let score_ordering = {
        let vector_name = request.source.vector_field_name();
        let collection_params = collection.collection_config.read().await;
        let vector_params = collection_params.params.get_vector_params(vector_name)?;
        vector_params.distance.distance_order()
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
                source.merge_filter(&exclude_groups);
            }
        }

        // Exclude already aggregated points
        let ids = aggregator.ids().clone();
        if !ids.is_empty() {
            let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
            source.merge_filter(&exclude_ids);
        }

        // Make request
        let points = request
            .r#do(
                collection,
                collection_by_name.clone(),
                read_consistency,
                shard_selection,
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
                source.merge_filter(&include_groups);
            }

            // Exclude already aggregated points
            let ids = aggregator.ids().clone();
            if !ids.is_empty() {
                let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
                source.merge_filter(&exclude_ids);
            }

            // Make request
            let points = request
                .r#do(
                    collection,
                    collection_by_name.clone(),
                    read_consistency,
                    shard_selection,
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
            request.source.with_payload(),
            request.source.with_vector().unwrap_or_default(),
            read_consistency,
            None,
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
    let ints = values.iter().filter_map(|v| v.as_i64()).collect_vec();

    if !ints.is_empty() {
        any_variants.push(AnyVariants::Integers(ints));
    }

    // gather string values
    let strs = values
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_owned()))
        .collect_vec();

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
        assert_eq!(groups.get(0).unwrap().hits.len(), 2);
        assert_eq!(groups.get(1).unwrap().hits.len(), 2);

        let a = groups.get(0).unwrap();
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
