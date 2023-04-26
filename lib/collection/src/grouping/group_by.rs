
use std::future::Future;

use itertools::Itertools;
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, IsNullCondition, Match,
    ScoredPoint, WithPayloadInterface, WithVector,
};
use serde_json::Value;
use tokio::sync::RwLockReadGuard;

use super::aggregator::GroupsAggregator;
use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{CollectionResult, RecommendRequest, SearchRequest};
use crate::recommendations::recommend_by;

#[derive(Clone, Debug)]
pub enum SourceRequest {
    Search(SearchRequest),
    Recommend(RecommendRequest),
}

impl SourceRequest {
    async fn r#do<'a, F, Fut>(
        &self,
        collection: &Collection,
        collection_by_name: F,
        include_key: String,
        top: usize,
    ) -> CollectionResult<Vec<ScoredPoint>>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
    {
        let only_group_by_key = Some(WithPayloadInterface::Fields(vec![include_key.clone()]));

        let key_not_null = Filter::new_must_not(Condition::IsNull(IsNullCondition::from(
            include_key.clone(),
        )));

        match self {
            SourceRequest::Search(request) => {
                let mut request = request.clone();

                request.limit *= top;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_null));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                println!("request: {:#?}", request);

                collection.search(request, None, None).await
            }
            SourceRequest::Recommend(request) => {
                let mut request = request.clone();

                request.limit *= top;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_null));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                println!("request: {:#?}", request);

                recommend_by(request, collection, collection_by_name, None).await
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
pub struct GroupBy {
    /// Request to use
    pub request: SourceRequest,
    /// Path to the field to group by
    pub path: String,
    /// Limit of points to return per group
    pub top: usize,
    /// Limit of groups to return
    pub groups: usize,
}

impl GroupBy {
    pub fn new(request: SourceRequest, path: String, top: usize) -> Self {
        let groups = match &request {
            SourceRequest::Search(request) => request.limit,
            SourceRequest::Recommend(request) => request.limit,
        };
        Self {
            request,
            path,
            top,
            groups,
        }
    }
}

#[derive(Debug)]
pub struct Group {
    pub hits: Vec<ScoredPoint>,
    pub group_id: Value,
}

impl From<GroupsAggregator> for Vec<Group> {
    fn from(groups: GroupsAggregator) -> Self {
        groups
            .groups()
            .iter()
            .map(|(group_id, hits)| Group {
                hits: hits.iter().cloned().sorted().rev().collect(),
                group_id: group_id.0.clone(),
            })
            .sorted_by_key(|g| g.hits[0].clone())
            .rev()
            .collect()
    }
}

/// Uses the request to fill up groups of points.
pub async fn group_by<'a, F, Fut>(
    request: GroupBy,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<Group>>
where
    F: Fn(String) -> Fut + Clone,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let mut groups = GroupsAggregator::new(request.groups, request.top, request.path.clone());

    // Try to complete amount of groups
    for i in 0..3 {
        let enough_groups = (request.groups - groups.len()) == 0;
        if enough_groups {
            break;
        }
        println!("COMPLETING AMOUNT OF GROUPS, lap: {i}");

        let mut req = request.request.clone();

        // construct filter to exclude already found groups
        let full_groups = groups.keys_of_filled_groups();
        if !full_groups.is_empty() {
            if let Some(match_any) = match_on(request.path.clone(), full_groups) {
                let exclude_groups = Filter::new_must_not(match_any);
                req.merge_filter(&exclude_groups);
            }
        }

        // exclude already aggregated points
        let ids = groups.ids();
        if !ids.is_empty() {
            let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
            req.merge_filter(&exclude_ids);
        }

        let points = req
            .r#do(
                collection,
                collection_by_name.clone(),
                request.path.clone(),
                request.top,
            )
            .await?;

        if points.is_empty() {
            break;
        }

        groups.add_points(&points)
    }

    // Try to fill up groups
    for i in 0..3 {
        let unsatisfied_groups = groups.keys_of_unfilled_groups();
        if unsatisfied_groups.is_empty() {
            break;
        }
        println!("FILLING GROUPS, lap: {}", i);

        let mut req = request.request.clone();

        // construct filter to only include unsatisfied groups
        if let Some(match_any) = match_on(request.path.clone(), unsatisfied_groups) {
            let include_groups = Filter::new_must(match_any);
            req.merge_filter(&include_groups);
        }

        // exclude already aggregated points
        let ids = groups.ids();
        if !ids.is_empty() {
            let exclude_ids = Filter::new_must_not(Condition::HasId(ids.into()));
            req.merge_filter(&exclude_ids);
        }

        let points = req
            .r#do(
                collection,
                collection_by_name.clone(),
                request.path.clone(),
                request.top,
            )
            .await?;

        if points.is_empty() {
            break;
        }

        groups.add_points(&points);
    }

    // flatten results
    let without_payload_result = groups.flatten();

    // enrich with payload and vector
    let flat_result = collection
        .fill_search_result_with_payload(
            without_payload_result,
            request.request.with_payload(),
            request.request.with_vector().unwrap_or_default(),
            read_consistency,
            None,
        )
        .await?;

    // re-group
    groups.hydrate_from(&flat_result);

    // turn to output form
    let result: Vec<Group> = groups.into();

    println!("RESULT: {:#?}", result);

    Ok(result)
}

/// Uses the set of values to create a Match::Any, if possible
fn match_on(path: String, values: Vec<Value>) -> Option<Condition> {
    match values[..] {
        [Value::Number(_), ..] => Some(Match::new_any(AnyVariants::Integers(
            values.into_iter().filter_map(|v| v.as_i64()).collect(),
        ))),
        [Value::String(_), ..] => Some(Match::new_any(AnyVariants::Keywords(
            values
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_owned()))
                .collect(),
        ))),
        _ => None, // also considers the case of empty values
    }
    .map(|m| Condition::Field(FieldCondition::new_match(path, m)))
}

fn print_points(points: &[ScoredPoint]) {
    for point in points {
        println!("{point:?}");
    }
    println!("-------------------");
}
