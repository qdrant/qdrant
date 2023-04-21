use std::collections::HashMap;
use std::future::Future;

use itertools::Itertools;
use segment::types::{
    AnyVariants, Condition, FieldCondition, Filter, IsNullCondition, Match, ScoredPoint,
    WithPayloadInterface, WithVector,
};
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{CollectionResult, RecommendRequest, SearchRequest};
use crate::recommendations::recommend_by;

#[derive(Clone)]
pub enum MainRequest {
    Search(SearchRequest),
    Recommend(RecommendRequest),
}

impl MainRequest {
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
            MainRequest::Search(request) => {
                let mut request = request.clone();

                request.limit *= top;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_null));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                collection.search(request, None, None).await
            }
            MainRequest::Recommend(request) => {
                let mut request = request.clone();

                request.limit *= top;

                request.filter = Some(request.filter.unwrap_or_default().merge(&key_not_null));

                // We're enriching the final results at the end, so we'll keep this minimal
                request.with_payload = only_group_by_key;
                request.with_vector = None;

                recommend_by(request, collection, collection_by_name, None).await
            }
        }
    }

    fn merge_filter(&mut self, filter: &Filter) {
        match self {
            MainRequest::Search(request) => {
                request.filter = Some(request.filter.clone().unwrap_or_default().merge(filter))
            }
            MainRequest::Recommend(request) => {
                request.filter = Some(request.filter.clone().unwrap_or_default().merge(filter))
            }
        }
    }

    fn with_payload(&self) -> Option<WithPayloadInterface> {
        match self {
            MainRequest::Search(request) => request.with_payload.clone(),
            MainRequest::Recommend(request) => request.with_payload.clone(),
        }
    }

    fn with_vector(&self) -> Option<WithVector> {
        match self {
            MainRequest::Search(request) => request.with_vector.clone(),
            MainRequest::Recommend(request) => request.with_vector.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GroupBy {
    /// Request to use
    pub request: MainRequest,
    /// Path to the field to group by
    pub key: String,
    /// Limit of points to return per group
    pub top: usize,
    /// Limit of groups to return
    pub groups: usize,
}

impl GroupBy {
    pub fn new(request: MainRequest, key: String, top: usize) -> Self {
        let groups = match &request {
            MainRequest::Search(request) => request.limit,
            MainRequest::Recommend(request) => request.limit,
        };
        Self {
            request,
            key,
            top,
            groups,
        }
    }
}

pub struct Group {
    pub hits: Vec<ScoredPoint>,
    pub group_id: String,
}
type Hits = Vec<ScoredPoint>;

impl Group {
    fn new(group_id: usize) -> Self {
        Self {
            hits: Vec::new(),
            group_id: group_id.to_string(),
        }
    }

    fn push(&mut self, point: &ScoredPoint) {
        self.hits.push(point.clone());
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
    let mut groups: HashMap<String, Hits> = HashMap::with_capacity(request.groups);

    // Try to complete amount of groups
    for _ in 0..3 {
        let enough_groups = (request.groups - groups.len()) == 0;
        if enough_groups {
            break;
        }

        // construct filter to exclude already found groups
        let full_groups: Vec<String> = groups
            .iter()
            .filter_map(|(key, g)| {
                if g.len() == request.top {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut req = request.request.clone();

        if !full_groups.is_empty() {
            let exclude_groups = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
                request.key.clone(),
                Match::from(AnyVariants::Keywords(full_groups)),
            )));
            req.merge_filter(&exclude_groups);
        }

        let points = req
            .r#do(
                collection,
                collection_by_name.clone(),
                request.key.clone(),
                request.top,
            )
            .await?;

        bucket_points(&mut groups, points, &request);
    }

    // Try to fill up groups
    for _ in 0..3 {
        let unsatisfied_groups: Vec<_> = groups
            .iter()
            .filter_map(|(key, g)| {
                if g.len() < request.top {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();
        if unsatisfied_groups.is_empty() {
            break;
        }

        let mut req = request.request.clone();

        // construct filter to only include unsatisfied groups
        let include_groups = Filter::new_must(Condition::Field(FieldCondition::new_match(
            request.key.clone(),
            Match::from(AnyVariants::Keywords(unsatisfied_groups)),
        )));

        req.merge_filter(&include_groups);

        let points = req
            .r#do(
                collection,
                collection_by_name.clone(),
                request.key.clone(),
                request.top,
            )
            .await?;

        bucket_points(&mut groups, points, &request);
    }

    // flatten results
    let without_payload_result = groups.iter_mut().fold(Vec::new(), |mut acc, (_, g)| {
        acc.append(g);
        acc
    });

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
    let mut groups = HashMap::with_capacity(request.groups);
    bucket_points(&mut groups, flat_result, &request);

    // turn to output form
    let result: Vec<_> = groups
        .iter_mut()
        .map(|(key, hits)| {
            let mut group = Group::new(key.parse().unwrap());

            std::mem::swap(&mut group.hits, hits);

            group
        })
        .collect();

    Ok(result)
}

fn bucket_points(groups: &mut HashMap<String, Hits>, points: Vec<ScoredPoint>, request: &GroupBy) {
    for point in points.iter() {
        if groups.len() >= request.groups {
            break;
        }

        let group_key = match point
            .payload
            .as_ref()
            .unwrap()
            .get_value(&request.key.clone())
            .values()
            .first()
        {
            Some(value) => value.to_string(),
            _ => continue, // TO FIX: there should not be payloads without the key, but something is allowing it
        };

        let g = groups.entry(group_key).or_insert(Hits::new());

        if g.len() < request.top {
            g.push(point.clone());
        }
    }
}
