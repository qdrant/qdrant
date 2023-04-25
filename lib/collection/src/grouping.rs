use std::collections::{HashMap, HashSet};
use std::future::Future;

use itertools::Itertools;
use segment::types::{
    AnyVariants, Condition, ExtendedPointId, FieldCondition, Filter, IsNullCondition, Match,
    ScoredPoint, WithPayloadInterface, WithVector,
};
use tokio::sync::RwLockReadGuard;

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
    pub group_id: String,
}
type Hits = HashSet<ScoredPoint>;

struct GroupsAggregator {
    groups: HashMap<String, Hits>,
    max_group_size: usize,
    grouped_by: String,
    max_groups: usize,
}

impl GroupsAggregator {
    fn new(groups: usize, group_size: usize, grouped_by: String) -> Self {
        Self {
            groups: HashMap::with_capacity(groups),
            max_group_size: group_size,
            grouped_by,
            max_groups: groups,
        }
    }

    /// Adds a point to the group that corresponds based on the group_by field, assumes that the point has the group_by field
    fn add_point(&mut self, point: &ScoredPoint) {
        // if the key contains multiple values, grabs the first one
        let group_value = point
            .payload
            .as_ref()
            .unwrap()
            .get_value(&self.grouped_by)
            .values()
            .first()
            .unwrap()
            .to_string()
            .trim_matches('"')
            .to_owned();

        println!("point_id: {:?}, group_value: {:#?}", point.id, group_value);

        if !self.groups.contains_key(&group_value) && self.groups.len() >= self.max_groups {
            return;
        }

        let group = self
            .groups
            .entry(group_value)
            .or_insert_with(|| HashSet::with_capacity(self.max_group_size));

        if group.len() >= self.max_group_size {
            return;
        }

        group.insert(point.clone());
    }

    /// Adds multiple points to the group that they corresponds based on the group_by field, assumes that the points always have the group_by field
    fn add_points(&mut self, points: &[ScoredPoint]) {
        print_points(points);
        points.iter().for_each(|point| self.add_point(point));
        for group in self.groups.iter() {
            println!("group: {:#?}", group);
        }
    }

    fn len(&self) -> usize {
        self.groups.len()
    }

    // gets the keys of the groups that have less than the max group size
    fn keys_of_unfilled_groups(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter(|(_, hits)| hits.len() < self.max_group_size)
            .map(|(key, _)| key.clone())
            .collect()
    }

    // gets the keys of the groups that have reached or exceeded the max group size
    fn keys_of_filled_groups(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter(|(_, hits)| hits.len() >= self.max_group_size)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Gets the ids of the already present points in all of the groups
    fn ids(&self) -> HashSet<ExtendedPointId> {
        self.groups
            .iter()
            .flat_map(|(_, hits)| hits.iter())
            .map(|p| p.id)
            .collect()
    }

    fn flatten(&self) -> Vec<ScoredPoint> {
        self.groups
            .iter()
            .flat_map(|(_, hits)| hits.iter())
            .cloned()
            .collect()
    }

    /// Copies the payload and vector from the provided points to the points inside of each of the groups
    fn hydrate_from(&mut self, points: &[ScoredPoint]) {
        for point in points {
            self.groups.iter_mut().for_each(|(_, ps)| {
                if ps.contains(point) {
                    ps.replace(point.clone())
                        .expect("The point should be in the group before replacing it! 😱");
                }
            });
        }
    }
}

impl Group {
    fn new(group_id: usize) -> Self {
        Self {
            hits: Vec::new(),
            group_id: group_id.to_string(),
        }
    }
}

impl From<GroupsAggregator> for Vec<Group> {
    fn from(groups: GroupsAggregator) -> Self {
        groups
            .groups
            .into_iter()
            .map(|(group_id, hits)| Group {
                hits: hits.into_iter().sorted().rev().collect(),
                group_id,
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
            let exclude_groups = Filter::new_must_not(Condition::Field(FieldCondition::new_match(
                request.path.clone(),
                Match::from(AnyVariants::Keywords(full_groups)),
            )));
            req.merge_filter(&exclude_groups);
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
        let include_groups = Filter::new_must(Condition::Field(FieldCondition::new_match(
            request.path.clone(),
            Match::from(AnyVariants::Keywords(unsatisfied_groups)),
        )));

        req.merge_filter(&include_groups);

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

fn print_points(points: &[ScoredPoint]) {
    for point in points {
        println!("{point:?}");
    }
    println!("-------------------");
}
#[cfg(test)]
mod unit_tests {

    use segment::types::Payload;

    use super::*;

    #[test]
    fn hydrate_from() {
        let mut aggregator = GroupsAggregator::new(2, 2, "docId".to_string());

        aggregator.groups.insert(
            "a".to_string(),
            [
                ScoredPoint {
                    id: 1.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
                ScoredPoint {
                    id: 2.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
            ]
            .into(),
        );

        aggregator.groups.insert(
            "b".to_string(),
            [
                ScoredPoint {
                    id: 3.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
                ScoredPoint {
                    id: 4.into(),
                    version: 0,
                    score: 1.0,
                    payload: None,
                    vector: None,
                },
            ]
            .into(),
        );

        let payload_a = Payload::from(serde_json::json!({"some_key": "some value a"}));
        let payload_b = Payload::from(serde_json::json!({"some_key": "some value b"}));

        let hydrated = vec![
            ScoredPoint {
                id: 1.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 2.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_a.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 3.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
            },
            ScoredPoint {
                id: 4.into(),
                version: 0,
                score: 1.0,
                payload: Some(payload_b.clone()),
                vector: None,
            },
        ];

        aggregator.hydrate_from(&hydrated);

        assert_eq!(aggregator.groups.len(), 2);
        assert_eq!(aggregator.groups.get("a").unwrap().len(), 2);
        assert_eq!(aggregator.groups.get("b").unwrap().len(), 2);

        let a = aggregator.groups.get("a").unwrap();
        let b = aggregator.groups.get("b").unwrap();

        assert!(a.iter().all(|x| x.payload == Some(payload_a.clone())));
        assert!(b.iter().all(|x| x.payload == Some(payload_b.clone())));
    }
}
