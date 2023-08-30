use std::collections::{HashMap, HashSet};
use std::future::Future;

use futures::future::try_join_all;
use itertools::Itertools;
use segment::data_types::vectors::{NamedVector, VectorElementType, DEFAULT_VECTOR_NAME};
use segment::types::{
    Condition, Filter, HasIdCondition, PointIdType, ScoredPoint, WithPayloadInterface, WithVector,
};
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    CollectionError, CollectionResult, PointRequest, RecommendRequest, RecommendRequestBatch,
    Record, SearchRequest, SearchRequestBatch, UsingVector,
};

fn avg_vectors<'a>(
    vectors: impl Iterator<Item = &'a Vec<VectorElementType>>,
) -> Vec<VectorElementType> {
    let mut count: usize = 0;
    let mut avg_vector: Vec<VectorElementType> = vec![];
    for vector in vectors {
        count += 1;
        for i in 0..vector.len() {
            if i >= avg_vector.len() {
                avg_vector.push(vector[i])
            } else {
                avg_vector[i] += vector[i];
            }
        }
    }

    for item in &mut avg_vector {
        *item /= count as VectorElementType;
    }

    avg_vector
}

pub async fn recommend_by<'a, F, Fut>(
    request: RecommendRequest,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<ScoredPoint>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    if request.limit == 0 {
        return Ok(vec![]);
    }
    // `recommend_by` is a special case of recommend_by_batch with a single batch
    let request_batch = RecommendRequestBatch {
        searches: vec![request],
    };
    let results = recommend_batch_by(
        request_batch,
        collection,
        collection_by_name,
        read_consistency,
    )
    .await?;
    Ok(results.into_iter().next().unwrap())
}

async fn retrieve_points(
    collection: &Collection,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<Record>> {
    collection
        .retrieve(
            PointRequest {
                ids,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: WithVector::Selector(vector_names),
            },
            read_consistency,
            None,
        )
        .await
}

enum CollectionRefHolder<'a> {
    Ref(&'a Collection),
    Guard(RwLockReadGuard<'a, Collection>),
}

async fn retrieve_points_with_locked_collection(
    collection_holder: CollectionRefHolder<'_>,
    ids: Vec<PointIdType>,
    vector_names: Vec<String>,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<Record>> {
    match collection_holder {
        CollectionRefHolder::Ref(collection) => {
            retrieve_points(collection, ids, vector_names, read_consistency).await
        }
        CollectionRefHolder::Guard(guard) => {
            retrieve_points(&guard, ids, vector_names, read_consistency).await
        }
    }
}

fn get_search_vector_name(request: &RecommendRequest) -> String {
    match &request.lookup_from {
        None => match &request.using {
            None => DEFAULT_VECTOR_NAME.to_owned(),
            Some(UsingVector::Name(vector_name)) => vector_name.clone(),
        },
        Some(lookup_from) => match &lookup_from.vector {
            None => DEFAULT_VECTOR_NAME.to_owned(),
            Some(vector_name) => vector_name.clone(),
        },
    }
}

/// Search points in a collection by already existing points in this or another collection.
///
/// Function works in following stages:
///
/// - Constructs queries to retrieve points from the existing collections
/// - Executes queries in parallel
/// - Converts retrieve results into lookup table
/// - Constructs regular search queries, execute them as single batch
///
/// # Arguments
///
/// * `request_batch` - batch recommendations request
/// * `search_runtime_handle` - tokio runtime handle to execute search queries
/// * `collection` - collection to search in
/// * `collection_by_name` - function to retrieve collection by name, used to retrieve points from other collections
///
pub async fn recommend_batch_by<'a, F, Fut>(
    request_batch: RecommendRequestBatch,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
) -> CollectionResult<Vec<Vec<ScoredPoint>>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    // shortcuts batch if all requests with limit=0
    if request_batch.searches.iter().all(|s| s.limit == 0) {
        return Ok(vec![]);
    }
    // pack all reference vector ids
    let mut all_reference_vectors_ids: HashMap<_, HashSet<PointIdType>> = Default::default();
    let mut vector_names_per_collection: HashMap<_, HashSet<String>> = Default::default();

    for request in &request_batch.searches {
        if request.positive.is_empty() {
            return Err(CollectionError::BadRequest {
                description: "At least one positive vector ID required".to_owned(),
            });
        }
        let collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

        let reference_vectors_ids = all_reference_vectors_ids
            .entry(collection_name)
            .or_insert_with(HashSet::new);

        let vector_names = vector_names_per_collection
            .entry(collection_name)
            .or_insert_with(HashSet::new);

        vector_names.insert(get_search_vector_name(request));

        for point_id in request.positive.iter().chain(&request.negative) {
            reference_vectors_ids.insert(*point_id);
        }
    }

    debug_assert!(all_reference_vectors_ids.len() == vector_names_per_collection.len());

    let mut collections_names: Vec<_> = Default::default();
    let mut vector_retrieves: Vec<_> = Default::default();
    for (collection_name, reference_vectors_ids) in all_reference_vectors_ids.into_iter() {
        collections_names.push(collection_name);
        let points: Vec<_> = reference_vectors_ids.into_iter().collect();
        let vector_names: Vec<_> = vector_names_per_collection
            .remove(&collection_name)
            .unwrap()
            .into_iter()
            .collect();
        match collection_name {
            None => vector_retrieves.push(retrieve_points_with_locked_collection(
                CollectionRefHolder::Ref(collection),
                points,
                vector_names,
                read_consistency,
            )),
            Some(name) => {
                let other_collection = collection_by_name(name.to_string()).await;
                match other_collection {
                    Some(other_collection) => {
                        vector_retrieves.push(retrieve_points_with_locked_collection(
                            CollectionRefHolder::Guard(other_collection),
                            points,
                            vector_names,
                            read_consistency,
                        ))
                    }
                    None => {
                        return Err(CollectionError::NotFound {
                            what: format!("Collection {name}"),
                        })
                    }
                }
            }
        }
    }

    let all_reference_vectors: Vec<Vec<Record>> = try_join_all(vector_retrieves).await?;

    let mut all_vectors_records_map: HashMap<_, _> = Default::default();

    for (collection_name, reference_vectors) in
        collections_names.into_iter().zip(all_reference_vectors)
    {
        for rec in reference_vectors {
            all_vectors_records_map.insert((collection_name, rec.id), rec);
        }
    }

    let mut searches = Vec::with_capacity(request_batch.searches.len());

    for request in &request_batch.searches {
        let vector_name = match &request.using {
            None => DEFAULT_VECTOR_NAME,
            Some(UsingVector::Name(name)) => name,
        };

        let lookup_vector_name = get_search_vector_name(request);

        let reference_vectors_ids = request
            .positive
            .iter()
            .chain(&request.negative)
            .cloned()
            .collect_vec();

        let request_from_collection = request.lookup_from.as_ref().map(|x| &x.collection);

        for &point_id in &reference_vectors_ids {
            if !all_vectors_records_map.contains_key(&(request_from_collection, point_id)) {
                return Err(CollectionError::PointNotFound {
                    missed_point_id: point_id,
                });
            }
        }

        let avg_positive = avg_vectors(request.positive.iter().filter_map(|vid| {
            let rec = all_vectors_records_map
                .get(&(request_from_collection, *vid))
                .unwrap();
            rec.get_vector_by_name(&lookup_vector_name)
        }));

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = avg_vectors(request.negative.iter().filter_map(|vid| {
                let rec = all_vectors_records_map
                    .get(&(request_from_collection, *vid))
                    .unwrap();
                rec.get_vector_by_name(&lookup_vector_name)
            }));

            avg_positive
                .iter()
                .cloned()
                .zip(avg_negative.iter().cloned())
                .map(|(pos, neg)| pos + pos - neg)
                .collect()
        };

        let search_request = SearchRequest {
            vector: NamedVector {
                name: vector_name.to_string(),
                vector: search_vector,
            }
            .into(),
            filter: Some(Filter {
                should: None,
                must: request
                    .filter
                    .clone()
                    .map(|filter| vec![Condition::Filter(filter)]),
                must_not: Some(vec![Condition::HasId(HasIdCondition {
                    has_id: reference_vectors_ids.iter().cloned().collect(),
                })]),
            }),
            with_payload: request.with_payload.clone(),
            with_vector: request.with_vector.clone(),
            params: request.params,
            limit: request.limit,
            score_threshold: request.score_threshold,
            offset: request.offset,
        };
        searches.push(search_request)
    }

    let search_batch_request = SearchRequestBatch { searches };

    collection
        .search_batch(search_batch_request, read_consistency, None)
        .await
}
