use std::collections::{HashMap, HashSet};
use std::future::Future;

use futures::future::try_join_all;
use itertools::Itertools;
use segment::data_types::vectors::{
    NamedRecoQuery, NamedVector, VectorElementType, VectorType, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, ScoredPoint,
    WithPayloadInterface, WithVector,
};
use segment::vector_storage::query::reco_query::RecoQuery;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch, PointRequest,
    QueryEnum, RecommendExample, RecommendRequest, RecommendRequestBatch, RecommendStrategy,
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
        // Validate amount of examples
        match request.strategy.unwrap_or_default() {
            RecommendStrategy::AverageVector => {
                if request.positive.is_empty() {
                    return Err(CollectionError::BadRequest {
                        description: "At least one positive vector ID required with this strategy"
                            .to_owned(),
                    });
                }
            }
            RecommendStrategy::BestScore => {
                if request.positive.is_empty() && request.negative.is_empty() {
                    return Err(CollectionError::BadRequest {
                        description: "At least one positive or negative vector ID required with this strategy"
                            .to_owned(),
                    });
                }
            }
        }

        let collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

        let reference_vectors_ids = all_reference_vectors_ids
            .entry(collection_name)
            .or_insert_with(HashSet::new);

        let vector_names = vector_names_per_collection
            .entry(collection_name)
            .or_insert_with(HashSet::new);

        vector_names.insert(get_search_vector_name(request));

        request
            .positive
            .iter()
            .chain(&request.negative)
            .filter_map(|example| example.as_point_id())
            .for_each(|point_id| {
                reference_vectors_ids.insert(point_id);
            });
    }

    debug_assert!(all_reference_vectors_ids.len() == vector_names_per_collection.len());

    let mut collections_names = Vec::new();
    let mut vector_retrieves = Vec::new();
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

    let mut results = Vec::with_capacity(request_batch.searches.len());

    // At this point batches that include both types of requests are going to be executed
    // sequentially in runs of requests of the same strategy
    //
    // [avg, avg, avg, score, score, score, avg]
    // |------------>|------------------->|--->|
    //      run1             run2          run3
    //
    // In the future we'll fix this by unify them into CoreSearchRequests and make a single batch
    for (strategy, run) in batch_by_strategy(&request_batch.searches) {
        let mut searches = Vec::new();
        let mut core_searches = Vec::new();
        match strategy {
            RecommendStrategy::AverageVector => searches.reserve_exact(run.len()),
            RecommendStrategy::BestScore => core_searches.reserve_exact(run.len()),
        }

        for request in run {
            let vector_name = match &request.using {
                None => DEFAULT_VECTOR_NAME,
                Some(UsingVector::Name(name)) => name,
            };

            let lookup_vector_name = get_search_vector_name(request);

            let reference_vectors_ids = request
                .positive
                .iter()
                .chain(&request.negative)
                .filter_map(|example| example.as_point_id())
                .collect_vec();

            let lookup_collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

            for &point_id in &reference_vectors_ids {
                if !all_vectors_records_map.contains_key(&(lookup_collection_name, point_id)) {
                    return Err(CollectionError::PointNotFound {
                        missed_point_id: point_id,
                    });
                }
            }

            let positive_vectors = convert_to_vectors(
                request.positive.iter(),
                &all_vectors_records_map,
                &lookup_vector_name,
                lookup_collection_name,
            );

            let negative_vectors = convert_to_vectors(
                request.negative.iter(),
                &all_vectors_records_map,
                &lookup_vector_name,
                lookup_collection_name,
            );

            match strategy {
                RecommendStrategy::AverageVector => {
                    let search = recommend_by_avg_vector(
                        request.clone(),
                        positive_vectors,
                        negative_vectors,
                        vector_name,
                        reference_vectors_ids,
                    );
                    searches.push(search);
                }
                RecommendStrategy::BestScore => {
                    let core_search = recommend_by_best_score(
                        request.clone(),
                        positive_vectors,
                        negative_vectors,
                        reference_vectors_ids,
                    );
                    core_searches.push(core_search);
                }
            };
        }

        let run_result = if !searches.is_empty() {
            let search_batch_request = SearchRequestBatch { searches };
            collection
                .search_batch(search_batch_request, read_consistency, None)
                .await?
        } else {
            let core_search_batch_request = CoreSearchRequestBatch {
                searches: core_searches,
            };
            collection
                .core_search_batch(core_search_batch_request, read_consistency, None)
                .await?
        };

        // Push run result to final results
        run_result.into_iter().for_each(|x| results.push(x));
    }

    Ok(results)
}

/// Groups the consecutive requests of the same strategy into separate batches
fn batch_by_strategy(
    requests: &[RecommendRequest],
) -> impl Iterator<Item = (RecommendStrategy, Vec<&RecommendRequest>)> {
    requests.iter().batching(|iter| {
        match iter.next() {
            None => None,
            Some(req) => {
                // start new batch
                let strategy = req.strategy.unwrap_or_default();
                let mut batch = vec![req];

                // continue until we see a different strategy
                batch.extend(
                    iter.take_while_ref(|req| req.strategy.unwrap_or_default() == strategy),
                );

                Some((strategy, batch))
            }
        }
    })
}
fn recommend_by_avg_vector<'a>(
    request: RecommendRequest,
    positive: impl Iterator<Item = &'a VectorType>,
    negative: impl Iterator<Item = &'a VectorType>,
    vector_name: &str,
    reference_vectors_ids: Vec<ExtendedPointId>,
) -> SearchRequest {
    let RecommendRequest {
        filter,
        with_payload,
        with_vector,
        params,
        limit,
        score_threshold,
        offset,
        ..
    } = request;

    let avg_positive = avg_vectors(positive);
    let negative = negative.collect_vec();

    let search_vector = if negative.is_empty() {
        avg_positive
    } else {
        let avg_negative = avg_vectors(negative.into_iter());

        avg_positive
            .iter()
            .zip(avg_negative.iter())
            .map(|(pos, neg)| pos + pos - neg)
            .collect()
    };

    SearchRequest {
        vector: NamedVector {
            name: vector_name.to_string(),
            vector: search_vector,
        }
        .into(),
        filter: Some(Filter {
            should: None,
            must: filter.clone().map(|filter| vec![Condition::Filter(filter)]),
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: reference_vectors_ids.iter().cloned().collect(),
            })]),
        }),
        with_payload,
        with_vector,
        params,
        limit,
        score_threshold,
        offset,
    }
}

fn recommend_by_best_score<'a>(
    request: RecommendRequest,
    positive: impl Iterator<Item = &'a VectorType>,
    negative: impl Iterator<Item = &'a VectorType>,
    reference_vectors_ids: Vec<PointIdType>,
) -> CoreSearchRequest {
    let positive = positive.cloned().collect();
    let negative = negative.cloned().collect();

    let query = QueryEnum::RecommendBestScore(NamedRecoQuery {
        query: RecoQuery::new(positive, negative),
        using: request.using.map(|x| match x {
            UsingVector::Name(name) => name,
        }),
    });

    CoreSearchRequest {
        query,
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
        params: request.params,
        limit: request.limit,
        offset: request.offset,
        with_payload: request.with_payload,
        with_vector: request.with_vector,
        score_threshold: request.score_threshold,
    }
}

fn convert_to_vectors<'a>(
    examples: impl Iterator<Item = &'a RecommendExample> + 'a,
    all_vectors_records_map: &'a HashMap<(Option<&String>, PointIdType), Record>,
    vector_name: &'a str,
    collection_name: Option<&'a String>,
) -> impl Iterator<Item = &'a VectorType> + 'a {
    examples.filter_map(move |example| match example {
        RecommendExample::Vector(vector) => Some(vector),
        RecommendExample::PointId(vid) => {
            let rec = all_vectors_records_map
                .get(&(collection_name, *vid))
                .unwrap();
            rec.get_vector_by_name(vector_name)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::types::RecommendStrategy;

    #[test]
    fn test_batch_by_strategy() {
        let requests = vec![
            RecommendRequest {
                strategy: Some(RecommendStrategy::AverageVector),
                ..Default::default()
            },
            RecommendRequest {
                strategy: None,
                ..Default::default()
            },
            RecommendRequest {
                strategy: Some(RecommendStrategy::BestScore),
                ..Default::default()
            },
            RecommendRequest {
                strategy: Some(RecommendStrategy::BestScore),
                ..Default::default()
            },
            RecommendRequest {
                strategy: Some(RecommendStrategy::BestScore),
                ..Default::default()
            },
            RecommendRequest {
                strategy: Some(RecommendStrategy::AverageVector),
                ..Default::default()
            },
        ];

        let batches: Vec<_> = batch_by_strategy(&requests)
            .map(|(strategy, batch)| (strategy, batch.len()))
            .collect();

        assert_eq!(
            batches,
            vec![
                (RecommendStrategy::AverageVector, 2),
                (RecommendStrategy::BestScore, 3),
                (RecommendStrategy::AverageVector, 1),
            ]
        );
    }
}
