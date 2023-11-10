use std::future::Future;
use std::time::Duration;

use itertools::Itertools;
use segment::data_types::vectors::{
    NamedQuery, NamedVector, VectorElementType, VectorType, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, ScoredPoint,
};
use segment::vector_storage::query::reco_query::RecoQuery;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::fetch_vectors::{convert_to_vectors, PointRef, ReferencedPoints};
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch, QueryEnum,
    RecommendRequest, RecommendRequestBatch, RecommendStrategy, UsingVector,
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
    timeout: Option<Duration>,
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
        timeout,
    )
    .await?;
    Ok(results.into_iter().next().unwrap())
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
/// * `collection` - collection to search in
/// * `collection_by_name` - function to retrieve collection by name, used to retrieve points from other collections
/// * `timeout` - timeout for the whole batch, in the searching stage. E.g. time in preprocessing won't be counted
///
pub async fn recommend_batch_by<'a, F, Fut>(
    request_batch: RecommendRequestBatch,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
) -> CollectionResult<Vec<Vec<ScoredPoint>>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    // shortcuts batch if all requests with limit=0
    if request_batch.searches.iter().all(|s| s.limit == 0) {
        return Ok(vec![]);
    }

    // Validate amount of examples
    request_batch.searches.iter().try_for_each(|request| {
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
        Ok(())
    })?;

    // pack all reference vector ids
    let mut all_referenced_ids = ReferencedPoints::new();
    request_batch.searches.iter().for_each(|request| {
        let collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

        let vector_name = get_search_vector_name(request);

        let point_ids_iter = request
            .positive
            .iter()
            .chain(request.negative.iter())
            .filter_map(|example| example.as_point_id());

        all_referenced_ids.add_from_iter(point_ids_iter, vector_name, collection_name);
    });

    // Fetch all referenced vectors
    let all_vectors_records_map = all_referenced_ids
        .fetch_vectors(collection, read_consistency, collection_by_name)
        .await?;

    let mut core_searches = Vec::with_capacity(request_batch.searches.len());

    for request in request_batch.searches.iter() {
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
            if !all_vectors_records_map.contains_key(&PointRef {
                collection_name: lookup_collection_name,
                point_id,
            }) {
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

        let search = match request.strategy.unwrap_or_default() {
            RecommendStrategy::AverageVector => recommend_by_avg_vector(
                request.clone(),
                positive_vectors,
                negative_vectors,
                vector_name,
                reference_vectors_ids,
            ),
            RecommendStrategy::BestScore => recommend_by_best_score(
                request,
                positive_vectors,
                negative_vectors,
                reference_vectors_ids,
            ),
        };

        core_searches.push(search);
    }

    let core_search_batch_request = CoreSearchRequestBatch {
        searches: core_searches,
    };

    collection
        .core_search_batch(core_search_batch_request, read_consistency, None, timeout)
        .await
}

fn recommend_by_avg_vector<'a>(
    request: RecommendRequest,
    positive: impl Iterator<Item = &'a VectorType>,
    negative: impl Iterator<Item = &'a VectorType>,
    vector_name: &str,
    reference_vectors_ids: Vec<ExtendedPointId>,
) -> CoreSearchRequest {
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

    CoreSearchRequest {
        query: QueryEnum::Nearest(
            NamedVector {
                name: vector_name.to_string(),
                vector: search_vector,
            }
            .into(),
        ),
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
    request: &RecommendRequest,
    positive: impl Iterator<Item = &'a VectorType>,
    negative: impl Iterator<Item = &'a VectorType>,
    reference_vectors_ids: Vec<PointIdType>,
) -> CoreSearchRequest {
    let positive = positive.cloned().collect();
    let negative = negative.cloned().collect();

    let query = QueryEnum::RecommendBestScore(NamedQuery {
        query: RecoQuery::new(positive, negative),
        using: request.using.clone().map(|x| match x {
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
        with_payload: request.with_payload.clone(),
        with_vector: request.with_vector.clone(),
        score_threshold: request.score_threshold,
    }
}
