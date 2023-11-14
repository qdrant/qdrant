use std::future::Future;
use std::time::Duration;

use itertools::Itertools;
use segment::data_types::vectors::{
    NamedQuery, NamedVector, VectorElementType, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, ScoredPoint,
};
use segment::vector_storage::query::reco_query::RecoQuery;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::batching::batch_requests;
use crate::common::fetch_vectors::{
    convert_to_vectors, convert_to_vectors_owned, resolve_referenced_vectors_batch,
    ReferencedVectors,
};
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch, QueryEnum,
    RecommendRequestInternal, RecommendStrategy, UsingVector,
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
    request: RecommendRequestInternal,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selector: ShardSelectorInternal,
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
    let request_batch = vec![(request, shard_selector)];
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

fn recommend_into_core_search(
    request: RecommendRequestInternal,
    all_vectors_records_map: &ReferencedVectors,
) -> CollectionResult<CoreSearchRequest> {
    let reference_vectors_ids = request
        .positive
        .iter()
        .chain(&request.negative)
        .filter_map(|example| example.as_point_id())
        .collect_vec();

    let lookup_collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

    for &point_id in &reference_vectors_ids {
        if all_vectors_records_map
            .get(&lookup_collection_name, point_id)
            .is_none()
        {
            return Err(CollectionError::PointNotFound {
                missed_point_id: point_id,
            });
        }
    }

    let res = match request.strategy.unwrap_or_default() {
        RecommendStrategy::AverageVector => {
            recommend_by_avg_vector(request, reference_vectors_ids, all_vectors_records_map)
        }
        RecommendStrategy::BestScore => {
            recommend_by_best_score(request, reference_vectors_ids, all_vectors_records_map)
        }
    };
    Ok(res)
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
    request_batch: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
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
    if request_batch.iter().all(|(s, _)| s.limit == 0) {
        return Ok(vec![]);
    }

    // Validate amount of examples
    request_batch.iter().try_for_each(|(request, _)| {
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

    let all_vectors_records_map = resolve_referenced_vectors_batch(
        &request_batch,
        collection,
        collection_by_name,
        read_consistency,
    )
    .await?;

    let res = batch_requests::<
        (RecommendRequestInternal, ShardSelectorInternal),
        ShardSelectorInternal,
        Vec<CoreSearchRequest>,
        Vec<_>,
        _,
        _,
    >(
        request_batch,
        |(_req, shard)| shard,
        |(req, _), acc| {
            recommend_into_core_search(req, &all_vectors_records_map).map(|core_req| {
                acc.push(core_req);
            })
        },
        |shard_selector, core_searches, requests| {
            if core_searches.is_empty() {
                return Ok(());
            }

            let core_search_batch_request = CoreSearchRequestBatch {
                searches: core_searches,
            };

            requests.push(collection.core_search_batch(
                core_search_batch_request,
                read_consistency,
                shard_selector,
                timeout,
            ));

            Ok(())
        },
    )?;

    let results = futures::future::try_join_all(res).await?;
    let flatten_results: Vec<Vec<_>> = results.into_iter().flatten().collect();
    Ok(flatten_results)
}

fn recommend_by_avg_vector(
    request: RecommendRequestInternal,
    reference_vectors_ids: Vec<ExtendedPointId>,
    all_vectors_records_map: &ReferencedVectors,
) -> CoreSearchRequest {
    let lookup_vector_name = request.get_search_vector_name();

    let RecommendRequestInternal {
        filter,
        with_payload,
        with_vector,
        params,
        limit,
        score_threshold,
        offset,
        using,
        positive,
        negative,
        lookup_from,
        ..
    } = request;

    let lookup_collection_name = lookup_from.as_ref().map(|x| &x.collection);

    let positive_vectors = convert_to_vectors(
        positive.iter(),
        all_vectors_records_map,
        &lookup_vector_name,
        lookup_collection_name,
    );

    let negative_vectors = convert_to_vectors(
        negative.iter(),
        all_vectors_records_map,
        &lookup_vector_name,
        lookup_collection_name,
    );

    let vector_name = match using {
        None => DEFAULT_VECTOR_NAME.to_string(),
        Some(UsingVector::Name(name)) => name,
    };

    let avg_positive = avg_vectors(positive_vectors);
    let negative = negative_vectors.collect_vec();

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
                name: vector_name,
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

fn recommend_by_best_score(
    request: RecommendRequestInternal,
    reference_vectors_ids: Vec<PointIdType>,
    all_vectors_records_map: &ReferencedVectors,
) -> CoreSearchRequest {
    let lookup_vector_name = request.get_search_vector_name();

    let RecommendRequestInternal {
        positive,
        negative,
        strategy: _,
        filter,
        params,
        limit,
        offset,
        with_payload,
        with_vector,
        score_threshold,
        using,
        lookup_from,
    } = request;

    let lookup_collection_name = lookup_from.as_ref().map(|x| &x.collection);

    let positive = convert_to_vectors_owned(
        positive,
        all_vectors_records_map,
        &lookup_vector_name,
        lookup_collection_name,
    );

    let negative = convert_to_vectors_owned(
        negative,
        all_vectors_records_map,
        &lookup_vector_name,
        lookup_collection_name,
    );

    let query = QueryEnum::RecommendBestScore(NamedQuery {
        query: RecoQuery::new(positive, negative),
        using: using.map(|x| match x {
            UsingVector::Name(name) => name,
        }),
    });

    CoreSearchRequest {
        query,
        filter: Some(Filter {
            should: None,
            must: filter.map(|filter| vec![Condition::Filter(filter)]),
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: reference_vectors_ids.into_iter().collect(),
            })]),
        }),
        params,
        limit,
        offset,
        with_payload,
        with_vector,
        score_threshold,
    }
}
