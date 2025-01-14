use std::future::Future;
use std::iter::Peekable;
use std::time::Duration;

use api::rest::RecommendStrategy;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::data_types::vectors::{
    DenseVector, NamedQuery, NamedVectorStruct, TypedMultiDenseVector, VectorElementType,
    VectorInternal, VectorRef, DEFAULT_VECTOR_NAME,
};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, ScoredPoint,
};
use segment::vector_storage::query::RecoQuery;
use sparse::common::sparse_vector::SparseVector;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::batching::batch_requests;
use crate::common::fetch_vectors::{
    convert_to_vectors, convert_to_vectors_owned, resolve_referenced_vectors_batch,
    ReferencedVectors,
};
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::query_enum::QueryEnum;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    RecommendRequestInternal, UsingVector,
};

fn avg_vectors<'a>(
    vectors: impl IntoIterator<Item = VectorRef<'a>>,
) -> CollectionResult<VectorInternal> {
    let mut avg_dense = DenseVector::default();
    let mut avg_sparse = SparseVector::default();
    let mut avg_multi: Option<TypedMultiDenseVector<VectorElementType>> = None;
    let mut dense_count = 0;
    let mut sparse_count = 0;
    let mut multi_count = 0;
    for vector in vectors {
        match vector {
            VectorRef::Dense(vector) => {
                dense_count += 1;
                for i in 0..vector.len() {
                    if i >= avg_dense.len() {
                        avg_dense.push(vector[i])
                    } else {
                        avg_dense[i] += vector[i];
                    }
                }
            }
            VectorRef::Sparse(vector) => {
                sparse_count += 1;
                avg_sparse = vector.combine_aggregate(&avg_sparse, |v1, v2| v1 + v2);
            }
            VectorRef::MultiDense(vector) => {
                multi_count += 1;
                avg_multi = Some(avg_multi.map_or_else(
                    || vector.to_owned(),
                    |mut avg_multi| {
                        avg_multi
                            .flattened_vectors
                            .extend_from_slice(vector.flattened_vectors);
                        avg_multi
                    },
                ));
            }
        }
    }

    match (dense_count, sparse_count, multi_count) {
        // TODO(sparse): what if vectors iterator is empty? We added CollectionError::BadRequest,
        // but it's not clear if it's the best solution.
        // Currently it's hard to return an zeroed vector, because we don't know its type: dense or sparse.
        (0, 0, 0) => Err(CollectionError::bad_input(
            "Positive vectors should not be empty with `average` strategy".to_owned(),
        )),
        (_, 0, 0) => {
            for item in &mut avg_dense {
                *item /= dense_count as VectorElementType;
            }
            Ok(VectorInternal::from(avg_dense))
        }
        (0, _, 0) => {
            for item in &mut avg_sparse.values {
                *item /= sparse_count as VectorElementType;
            }
            Ok(VectorInternal::from(avg_sparse))
        }
        (0, 0, _) => match avg_multi {
            Some(avg_multi) => Ok(VectorInternal::from(avg_multi)),
            None => Err(CollectionError::bad_input(
                "Positive vectors should not be empty with `average` strategy".to_owned(),
            )),
        },
        (_, _, _) => Err(CollectionError::bad_input(
            "Can't average vectors with different types".to_owned(),
        )),
    }
}

fn merge_positive_and_negative_avg(
    positive: VectorInternal,
    negative: VectorInternal,
) -> CollectionResult<VectorInternal> {
    match (positive, negative) {
        (VectorInternal::Dense(positive), VectorInternal::Dense(negative)) => {
            let vector: DenseVector = positive
                .iter()
                .zip(negative.iter())
                .map(|(pos, neg)| pos + pos - neg)
                .collect();
            Ok(vector.into())
        }
        (VectorInternal::Sparse(positive), VectorInternal::Sparse(negative)) => Ok(positive
            .combine_aggregate(&negative, |pos, neg| pos + pos - neg)
            .into()),
        (VectorInternal::MultiDense(mut positive), VectorInternal::MultiDense(negative)) => {
            // merge positive and negative vectors as concatenated vectors with negative vectors negated
            positive.flattened_vectors.extend(negative.flattened_vectors.into_iter().map(|x| -x));
            Ok(VectorInternal::MultiDense(positive))
        },
        _ => Err(CollectionError::bad_input(
            "Positive and negative vectors should be of the same type, either all dense or all sparse or all multi".to_owned(),
        )),
    }
}

pub fn avg_vector_for_recommendation<'a>(
    positive: impl IntoIterator<Item = VectorRef<'a>>,
    mut negative: Peekable<impl Iterator<Item = VectorRef<'a>>>,
) -> CollectionResult<VectorInternal> {
    let avg_positive = avg_vectors(positive)?;

    let search_vector = if negative.peek().is_none() {
        avg_positive
    } else {
        let avg_negative = avg_vectors(negative)?;
        merge_positive_and_negative_avg(avg_positive, avg_negative)?
    };

    Ok(search_vector)
}

pub async fn recommend_by<'a, F, Fut>(
    request: RecommendRequestInternal,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    shard_selector: ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
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
        hw_measurement_acc,
    )
    .await?;
    Ok(results.into_iter().next().unwrap())
}

pub fn recommend_into_core_search(
    collection_name: &str,
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
            .get(lookup_collection_name, point_id)
            .is_none()
        {
            return Err(CollectionError::PointNotFound {
                missed_point_id: point_id,
            });
        }
    }

    // do not exclude vector ids from different lookup collection
    let reference_vectors_ids_to_exclude = match lookup_collection_name {
        Some(lookup_collection_name) if lookup_collection_name != collection_name => vec![],
        _ => reference_vectors_ids,
    };

    match request.strategy.unwrap_or_default() {
        RecommendStrategy::AverageVector => recommend_by_avg_vector(
            request,
            reference_vectors_ids_to_exclude,
            all_vectors_records_map,
        ),
        RecommendStrategy::BestScore => Ok(recommend_by_best_score(
            request,
            reference_vectors_ids_to_exclude,
            all_vectors_records_map,
        )),
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
    request_batch: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<Vec<ScoredPoint>>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let start = std::time::Instant::now();

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
        timeout,
        hw_measurement_acc.clone(),
    )
    .await?;

    // update timeout
    let timeout = timeout.map(|timeout| timeout.saturating_sub(start.elapsed()));

    let res = batch_requests::<
        (RecommendRequestInternal, ShardSelectorInternal),
        ShardSelectorInternal,
        Vec<CoreSearchRequest>,
        Vec<_>,
    >(
        request_batch,
        |(_req, shard)| shard,
        |(req, _), acc| {
            recommend_into_core_search(&collection.id, req, &all_vectors_records_map).map(
                |core_req| {
                    acc.push(core_req);
                },
            )
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
                hw_measurement_acc.clone(),
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
    reference_vectors_ids_to_exclude: Vec<ExtendedPointId>,
    all_vectors_records_map: &ReferencedVectors,
) -> CollectionResult<CoreSearchRequest> {
    let lookup_vector_name = request.get_lookup_vector_name();

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

    let search_vector =
        avg_vector_for_recommendation(positive_vectors, negative_vectors.peekable())?;

    Ok(CoreSearchRequest {
        query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
            search_vector,
            vector_name,
        )),
        filter: Some(Filter {
            should: None,
            min_should: None,
            must: filter.map(|filter| vec![Condition::Filter(filter)]),
            // Exclude vector ids from the same collection given as lookup params
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: reference_vectors_ids_to_exclude.into_iter().collect(),
            })]),
        }),
        with_payload,
        with_vector,
        params,
        limit,
        score_threshold,
        offset: offset.unwrap_or_default(),
    })
}

fn recommend_by_best_score(
    request: RecommendRequestInternal,
    reference_vectors_ids_to_exclude: Vec<PointIdType>,
    all_vectors_records_map: &ReferencedVectors,
) -> CoreSearchRequest {
    let lookup_vector_name = request.get_lookup_vector_name();

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
            min_should: None,
            must: filter.map(|filter| vec![Condition::Filter(filter)]),
            must_not: Some(vec![Condition::HasId(HasIdCondition {
                has_id: reference_vectors_ids_to_exclude.into_iter().collect(),
            })]),
        }),
        params,
        limit,
        offset: offset.unwrap_or_default(),
        with_payload,
        with_vector,
        score_threshold,
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{VectorInternal, VectorRef};
    use sparse::common::sparse_vector::SparseVector;

    use super::avg_vectors;

    #[test]
    fn test_avg_vectors() {
        let vectors: Vec<VectorInternal> = vec![
            vec![1.0, 2.0, 3.0].into(),
            vec![1.0, 2.0, 3.0].into(),
            vec![1.0, 2.0, 3.0].into(),
        ];
        assert_eq!(
            avg_vectors(vectors.iter().map(VectorRef::from)).unwrap(),
            vec![1.0, 2.0, 3.0].into(),
        );

        let vectors: Vec<VectorInternal> = vec![
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.1, 0.2])
                .unwrap()
                .into(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 1.0, 2.0])
                .unwrap()
                .into(),
        ];
        assert_eq!(
            avg_vectors(vectors.iter().map(VectorRef::from)).unwrap(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.55, 1.1])
                .unwrap()
                .into(),
        );

        let vectors: Vec<VectorInternal> = vec![
            vec![1.0, 2.0, 3.0].into(),
            SparseVector::new(vec![0, 1, 2], vec![0.0, 0.1, 0.2])
                .unwrap()
                .into(),
        ];
        assert!(avg_vectors(vectors.iter().map(VectorRef::from)).is_err());
    }
}
