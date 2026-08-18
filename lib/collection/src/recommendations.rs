use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use api::rest::RecommendStrategy;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use segment::data_types::vectors::{NamedQuery, VectorInternal};
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PointIdType, ScoredPoint,
};
use segment::vector_storage::query::{RecoQuery, avg_vector_for_recommendation};
use shard::query::query_enum::QueryEnum;
use shard::search::CoreSearchRequestBatch;

use crate::collection::Collection;
use crate::common::batching::{batch_requests, empty_batch_results};
use crate::common::fetch_vectors::{
    ReferencedVectors, convert_to_vectors, convert_to_vectors_owned,
    resolve_referenced_vectors_batch,
};
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::routing::RoutingToken;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, RecommendRequestInternal, UsingVector,
};

#[allow(clippy::too_many_arguments)]
pub async fn recommend_by<F, Fut>(
    request: RecommendRequestInternal,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    routing_token: Option<RoutingToken>,
    shard_selector: ShardSelectorInternal,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<ScoredPoint>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<Arc<Collection>>>,
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
        routing_token,
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
        RecommendStrategy::BestScore => Ok(recommend_by_custom_score(
            request,
            reference_vectors_ids_to_exclude,
            all_vectors_records_map,
            QueryEnum::RecommendBestScore,
        )),
        RecommendStrategy::SumScores => Ok(recommend_by_custom_score(
            request,
            reference_vectors_ids_to_exclude,
            all_vectors_records_map,
            QueryEnum::RecommendSumScores,
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
pub async fn recommend_batch_by<F, Fut>(
    request_batch: Vec<(RecommendRequestInternal, ShardSelectorInternal)>,
    collection: &Collection,
    collection_by_name: F,
    read_consistency: Option<ReadConsistency>,
    routing_token: Option<RoutingToken>,
    timeout: Option<Duration>,
    hw_measurement_acc: HwMeasurementAcc,
) -> CollectionResult<Vec<Vec<ScoredPoint>>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<Arc<Collection>>>,
{
    let start = std::time::Instant::now();

    // shortcuts batch if all requests with limit=0
    if request_batch.iter().all(|(s, _)| s.limit == 0) {
        return Ok(empty_batch_results(request_batch.len()));
    }

    // Validate amount of examples
    request_batch.iter().try_for_each(|(request, _)| {
        match request.strategy.unwrap_or_default() {
            RecommendStrategy::AverageVector => {
                if request.positive.is_empty() {
                    return Err(CollectionError::bad_request(
                        "At least one positive vector ID required with this strategy",
                    ));
                }
            }
            RecommendStrategy::BestScore | RecommendStrategy::SumScores => {
                if request.positive.is_empty() && request.negative.is_empty() {
                    return Err(CollectionError::bad_request(
                        "At least one positive or negative vector ID required with this strategy",
                    ));
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
        routing_token,
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
                routing_token,
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

    let search_vector =
        avg_vector_for_recommendation(positive_vectors, negative_vectors.peekable())?;

    Ok(CoreSearchRequest {
        query: QueryEnum::Nearest(NamedQuery {
            query: search_vector,
            using: using.map(|name| name.as_name()),
        }),
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

fn recommend_by_custom_score(
    request: RecommendRequestInternal,
    reference_vectors_ids_to_exclude: Vec<PointIdType>,
    all_vectors_records_map: &ReferencedVectors,
    query_variant: impl Fn(NamedQuery<RecoQuery<VectorInternal>>) -> QueryEnum,
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

    let query = query_variant(NamedQuery {
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
