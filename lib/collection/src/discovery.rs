use std::time::Duration;

use futures::Future;
use itertools::Itertools;
use segment::data_types::vectors::NamedQuery;
use segment::types::{Condition, Filter, HasIdCondition, ScoredPoint};
use segment::vector_storage::query::context_query::{ContextPair, ContextQuery};
use segment::vector_storage::query::discovery_query::DiscoveryQuery;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::batching::batch_requests;
use crate::common::fetch_vectors::{
    convert_to_vectors, resolve_referenced_vectors_batch, ReferencedVectors,
};
use crate::common::retrieve_request_trait::RetrieveRequest;
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    DiscoverRequestInternal, QueryEnum,
};

fn discovery_into_core_search(
    request: DiscoverRequestInternal,
    all_vectors_records_map: &ReferencedVectors,
) -> CollectionResult<CoreSearchRequest> {
    let lookup_collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

    let lookup_vector_name = request.get_search_vector_name();

    // Check we actually fetched all referenced vectors in this request
    let referenced_ids = request.get_referenced_point_ids();

    for &point_id in &referenced_ids {
        if all_vectors_records_map
            .get(&lookup_collection_name, point_id)
            .is_none()
        {
            return Err(CollectionError::PointNotFound {
                missed_point_id: point_id,
            });
        }
    }

    let target = convert_to_vectors(
        request.target.iter(),
        all_vectors_records_map,
        &lookup_vector_name,
        lookup_collection_name,
    )
    .next()
    .map(|v| v.to_owned());

    let context_pairs = request
        .context
        .iter()
        .flatten()
        .map(|pair| {
            let mut vector_pair = convert_to_vectors(
                pair.iter(),
                all_vectors_records_map,
                &lookup_vector_name,
                lookup_collection_name,
            )
            .map(|v| v.to_owned());

            ContextPair {
                // SAFETY: we know there are two elements in the iterator
                positive: vector_pair.next().unwrap(),
                negative: vector_pair.next().unwrap(),
            }
        })
        .collect_vec();

    let query: QueryEnum = match (target, context_pairs) {
        // Target with/without pairs => Discovery
        (Some(target), pairs) => QueryEnum::Discover(NamedQuery {
            query: DiscoveryQuery::new(target, pairs),
            using: Some(lookup_vector_name),
        }),

        // Only pairs => Context
        (None, pairs) => QueryEnum::Context(NamedQuery {
            query: ContextQuery::new(pairs),
            using: Some(lookup_vector_name),
        }),
    };

    let filter = {
        let not_ids = Filter::new_must_not(Condition::HasId(HasIdCondition {
            has_id: referenced_ids.into_iter().collect(),
        }));

        match &request.filter {
            None => not_ids,
            Some(filter) => not_ids.merge(filter),
        }
    };

    let core_search = CoreSearchRequest {
        query,
        filter: Some(filter),
        params: request.params,
        limit: request.limit,
        offset: request.offset.unwrap_or_default(),
        with_payload: request.with_payload,
        with_vector: request.with_vector,
        score_threshold: None,
    };

    Ok(core_search)
}

pub async fn discover<'a, F, Fut>(
    request: DiscoverRequestInternal,
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
    // `discover` is a special case of discover_batch with a single batch
    let request_batch = vec![(request, shard_selector)];

    let results = discover_batch(
        request_batch,
        collection,
        collection_by_name,
        read_consistency,
        timeout,
    )
    .await?;
    Ok(results.into_iter().next().unwrap())
}

pub async fn discover_batch<'a, F, Fut>(
    request_batch: Vec<(DiscoverRequestInternal, ShardSelectorInternal)>,
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

    // Validate context_pairs and/or target have value(s)
    request_batch.iter().try_for_each(|(request, _)| {
        let no_pairs = request.context.is_none()
            || request
                .context
                .as_ref()
                .is_some_and(|pairs| pairs.is_empty());

        let no_target = request.target.is_none();

        if no_pairs && no_target {
            return Err(CollectionError::bad_request(
                "target and/or context_pairs must be specified".to_string(),
            ));
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
        (DiscoverRequestInternal, ShardSelectorInternal),
        ShardSelectorInternal,
        Vec<CoreSearchRequest>,
        Vec<_>,
    >(
        request_batch,
        |(_req, shard)| shard,
        |(req, _), acc| {
            discovery_into_core_search(req, &all_vectors_records_map).map(|core_req| {
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
