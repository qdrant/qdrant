use std::time::Duration;

use futures::Future;
use itertools::Itertools;
use segment::data_types::vectors::{NamedQuery, DEFAULT_VECTOR_NAME};
use segment::types::{Condition, Filter, HasIdCondition, ScoredPoint};
use segment::vector_storage::query::context_query::{ContextPair, ContextQuery};
use segment::vector_storage::query::discovery_query::DiscoveryQuery;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::common::fetch_vectors::{convert_to_vectors, PointRef, ReferencedPoints};
use crate::operations::consistency_params::ReadConsistency;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch, DiscoverRequest,
    DiscoverRequestBatch, QueryEnum, RecommendExample, UsingVector,
};

pub async fn discover<'a, F, Fut>(
    request: DiscoverRequest,
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
    // `discover` is a special case of discover_batch with a single batch
    let request_batch = DiscoverRequestBatch {
        searches: vec![request],
    };
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
    request_batch: DiscoverRequestBatch,
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

    // Validate context_pairs and/or target have value(s)
    request_batch.searches.iter().try_for_each(|request| {
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

    // Pack all referenced ids for retrieving them once
    let mut id_references = ReferencedPoints::new();
    request_batch.searches.iter().for_each(|request| {
        let collection_name = request.lookup_from.as_ref().map(|s| &s.collection);

        let vector_name = get_vector_name(request);

        let ids_iter = iterate_examples(request).filter_map(RecommendExample::as_point_id);

        id_references.add_from_iter(ids_iter, vector_name, collection_name);
    });

    // Fetch all referenced vectors
    let referenced_vectors = id_references
        .fetch_vectors(collection, read_consistency, collection_by_name)
        .await?;

    // Create core search requests
    let mut core_searches = Vec::with_capacity(request_batch.searches.len());
    for request in &request_batch.searches {
        let lookup_collection_name = request.lookup_from.as_ref().map(|x| &x.collection);

        let lookup_vector_name = get_vector_name(request);

        // Check we actually fetched all referenced vectors in this request
        let referenced_ids = iterate_examples(request)
            .filter_map(RecommendExample::as_point_id)
            .collect_vec();

        if let Some(id_not_found) = referenced_ids.iter().find(|&point_id| {
            !referenced_vectors.contains_key(&PointRef {
                collection_name: lookup_collection_name,
                point_id: *point_id,
            })
        }) {
            return Err(CollectionError::PointNotFound {
                missed_point_id: *id_not_found,
            });
        }

        let target = convert_to_vectors(
            request.target.iter(),
            &referenced_vectors,
            &lookup_vector_name,
            lookup_collection_name,
        )
        .next()
        .cloned();

        let context_pairs = request
            .context
            .iter()
            .flatten()
            .map(|pair| {
                let mut vector_pair = convert_to_vectors(
                    pair.iter(),
                    &referenced_vectors,
                    &lookup_vector_name,
                    lookup_collection_name,
                )
                .cloned();

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
            let not_ids = Filter {
                should: None,
                must: None,
                must_not: Some(vec![Condition::HasId(HasIdCondition {
                    has_id: referenced_ids.iter().cloned().collect(),
                })]),
            };

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
            offset: request.offset,
            with_payload: request.with_payload.clone(),
            with_vector: request.with_vector.clone(),
            score_threshold: None,
        };

        core_searches.push(core_search);
    }

    // Do search
    let batch = CoreSearchRequestBatch {
        searches: core_searches,
    };

    collection
        .core_search_batch(batch, read_consistency, None, timeout)
        .await
}

fn iterate_examples(request: &DiscoverRequest) -> impl Iterator<Item = &RecommendExample> {
    request
        .context
        .iter()
        .flat_map(|pairs| pairs.iter().flat_map(|pair| pair.iter()))
        .chain(request.target.iter())
}

fn get_vector_name(request: &DiscoverRequest) -> String {
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
