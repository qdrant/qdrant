use std::collections::{HashMap, HashSet};
use std::future::Future;

use itertools::Itertools;
use segment::data_types::vectors::{NamedVector, VectorElementType, DEFAULT_VECTOR_NAME};
use segment::types::{Condition, Filter, HasIdCondition, ScoredPoint, WithPayloadInterface};
use tokio::runtime::Handle;
use tokio::sync::RwLockReadGuard;

use crate::collection::Collection;
use crate::operations::types::{
    CollectionError, CollectionResult, PointRequest, RecommendRequest, RecommendRequestBatch,
    SearchRequest, SearchRequestBatch, UsingVector,
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
    search_runtime_handle: &Handle,
    current_collection: &str,
    collection_by_name: F,
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
        search_runtime_handle,
        current_collection,
        collection_by_name,
    )
    .await?;
    Ok(results.into_iter().next().unwrap())
}

pub async fn recommend_batch_by<'a, F, Fut>(
    request_batch: RecommendRequestBatch,
    search_runtime_handle: &Handle,
    current_collection: &str,
    collection_by_name: F,
) -> CollectionResult<Vec<Vec<ScoredPoint>>>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Option<RwLockReadGuard<'a, Collection>>>,
{
    let collection = collection_by_name(current_collection.to_string())
        .await
        .ok_or_else(|| CollectionError::NotFound {
            what: format!("collection {}", current_collection),
        })?;

    // shortcuts batch if all requests with limit=0
    if request_batch.searches.iter().all(|s| s.limit == 0) {
        return Ok(vec![]);
    }
    // pack all reference vector ids
    let mut all_reference_vectors_ids = HashSet::new();
    for request in &request_batch.searches {
        if request.positive.is_empty() {
            return Err(CollectionError::BadRequest {
                description: "At least one positive vector ID required".to_owned(),
            });
        }
        for point_id in request.positive.iter().chain(&request.negative) {
            all_reference_vectors_ids.insert(*point_id);
        }
    }

    // batch vector retrieval
    let all_vectors = collection
        .retrieve(
            PointRequest {
                ids: all_reference_vectors_ids.into_iter().collect(),
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: true.into(),
            },
            None,
        )
        .await?;

    let mut searches = Vec::with_capacity(request_batch.searches.len());

    for request in request_batch.searches {
        let vector_name = match request.using {
            None => DEFAULT_VECTOR_NAME.to_owned(),
            Some(UsingVector::Name(name)) => name,
        };

        //let rec_vectors = rec.get
        let mut all_vectors_map = HashMap::new();

        for rec in all_vectors.iter() {
            let vector = rec.get_vector_by_name(&vector_name);
            if let Some(vector) = vector {
                all_vectors_map.insert(rec.id, vector);
            } else {
                return Err(CollectionError::BadRequest {
                    description: format!(
                        "Vector '{}' not found, expected one of {:?}",
                        vector_name,
                        rec.vector_names()
                    ),
                });
            }
        }

        let reference_vectors_ids = request
            .positive
            .iter()
            .chain(&request.negative)
            .cloned()
            .collect_vec();

        for &point_id in &reference_vectors_ids {
            if !all_vectors_map.contains_key(&point_id) {
                return Err(CollectionError::PointNotFound {
                    missed_point_id: point_id,
                });
            }
        }

        let avg_positive = avg_vectors(
            request
                .positive
                .iter()
                .map(|vid| *all_vectors_map.get(vid).unwrap()),
        );

        let search_vector = if request.negative.is_empty() {
            avg_positive
        } else {
            let avg_negative = avg_vectors(
                request
                    .negative
                    .iter()
                    .map(|vid| *all_vectors_map.get(vid).unwrap()),
            );

            avg_positive
                .iter()
                .cloned()
                .zip(avg_negative.iter().cloned())
                .map(|(pos, neg)| pos + pos - neg)
                .collect()
        };

        let search_request = SearchRequest {
            vector: NamedVector {
                name: vector_name,
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
            with_vector: request.with_vector,
            params: request.params,
            limit: request.limit,
            score_threshold: request.score_threshold,
            offset: request.offset,
        };
        searches.push(search_request)
    }

    let search_batch_request = SearchRequestBatch { searches };

    collection
        .search_batch(search_batch_request, search_runtime_handle, None)
        .await
}
