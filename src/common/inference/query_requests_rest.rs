use api::rest::schema as rest;
use collection::lookup::WithLookup;
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
    VectorInputInternal, VectorQuery,
};
use collection::operations::universal_query::shard_query::{FusionInternal, SampleInternal};
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{MultiDenseVectorInternal, VectorInternal, DEFAULT_VECTOR_NAME};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use storage::content_manager::errors::StorageError;

use crate::common::inference::batch_processing::collect_query_groups_request;
use crate::common::inference::infer_processing::BatchAccumInferred;
use crate::common::inference::service::{InferenceData, InferenceType};

pub async fn convert_query_groups_request_from_rest(
    request: rest::QueryGroupsRequestInternal,
) -> Result<CollectionQueryGroupsRequest, StorageError> {
    let batch = collect_query_groups_request(&request);
    let rest::QueryGroupsRequestInternal {
        prefetch,
        query,
        using,
        filter,
        score_threshold,
        params,
        with_vector,
        with_payload,
        lookup_from,
        group_request,
    } = request;

    let inferred = BatchAccumInferred::from_batch_accum(batch, InferenceType::Search).await?;
    let query = query
        .map(|q| convert_query_with_inferred(q, &inferred))
        .transpose()?;

    let prefetch = prefetch
        .map(|prefetches| {
            prefetches
                .into_iter()
                .map(|p| convert_prefetch_with_inferred(p, &inferred))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(CollectionQueryGroupsRequest {
        prefetch,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter,
        score_threshold,
        params,
        with_vector: with_vector.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
        with_payload: with_payload.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
        lookup_from,
        limit: group_request
            .limit
            .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        group_by: group_request.group_by,
        group_size: group_request
            .group_size
            .unwrap_or(CollectionQueryRequest::DEFAULT_GROUP_SIZE),
        with_lookup: group_request.with_lookup.map(WithLookup::from),
    })
}

pub async fn convert_query_request_from_rest(
    request: rest::QueryRequestInternal,
) -> Result<CollectionQueryRequest, StorageError> {
    let rest::QueryRequestInternal {
        prefetch,
        query,
        using,
        filter,
        score_threshold,
        params,
        limit,
        offset,
        with_vector,
        with_payload,
        lookup_from,
    } = request;

    let prefetch = prefetch
        .map(|prefetches| {
            prefetches
                .into_iter()
                .map(convert_collection_prefetch)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let query = query.map(convert_query).transpose()?;

    Ok(CollectionQueryRequest {
        prefetch,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter,
        score_threshold,
        limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        offset: offset.unwrap_or(CollectionQueryRequest::DEFAULT_OFFSET),
        params,
        with_vector: with_vector.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
        with_payload: with_payload.unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
        lookup_from,
    })
}

fn convert_collection_prefetch(
    prefetch: rest::Prefetch,
) -> Result<CollectionPrefetch, StorageError> {
    let rest::Prefetch {
        prefetch,
        query,
        using,
        filter,
        score_threshold,
        params,
        limit,
        lookup_from,
    } = prefetch;

    let query = query.map(convert_query).transpose()?;

    let prefetch = prefetch
        .map(|prefetches| {
            prefetches
                .into_iter()
                .map(convert_collection_prefetch)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(CollectionPrefetch {
        prefetch,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter,
        score_threshold,
        limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params,
        lookup_from,
    })
}

fn convert_query(query: rest::QueryInterface) -> Result<Query, StorageError> {
    let query = rest::Query::from(query);

    match query {
        rest::Query::Nearest(nearest) => Ok(Query::Vector(VectorQuery::Nearest(
            convert_vector_input(nearest.nearest)?,
        ))),
        rest::Query::Recommend(recommend) => {
            Ok(Query::Vector(convert_recommend_input(recommend.recommend)?))
        }
        rest::Query::Discover(discover) => {
            Ok(Query::Vector(convert_discover_input(discover.discover)?))
        }
        rest::Query::Context(context) => Ok(Query::Vector(convert_context_input(context.context)?)),
        rest::Query::OrderBy(order_by) => Ok(Query::OrderBy(OrderBy::from(order_by.order_by))),
        rest::Query::Fusion(fusion) => Ok(Query::Fusion(FusionInternal::from(fusion.fusion))),
        rest::Query::Sample(sample) => Ok(Query::Sample(SampleInternal::from(sample.sample))),
    }
}

fn convert_recommend_input(
    recommend: rest::RecommendInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::RecommendInput {
        positive,
        negative,
        strategy,
    } = recommend;

    let positives = positive
        .map(|pos| pos.into_iter().map(convert_vector_input).collect())
        .transpose()?
        .unwrap_or_default();

    let negatives = negative
        .map(|neg| neg.into_iter().map(convert_vector_input).collect())
        .transpose()?
        .unwrap_or_default();

    let reco_query = RecoQuery::new(positives, negatives);

    match strategy.unwrap_or_default() {
        rest::RecommendStrategy::AverageVector => {
            Ok(VectorQuery::RecommendAverageVector(reco_query))
        }
        rest::RecommendStrategy::BestScore => Ok(VectorQuery::RecommendBestScore(reco_query)),
    }
}

fn convert_discover_input(
    discover: rest::DiscoverInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::DiscoverInput { target, context } = discover;

    let target = convert_vector_input(target)?;
    let context = context
        .map(|pairs| {
            pairs
                .into_iter()
                .map(context_pair_from_rest)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
}

fn convert_context_input(
    context: rest::ContextInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::ContextInput(context) = context;

    let context = context
        .map(|pairs| {
            pairs
                .into_iter()
                .map(context_pair_from_rest)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(VectorQuery::Context(ContextQuery::new(context)))
}

fn convert_vector_input(vector: rest::VectorInput) -> Result<VectorInputInternal, StorageError> {
    match vector {
        rest::VectorInput::Id(id) => Ok(VectorInputInternal::Id(id)),
        rest::VectorInput::DenseVector(dense) => {
            Ok(VectorInputInternal::Vector(VectorInternal::Dense(dense)))
        }
        rest::VectorInput::SparseVector(sparse) => {
            Ok(VectorInputInternal::Vector(VectorInternal::Sparse(sparse)))
        }
        rest::VectorInput::MultiDenseVector(multi_dense) => Ok(VectorInputInternal::Vector(
            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(multi_dense)),
        )),
        rest::VectorInput::Document(_)
        | rest::VectorInput::Image(_)
        | rest::VectorInput::Object(_) => Err(StorageError::inference_error(
            "Inference is not supported in sync context",
        )),
    }
}

/// Circular dependencies prevents us from implementing `From` directly
fn context_pair_from_rest(
    value: rest::ContextPair,
) -> Result<ContextPair<VectorInputInternal>, StorageError> {
    let rest::ContextPair { positive, negative } = value;

    Ok(ContextPair {
        positive: convert_vector_input(positive)?,
        negative: convert_vector_input(negative)?,
    })
}

fn convert_vector_input_with_inferred(
    vector: rest::VectorInput,
    inferred: &BatchAccumInferred,
) -> Result<VectorInputInternal, StorageError> {
    match vector {
        rest::VectorInput::Id(id) => Ok(VectorInputInternal::Id(id)),
        rest::VectorInput::DenseVector(dense) => {
            Ok(VectorInputInternal::Vector(VectorInternal::Dense(dense)))
        }
        rest::VectorInput::SparseVector(sparse) => {
            Ok(VectorInputInternal::Vector(VectorInternal::Sparse(sparse)))
        }
        rest::VectorInput::MultiDenseVector(multi_dense) => Ok(VectorInputInternal::Vector(
            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(multi_dense)),
        )),
        rest::VectorInput::Document(doc) => {
            let data = InferenceData::Document(doc);
            let vector = inferred.get_vector(&data).ok_or_else(|| {
                StorageError::inference_error("Missing inferred vector for document")
            })?;
            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
        rest::VectorInput::Image(img) => {
            let data = InferenceData::Image(img);
            let vector = inferred.get_vector(&data).ok_or_else(|| {
                StorageError::inference_error("Missing inferred vector for image")
            })?;
            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
        rest::VectorInput::Object(obj) => {
            let data = InferenceData::Object(obj);
            let vector = inferred.get_vector(&data).ok_or_else(|| {
                StorageError::inference_error("Missing inferred vector for object")
            })?;
            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
    }
}

fn convert_query_with_inferred(
    query: rest::QueryInterface,
    inferred: &BatchAccumInferred,
) -> Result<Query, StorageError> {
    let query = rest::Query::from(query);
    match query {
        rest::Query::Nearest(nearest) => {
            let vector = convert_vector_input_with_inferred(nearest.nearest, inferred)?;
            Ok(Query::Vector(VectorQuery::Nearest(vector)))
        }
        rest::Query::Recommend(recommend) => {
            let rest::RecommendInput {
                positive,
                negative,
                strategy,
            } = recommend.recommend;
            let positives = positive
                .into_iter()
                .flatten()
                .map(|v| convert_vector_input_with_inferred(v, inferred))
                .collect::<Result<Vec<_>, _>>()?;
            let negatives = negative
                .into_iter()
                .flatten()
                .map(|v| convert_vector_input_with_inferred(v, inferred))
                .collect::<Result<Vec<_>, _>>()?;
            let reco_query = RecoQuery::new(positives, negatives);
            match strategy.unwrap_or_default() {
                rest::RecommendStrategy::AverageVector => Ok(Query::Vector(
                    VectorQuery::RecommendAverageVector(reco_query),
                )),
                rest::RecommendStrategy::BestScore => {
                    Ok(Query::Vector(VectorQuery::RecommendBestScore(reco_query)))
                }
            }
        }
        rest::Query::Discover(discover) => {
            let rest::DiscoverInput { target, context } = discover.discover;
            let target = convert_vector_input_with_inferred(target, inferred)?;
            let context = context
                .into_iter()
                .flatten()
                .map(|pair| context_pair_from_rest_with_inferred(pair, inferred))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Query::Vector(VectorQuery::Discover(DiscoveryQuery::new(
                target, context,
            ))))
        }
        rest::Query::Context(context) => {
            let rest::ContextInput(context) = context.context;
            let context = context
                .into_iter()
                .flatten()
                .map(|pair| context_pair_from_rest_with_inferred(pair, inferred))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Query::Vector(VectorQuery::Context(ContextQuery::new(
                context,
            ))))
        }
        rest::Query::OrderBy(order_by) => Ok(Query::OrderBy(OrderBy::from(order_by.order_by))),
        rest::Query::Fusion(fusion) => Ok(Query::Fusion(FusionInternal::from(fusion.fusion))),
        rest::Query::Sample(sample) => Ok(Query::Sample(SampleInternal::from(sample.sample))),
    }
}

fn convert_prefetch_with_inferred(
    prefetch: rest::Prefetch,
    inferred: &BatchAccumInferred,
) -> Result<CollectionPrefetch, StorageError> {
    let rest::Prefetch {
        prefetch,
        query,
        using,
        filter,
        score_threshold,
        params,
        limit,
        lookup_from,
    } = prefetch;

    let query = query
        .map(|q| convert_query_with_inferred(q, inferred))
        .transpose()?;
    let nested_prefetches = prefetch
        .map(|prefetches| {
            prefetches
                .into_iter()
                .map(|p| convert_prefetch_with_inferred(p, inferred))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok(CollectionPrefetch {
        prefetch: nested_prefetches,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter,
        score_threshold,
        limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params,
        lookup_from,
    })
}

fn context_pair_from_rest_with_inferred(
    value: rest::ContextPair,
    inferred: &BatchAccumInferred,
) -> Result<ContextPair<VectorInputInternal>, StorageError> {
    let rest::ContextPair { positive, negative } = value;
    Ok(ContextPair {
        positive: convert_vector_input_with_inferred(positive, inferred)?,
        negative: convert_vector_input_with_inferred(negative, inferred)?,
    })
}

#[cfg(test)]
mod tests {
    use api::rest::schema as rest;
    use segment::data_types::vectors::VectorInternal;

    use super::*;

    #[tokio::test]
    async fn test_context_pair_from_rest_dense_vectors() {
        let input = rest::ContextPair {
            positive: rest::VectorInput::DenseVector(vec![1.0, 2.0, 3.0]),
            negative: rest::VectorInput::DenseVector(vec![4.0, 5.0, 6.0]),
        };

        let result = context_pair_from_rest(input).unwrap();

        match (result.positive, result.negative) {
            (
                VectorInputInternal::Vector(VectorInternal::Dense(pos)),
                VectorInputInternal::Vector(VectorInternal::Dense(neg)),
            ) => {
                assert_eq!(pos, vec![1.0, 2.0, 3.0]);
                assert_eq!(neg, vec![4.0, 5.0, 6.0]);
            }
            _ => panic!("Expected Dense vectors"),
        }
    }

    #[tokio::test]
    async fn test_convert_vector_input_dense() {
        let input = rest::VectorInput::DenseVector(vec![1.0, 2.0, 3.0]);
        match convert_vector_input(input).unwrap() {
            VectorInputInternal::Vector(VectorInternal::Dense(vec)) => {
                assert_eq!(vec, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vector"),
        }
    }
}
