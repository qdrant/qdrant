use api::rest::schema as rest;
use collection::lookup::WithLookup;
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
    VectorInputInternal, VectorQuery,
};
use collection::operations::universal_query::shard_query::{FusionInternal, SampleInternal};
use futures_util::future::try_join_all;
use log::{debug, error, warn};
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{MultiDenseVectorInternal, VectorInternal, DEFAULT_VECTOR_NAME};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use storage::content_manager::errors::StorageError;

use crate::common::inference::service::{InferenceData, InferenceService, InferenceType};

pub async fn convert_query_groups_request_from_rest(
    request: rest::QueryGroupsRequestInternal,
) -> Result<CollectionQueryGroupsRequest, StorageError> {
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

    let query = if let Some(q) = query {
        Some(convert_query(q).await?)
    } else {
        None
    };

    let prefetch = if let Some(prefetches) = prefetch {
        try_join_all(prefetches.into_iter().map(convert_collection_prefetch)).await?
    } else {
        Vec::new()
    };

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

    let prefetch = if let Some(prefetches) = prefetch {
        try_join_all(prefetches.into_iter().map(convert_collection_prefetch)).await?
    } else {
        Vec::new()
    };

    let query = if let Some(q) = query {
        Some(convert_query(q).await?)
    } else {
        None
    };

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

async fn convert_collection_prefetch(
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

    let query = match query {
        Some(q) => Some(convert_query(q).await?),
        None => None,
    };

    let prefetch = try_join_all(
        prefetch
            .into_iter()
            .flatten()
            .map(convert_collection_prefetch),
    )
    .await?;

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

async fn convert_query(query: rest::QueryInterface) -> Result<Query, StorageError> {
    let query = rest::Query::from(query);

    match query {
        rest::Query::Nearest(nearest) => Ok(Query::Vector(VectorQuery::Nearest(
            convert_vector_input(nearest.nearest).await?,
        ))),
        rest::Query::Recommend(recommend) => Ok(Query::Vector(
            convert_recommend_input(recommend.recommend).await?,
        )),
        rest::Query::Discover(discover) => Ok(Query::Vector(
            convert_discover_input(discover.discover).await?,
        )),
        rest::Query::Context(context) => {
            Ok(Query::Vector(convert_context_input(context.context).await?))
        }
        rest::Query::OrderBy(order_by) => Ok(Query::OrderBy(OrderBy::from(order_by.order_by))),
        rest::Query::Fusion(fusion) => Ok(Query::Fusion(FusionInternal::from(fusion.fusion))),
        rest::Query::Sample(sample) => Ok(Query::Sample(SampleInternal::from(sample.sample))),
    }
}

async fn convert_recommend_input(
    recommend: rest::RecommendInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::RecommendInput {
        positive,
        negative,
        strategy,
    } = recommend;

    let positives = try_join_all(positive.into_iter().flatten().map(convert_vector_input)).await?;
    let negatives = try_join_all(negative.into_iter().flatten().map(convert_vector_input)).await?;

    let reco_query = RecoQuery::new(positives, negatives);

    match strategy.unwrap_or_default() {
        rest::RecommendStrategy::AverageVector => {
            Ok(VectorQuery::RecommendAverageVector(reco_query))
        }
        rest::RecommendStrategy::BestScore => Ok(VectorQuery::RecommendBestScore(reco_query)),
    }
}

async fn convert_discover_input(
    discover: rest::DiscoverInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::DiscoverInput { target, context } = discover;

    let target = convert_vector_input(target).await?;
    let context = try_join_all(
        context
            .into_iter()
            .flatten()
            .map(context_pair_from_rest)
            .collect::<Vec<_>>(),
    )
    .await?;
    Ok(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
}

async fn convert_context_input(
    context: rest::ContextInput,
) -> Result<VectorQuery<VectorInputInternal>, StorageError> {
    let rest::ContextInput(context) = context;

    let context = try_join_all(context.into_iter().flatten().map(context_pair_from_rest)).await?;
    Ok(VectorQuery::Context(ContextQuery::new(context)))
}

async fn convert_vector_input(
    vector: rest::VectorInput,
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
            // TODO(universal-query): Validate at API level
            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(multi_dense)),
        )),
        rest::VectorInput::Document(doc) => {
            process_inference_input(InferenceData::Document(doc.clone()), "document", Some(doc))
                .await
        }
        rest::VectorInput::Image(img) => {
            process_inference_input(InferenceData::Image(img.clone()), "image", Some(img)).await
        }
        rest::VectorInput::Object(obj) => {
            process_inference_input(InferenceData::Object(obj.clone()), "object", Some(obj)).await
        }
    }
}

/// Circular dependencies prevents us from implementing `From` directly
async fn context_pair_from_rest(
    value: rest::ContextPair,
) -> Result<ContextPair<VectorInputInternal>, StorageError> {
    let rest::ContextPair { positive, negative } = value;

    Ok(ContextPair {
        positive: convert_vector_input(positive).await?,
        negative: convert_vector_input(negative).await?,
    })
}

async fn process_inference_input<T: std::fmt::Debug>(
    data: InferenceData,
    input_type: &str,
    debug_content: Option<T>,
) -> Result<VectorInputInternal, StorageError> {
    let service = InferenceService::global().clone().ok_or_else(|| {
        error!("InferenceService not initialized for {input_type}");
        StorageError::inference_error(format!(
            "InferenceService not initialized for {input_type} processing"
        ))
    })?;

    debug!("Starting inference processing for {input_type}");

    let vectors = service
        .infer(data, InferenceType::Search)
        .await
        .map_err(|e| {
            error!(
                "Failed to process {input_type}: {error}",
                error = e.to_string()
            );
            StorageError::inference_error(format!("Failed to process {input_type}: {e}"))
        })?;

    if vectors.is_empty() {
        // log the content too in case of empty vector
        if let Some(content) = debug_content {
            warn!("Empty vector array for {input_type}. Content: {content:?}");
        } else {
            warn!("Empty vector array for {input_type}");
        }

        return Err(StorageError::inference_error(format!(
            "Inference service returned empty result for {input_type}. This might indicate an issue with the input format or the inference service configuration."
        )));
    }

    if vectors.len() > 1 {
        warn!(
            "Multiple vectors returned for {input_type}. Count: {count}. Using only the first one",
            count = vectors.len()
        );
    }

    debug!(
        "Successfully processed {input_type} inference. Vector count: {count}",
        count = vectors.len()
    );

    Ok(VectorInputInternal::Vector(VectorInternal::from(
        vectors.into_iter().next().unwrap(),
    )))
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

        let result = context_pair_from_rest(input).await.unwrap();

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
        match convert_vector_input(input).await.unwrap() {
            VectorInputInternal::Vector(VectorInternal::Dense(vec)) => {
                assert_eq!(vec, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vector"),
        }
    }
}
