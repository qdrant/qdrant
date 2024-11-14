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

use crate::common::inference::batch_processing::{
    collect_query_groups_request, collect_query_request,
};
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
    let batch = collect_query_request(&request);
    let inferred = BatchAccumInferred::from_batch_accum(batch, InferenceType::Search).await?;
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
                .map(|p| convert_prefetch_with_inferred(p, &inferred))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    let query = query
        .map(|q| convert_query_with_inferred(q, &inferred))
        .transpose()?;

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
    use std::collections::HashMap;

    use api::rest::schema::{Document, Image, InferenceObject, NearestQuery};
    use collection::operations::point_ops::VectorPersisted;
    use serde_json::json;

    use super::*;

    fn create_test_document(text: &str) -> Document {
        Document {
            text: text.to_string(),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    fn create_test_image(url: &str) -> Image {
        Image {
            image: json!({"data": url.to_string()}),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    fn create_test_object(data: &str) -> InferenceObject {
        InferenceObject {
            object: json!({"data": data}),
            model: "test-model".to_string(),
            options: Default::default(),
        }
    }

    fn create_test_inferred_batch() -> BatchAccumInferred {
        let mut objects = HashMap::new();

        let doc = InferenceData::Document(create_test_document("test"));
        let img = InferenceData::Image(create_test_image("test.jpg"));
        let obj = InferenceData::Object(create_test_object("test"));

        let dense_vector = vec![1.0, 2.0, 3.0];
        let vector_persisted = VectorPersisted::Dense(dense_vector);

        objects.insert(doc, vector_persisted.clone());
        objects.insert(img, vector_persisted.clone());
        objects.insert(obj, vector_persisted);

        BatchAccumInferred { objects }
    }

    #[test]
    fn test_convert_vector_input_with_inferred_dense() {
        let inferred = create_test_inferred_batch();
        let vector = rest::VectorInput::DenseVector(vec![1.0, 2.0, 3.0]);

        let result = convert_vector_input_with_inferred(vector, &inferred).unwrap();
        match result {
            VectorInputInternal::Vector(VectorInternal::Dense(values)) => {
                assert_eq!(values, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vector"),
        }
    }

    #[test]
    fn test_convert_vector_input_with_inferred_document() {
        let inferred = create_test_inferred_batch();
        let doc = create_test_document("test");
        let vector = rest::VectorInput::Document(doc);

        let result = convert_vector_input_with_inferred(vector, &inferred).unwrap();
        match result {
            VectorInputInternal::Vector(VectorInternal::Dense(values)) => {
                assert_eq!(values, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vector from inference"),
        }
    }

    #[test]
    fn test_convert_vector_input_with_inferred_missing() {
        let inferred = create_test_inferred_batch();
        let doc = create_test_document("missing");
        let vector = rest::VectorInput::Document(doc);

        let result = convert_vector_input_with_inferred(vector, &inferred);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing inferred vector"));
    }

    #[test]
    fn test_context_pair_from_rest_with_inferred() {
        let inferred = create_test_inferred_batch();
        let pair = rest::ContextPair {
            positive: rest::VectorInput::DenseVector(vec![1.0, 2.0, 3.0]),
            negative: rest::VectorInput::Document(create_test_document("test")),
        };

        let result = context_pair_from_rest_with_inferred(pair, &inferred).unwrap();
        match (result.positive, result.negative) {
            (
                VectorInputInternal::Vector(VectorInternal::Dense(pos)),
                VectorInputInternal::Vector(VectorInternal::Dense(neg)),
            ) => {
                assert_eq!(pos, vec![1.0, 2.0, 3.0]);
                assert_eq!(neg, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vectors"),
        }
    }

    #[test]
    fn test_convert_query_with_inferred_nearest() {
        let inferred = create_test_inferred_batch();
        let nearest = NearestQuery {
            nearest: rest::VectorInput::Document(create_test_document("test")),
        };
        let query = rest::QueryInterface::Query(rest::Query::Nearest(nearest));

        let result = convert_query_with_inferred(query, &inferred).unwrap();
        match result {
            Query::Vector(VectorQuery::Nearest(vector)) => match vector {
                VectorInputInternal::Vector(VectorInternal::Dense(values)) => {
                    assert_eq!(values, vec![1.0, 2.0, 3.0]);
                }
                _ => panic!("Expected dense vector"),
            },
            _ => panic!("Expected nearest query"),
        }
    }
}
