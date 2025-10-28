use api::conversions::json::json_path_from_proto;
use api::grpc::qdrant::RecommendInput;
use api::grpc::qdrant::query::Variant;
use api::grpc::{InferenceUsage, qdrant as grpc};
use api::rest::{self, LookupLocation, RecommendStrategy};
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Mmr, NearestWithMmr,
    Query, VectorInputInternal, VectorQuery,
};
use collection::operations::universal_query::formula::FormulaInternal;
use collection::operations::universal_query::shard_query::{FusionInternal, SampleInternal};
use ordered_float::OrderedFloat;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, MultiDenseVectorInternal, VectorInternal};
use segment::types::{Filter, PointIdType, SearchParams};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use tonic::Status;

use crate::common::inference::InferenceToken;
use crate::common::inference::batch_processing_grpc::{
    BatchAccumGrpc, collect_prefetch, collect_query,
};
use crate::common::inference::infer_processing::BatchAccumInferred;
use crate::common::inference::service::{InferenceData, InferenceType};

/// ToDo: this function is supposed to call an inference endpoint internally
pub async fn convert_query_point_groups_from_grpc(
    query: grpc::QueryPointGroups,
    inference_token: InferenceToken,
) -> Result<(CollectionQueryGroupsRequest, InferenceUsage), Status> {
    let grpc::QueryPointGroups {
        collection_name: _,
        prefetch,
        query,
        using,
        filter,
        params,
        score_threshold,
        with_payload,
        with_vectors,
        lookup_from,
        limit,
        group_size,
        group_by,
        with_lookup,
        read_consistency: _,
        timeout: _,
        shard_key_selector: _,
    } = query;

    let mut batch = BatchAccumGrpc::new();

    if let Some(q) = &query {
        collect_query(q, &mut batch)?;
    }

    for p in &prefetch {
        collect_prefetch(p, &mut batch)?;
    }

    let BatchAccumGrpc { objects } = batch;

    let (inferred, usage) =
        BatchAccumInferred::from_objects(objects, InferenceType::Search, inference_token)
            .await
            .map_err(|e| Status::internal(format!("Inference error: {e}")))?;

    let query = if let Some(q) = query {
        Some(convert_query_with_inferred(q, &inferred)?)
    } else {
        None
    };

    let prefetch = prefetch
        .into_iter()
        .map(|p| convert_prefetch_with_inferred(p, &inferred))
        .collect::<Result<Vec<_>, _>>()?;

    let request = CollectionQueryGroupsRequest {
        prefetch,
        query,
        using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_owned()),
        filter: filter.map(TryFrom::try_from).transpose()?,
        score_threshold,
        with_vector: with_vectors
            .map(From::from)
            .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
        with_payload: with_payload
            .map(TryFrom::try_from)
            .transpose()?
            .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
        lookup_from: lookup_from.map(LookupLocation::try_from).transpose()?,
        group_by: json_path_from_proto(&group_by)?,
        group_size: group_size
            .map(|s| s as usize)
            .unwrap_or(CollectionQueryRequest::DEFAULT_GROUP_SIZE),
        limit: limit
            .map(|l| l as usize)
            .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params: params.map(From::from),
        with_lookup: with_lookup.map(TryFrom::try_from).transpose()?,
    };

    Ok((request, usage.unwrap_or_default().into()))
}

/// ToDo: this function is supposed to call an inference endpoint internally
pub async fn convert_query_points_from_grpc(
    query: grpc::QueryPoints,
    inference_token: InferenceToken,
) -> Result<(CollectionQueryRequest, InferenceUsage), Status> {
    let grpc::QueryPoints {
        collection_name: _,
        prefetch,
        query,
        using,
        filter,
        params,
        score_threshold,
        limit,
        offset,
        with_payload,
        with_vectors,
        read_consistency: _,
        shard_key_selector: _,
        lookup_from,
        timeout: _,
    } = query;

    let mut batch = BatchAccumGrpc::new();

    if let Some(q) = &query {
        collect_query(q, &mut batch)?;
    }

    for p in &prefetch {
        collect_prefetch(p, &mut batch)?;
    }

    let BatchAccumGrpc { objects } = batch;

    let (inferred, usage) =
        BatchAccumInferred::from_objects(objects, InferenceType::Search, inference_token)
            .await
            .map_err(|e| Status::internal(format!("Inference error: {e}")))?;

    let prefetch = prefetch
        .into_iter()
        .map(|p| convert_prefetch_with_inferred(p, &inferred))
        .collect::<Result<Vec<_>, _>>()?;

    let query = query
        .map(|q| convert_query_with_inferred(q, &inferred))
        .transpose()?;

    Ok((
        CollectionQueryRequest {
            prefetch,
            query,
            using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_owned()),
            filter: filter.map(TryFrom::try_from).transpose()?,
            score_threshold,
            limit: limit
                .map(|l| l as usize)
                .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
            offset: offset
                .map(|o| o as usize)
                .unwrap_or(CollectionQueryRequest::DEFAULT_OFFSET),
            params: params.map(From::from),
            with_vector: with_vectors
                .map(From::from)
                .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
            with_payload: with_payload
                .map(TryFrom::try_from)
                .transpose()?
                .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
            lookup_from: lookup_from.map(LookupLocation::try_from).transpose()?,
        },
        usage.unwrap_or_default().into(),
    ))
}

fn convert_prefetch_with_inferred(
    prefetch: grpc::PrefetchQuery,
    inferred: &BatchAccumInferred,
) -> Result<CollectionPrefetch, Status> {
    let grpc::PrefetchQuery {
        prefetch,
        query,
        using,
        filter,
        params,
        score_threshold,
        limit,
        lookup_from,
    } = prefetch;

    let nested_prefetches = prefetch
        .into_iter()
        .map(|p| convert_prefetch_with_inferred(p, inferred))
        .collect::<Result<Vec<_>, _>>()?;

    let query = query
        .map(|q| convert_query_with_inferred(q, inferred))
        .transpose()?;

    Ok(CollectionPrefetch {
        prefetch: nested_prefetches,
        query,
        using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_owned()),
        filter: filter.map(Filter::try_from).transpose()?,
        score_threshold: score_threshold.map(OrderedFloat),
        limit: limit
            .map(|l| l as usize)
            .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params: params.map(SearchParams::from),
        lookup_from: lookup_from.map(LookupLocation::try_from).transpose()?,
    })
}

fn convert_query_with_inferred(
    query: grpc::Query,
    inferred: &BatchAccumInferred,
) -> Result<Query, Status> {
    let variant = query
        .variant
        .ok_or_else(|| Status::invalid_argument("Query variant is missing"))?;

    let query = match variant {
        Variant::Nearest(nearest) => {
            let vector = convert_vector_input_with_inferred(nearest, inferred)?;
            Query::Vector(VectorQuery::Nearest(vector))
        }
        Variant::Recommend(recommend) => {
            let RecommendInput {
                positive,
                negative,
                strategy,
            } = recommend;

            let positives = positive
                .into_iter()
                .map(|v| convert_vector_input_with_inferred(v, inferred))
                .collect::<Result<Vec<_>, _>>()?;

            let negatives = negative
                .into_iter()
                .map(|v| convert_vector_input_with_inferred(v, inferred))
                .collect::<Result<Vec<_>, _>>()?;

            let reco_query = RecoQuery::new(positives, negatives);

            let strategy = strategy
                .and_then(|x| grpc::RecommendStrategy::try_from(x).ok())
                .map(RecommendStrategy::from)
                .unwrap_or_default();

            match strategy {
                RecommendStrategy::AverageVector => {
                    Query::Vector(VectorQuery::RecommendAverageVector(reco_query))
                }
                RecommendStrategy::BestScore => {
                    Query::Vector(VectorQuery::RecommendBestScore(reco_query))
                }
                RecommendStrategy::SumScores => {
                    Query::Vector(VectorQuery::RecommendSumScores(reco_query))
                }
            }
        }
        Variant::Discover(discover) => {
            let grpc::DiscoverInput { target, context } = discover;

            let target = target
                .map(|t| convert_vector_input_with_inferred(t, inferred))
                .transpose()?
                .ok_or_else(|| Status::invalid_argument("DiscoverInput target is missing"))?;

            let grpc::ContextInput { pairs } = context
                .ok_or_else(|| Status::invalid_argument("DiscoverInput context is missing"))?;

            let context = pairs
                .into_iter()
                .map(|pair| context_pair_from_grpc_with_inferred(pair, inferred))
                .collect::<Result<_, _>>()?;

            Query::Vector(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
        }
        Variant::Context(context) => {
            let context_query = context_query_from_grpc_with_inferred(context, inferred)?;
            Query::Vector(VectorQuery::Context(context_query))
        }
        Variant::OrderBy(order_by) => Query::OrderBy(OrderBy::try_from(order_by)?),
        Variant::Fusion(fusion) => Query::Fusion(FusionInternal::try_from(fusion)?),
        Variant::Rrf(rrf) => Query::Fusion(FusionInternal::try_from(rrf)?),
        Variant::Formula(formula) => Query::Formula(FormulaInternal::try_from(formula)?),
        Variant::Sample(sample) => Query::Sample(SampleInternal::try_from(sample)?),
        Variant::NearestWithMmr(grpc::NearestInputWithMmr { nearest, mmr }) => {
            let nearest =
                nearest.ok_or_else(|| Status::invalid_argument("nearest vector is missing"))?;
            let nearest = convert_vector_input_with_inferred(nearest, inferred)?;

            let mmr = mmr.ok_or_else(|| Status::invalid_argument("mmr is missing"))?;
            let grpc::Mmr {
                diversity,
                candidates_limit,
            } = mmr;
            let mmr = Mmr {
                diversity,
                candidates_limit: candidates_limit.map(|x| x as usize),
            };

            Query::Vector(VectorQuery::NearestWithMmr(NearestWithMmr { nearest, mmr }))
        }
    };

    Ok(query)
}

fn convert_vector_input_with_inferred(
    vector: grpc::VectorInput,
    inferred: &BatchAccumInferred,
) -> Result<VectorInputInternal, Status> {
    use api::grpc::qdrant::vector_input::Variant;

    let variant = vector
        .variant
        .ok_or_else(|| Status::invalid_argument("VectorInput variant is missing"))?;

    match variant {
        Variant::Id(id) => Ok(VectorInputInternal::Id(PointIdType::try_from(id)?)),
        Variant::Dense(dense) => Ok(VectorInputInternal::Vector(VectorInternal::Dense(
            From::from(dense),
        ))),
        Variant::Sparse(sparse) => Ok(VectorInputInternal::Vector(VectorInternal::Sparse(
            From::from(sparse),
        ))),
        Variant::MultiDense(multi_dense) => Ok(VectorInputInternal::Vector(
            VectorInternal::MultiDense(MultiDenseVectorInternal::from(multi_dense)),
        )),
        Variant::Document(doc) => {
            let doc: rest::Document = doc
                .try_into()
                .map_err(|e| Status::internal(format!("Document conversion error: {e}")))?;
            let data = InferenceData::Document(doc);
            let vector = inferred
                .get_vector(&data)
                .ok_or_else(|| Status::internal("Missing inferred vector for document"))?;

            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
        Variant::Image(img) => {
            let img: rest::Image = img
                .try_into()
                .map_err(|e| Status::internal(format!("Image conversion error: {e}",)))?;
            let data = InferenceData::Image(img);

            let vector = inferred
                .get_vector(&data)
                .ok_or_else(|| Status::internal("Missing inferred vector for image"))?;

            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
        Variant::Object(obj) => {
            let obj: rest::InferenceObject = obj
                .try_into()
                .map_err(|e| Status::internal(format!("Object conversion error: {e}")))?;
            let data = InferenceData::Object(obj);
            let vector = inferred
                .get_vector(&data)
                .ok_or_else(|| Status::internal("Missing inferred vector for object"))?;

            Ok(VectorInputInternal::Vector(VectorInternal::from(
                vector.clone(),
            )))
        }
    }
}

fn context_query_from_grpc_with_inferred(
    value: grpc::ContextInput,
    inferred: &BatchAccumInferred,
) -> Result<ContextQuery<VectorInputInternal>, Status> {
    let grpc::ContextInput { pairs } = value;

    Ok(ContextQuery {
        pairs: pairs
            .into_iter()
            .map(|pair| context_pair_from_grpc_with_inferred(pair, inferred))
            .collect::<Result<_, _>>()?,
    })
}

fn context_pair_from_grpc_with_inferred(
    value: grpc::ContextInputPair,
    inferred: &BatchAccumInferred,
) -> Result<ContextPair<VectorInputInternal>, Status> {
    let grpc::ContextInputPair { positive, negative } = value;

    let positive =
        positive.ok_or_else(|| Status::invalid_argument("ContextPair positive is missing"))?;
    let negative =
        negative.ok_or_else(|| Status::invalid_argument("ContextPair negative is missing"))?;

    Ok(ContextPair {
        positive: convert_vector_input_with_inferred(positive, inferred)?,
        negative: convert_vector_input_with_inferred(negative, inferred)?,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::grpc::qdrant::Value;
    use api::grpc::qdrant::value::Kind;
    use api::grpc::qdrant::vector_input::Variant;
    use collection::operations::point_ops::VectorPersisted;

    use super::*;

    fn create_test_document() -> api::grpc::qdrant::Document {
        api::grpc::qdrant::Document {
            text: "test".to_string(),
            model: "test-model".to_string(),
            options: HashMap::new(),
        }
    }

    fn create_test_image() -> api::grpc::qdrant::Image {
        api::grpc::qdrant::Image {
            image: Some(Value {
                kind: Some(Kind::StringValue("test.jpg".to_string())),
            }),
            model: "test-model".to_string(),
            options: HashMap::new(),
        }
    }

    fn create_test_object() -> api::grpc::qdrant::InferenceObject {
        api::grpc::qdrant::InferenceObject {
            object: Some(Value {
                kind: Some(Kind::StringValue("test".to_string())),
            }),
            model: "test-model".to_string(),
            options: HashMap::new(),
        }
    }

    fn create_test_inferred_batch() -> BatchAccumInferred {
        let mut objects = HashMap::new();

        let grpc_doc = create_test_document();
        let grpc_img = create_test_image();
        let grpc_obj = create_test_object();

        let doc: rest::Document = grpc_doc.try_into().unwrap();
        let img: rest::Image = grpc_img.try_into().unwrap();
        let obj: rest::InferenceObject = grpc_obj.try_into().unwrap();

        let doc_data = InferenceData::Document(doc);
        let img_data = InferenceData::Image(img);
        let obj_data = InferenceData::Object(obj);

        let dense_vector = vec![1.0, 2.0, 3.0];
        let vector_persisted = VectorPersisted::Dense(dense_vector);

        objects.insert(doc_data, vector_persisted.clone());
        objects.insert(img_data, vector_persisted.clone());
        objects.insert(obj_data, vector_persisted);

        BatchAccumInferred { objects }
    }

    #[test]
    fn test_convert_vector_input_with_inferred_dense() {
        let inferred = create_test_inferred_batch();
        let vector = grpc::VectorInput {
            variant: Some(Variant::Dense(grpc::DenseVector {
                data: vec![1.0, 2.0, 3.0],
            })),
        };

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
        let doc = create_test_document();
        let vector = grpc::VectorInput {
            variant: Some(Variant::Document(doc)),
        };

        let result = convert_vector_input_with_inferred(vector, &inferred).unwrap();
        match result {
            VectorInputInternal::Vector(VectorInternal::Dense(values)) => {
                assert_eq!(values, vec![1.0, 2.0, 3.0]);
            }
            _ => panic!("Expected dense vector from inference"),
        }
    }

    #[test]
    fn test_convert_vector_input_missing_variant() {
        let inferred = create_test_inferred_batch();
        let vector = grpc::VectorInput { variant: None };

        let result = convert_vector_input_with_inferred(vector, &inferred);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("variant is missing"));
    }

    #[test]
    fn test_context_pair_from_grpc_with_inferred() {
        let inferred = create_test_inferred_batch();
        let pair = grpc::ContextInputPair {
            positive: Some(grpc::VectorInput {
                variant: Some(Variant::Dense(grpc::DenseVector {
                    data: vec![1.0, 2.0, 3.0],
                })),
            }),
            negative: Some(grpc::VectorInput {
                variant: Some(Variant::Document(create_test_document())),
            }),
        };

        let result = context_pair_from_grpc_with_inferred(pair, &inferred).unwrap();
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
    fn test_context_pair_missing_vectors() {
        let inferred = create_test_inferred_batch();
        let pair = grpc::ContextInputPair {
            positive: None,
            negative: Some(grpc::VectorInput {
                variant: Some(Variant::Document(create_test_document())),
            }),
        };

        let result = context_pair_from_grpc_with_inferred(pair, &inferred);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message()
                .contains("positive is missing"),
        );
    }
}
