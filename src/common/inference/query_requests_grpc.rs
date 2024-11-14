use api::conversions::json::json_path_from_proto;
use api::grpc::qdrant as grpc;
use api::grpc::qdrant::query::Variant;
use api::grpc::qdrant::RecommendInput;
use api::rest;
use api::rest::RecommendStrategy;
use collection::operations::universal_query::collection_query::{
    CollectionPrefetch, CollectionQueryGroupsRequest, CollectionQueryRequest, Query,
    VectorInputInternal, VectorQuery,
};
use collection::operations::universal_query::shard_query::{FusionInternal, SampleInternal};
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{VectorInternal, DEFAULT_VECTOR_NAME};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};
use tonic::Status;

use crate::common::inference::batch_processing_grpc::{
    collect_prefetch, collect_query, BatchAccumGrpc,
};
use crate::common::inference::infer_processing::BatchAccumInferred;
use crate::common::inference::service::{InferenceData, InferenceType};

/// ToDo: this function is supposed to call an inference endpoint internally
pub async fn convert_query_point_groups_from_grpc(
    query: grpc::QueryPointGroups,
) -> Result<CollectionQueryGroupsRequest, Status> {
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

    let inferred = BatchAccumInferred::from_objects(objects, InferenceType::Search)
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
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter: filter.map(TryFrom::try_from).transpose()?,
        score_threshold,
        with_vector: with_vectors
            .map(From::from)
            .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
        with_payload: with_payload
            .map(TryFrom::try_from)
            .transpose()?
            .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
        lookup_from: lookup_from.map(From::from),
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

    Ok(request)
}

/// ToDo: this function is supposed to call an inference endpoint internally
pub async fn convert_query_points_from_grpc(
    query: grpc::QueryPoints,
) -> Result<CollectionQueryRequest, Status> {
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

    let inferred = BatchAccumInferred::from_objects(objects, InferenceType::Search)
        .await
        .map_err(|e| Status::internal(format!("Inference error: {e}")))?;

    let prefetch = prefetch
        .into_iter()
        .map(|p| convert_prefetch_with_inferred(p, &inferred))
        .collect::<Result<Vec<_>, _>>()?;

    let query = query
        .map(|q| convert_query_with_inferred(q, &inferred))
        .transpose()?;

    Ok(CollectionQueryRequest {
        prefetch,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
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
        lookup_from: lookup_from.map(From::from),
    })
}

fn convert_prefetch_query(query: grpc::PrefetchQuery) -> Result<CollectionPrefetch, Status> {
    let grpc::PrefetchQuery {
        prefetch,
        query,
        using,
        filter,
        params,
        score_threshold,
        limit,
        lookup_from,
    } = query;

    let prefetch: Result<_, _> = prefetch.into_iter().map(convert_prefetch_query).collect();

    let query = query.map(convert_query).transpose()?;

    let collection_query = CollectionPrefetch {
        prefetch: prefetch?,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter: filter.map(TryFrom::try_from).transpose()?,
        score_threshold,
        limit: limit
            .map(|l| l as usize)
            .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params: params.map(From::from),
        lookup_from: lookup_from.map(From::from),
    };

    Ok(collection_query)
}

fn convert_query(query: grpc::Query) -> Result<Query, Status> {
    use api::grpc::qdrant::query::Variant;

    let variant = query
        .variant
        .ok_or_else(|| Status::invalid_argument("Query variant is missing"))?;

    let query = match variant {
        Variant::Nearest(nearest) => {
            Query::Vector(VectorQuery::Nearest(convert_vector_input(nearest)?))
        }
        Variant::Recommend(recommend) => Query::Vector(convert_recommend_input(recommend)?),
        Variant::Discover(discover) => Query::Vector(convert_discover_input(discover)?),
        Variant::Context(context) => Query::Vector(convert_context_input(context)?),
        Variant::OrderBy(order_by) => Query::OrderBy(OrderBy::try_from(order_by)?),
        Variant::Fusion(fusion) => Query::Fusion(FusionInternal::try_from(fusion)?),
        Variant::Sample(sample) => Query::Sample(SampleInternal::try_from(sample)?),
    };

    Ok(query)
}

fn convert_recommend_input(
    value: grpc::RecommendInput,
) -> Result<VectorQuery<VectorInputInternal>, Status> {
    let RecommendInput {
        positive,
        negative,
        strategy,
    } = value;

    let positives = positive
        .into_iter()
        .map(convert_vector_input)
        .collect::<Result<Vec<_>, _>>()?;
    let negatives = negative
        .into_iter()
        .map(convert_vector_input)
        .collect::<Result<Vec<_>, _>>()?;

    let reco_query = RecoQuery::new(positives, negatives);

    let strategy = strategy
        .and_then(|x|
            // XXX: Invalid values silently converted to None
            grpc::RecommendStrategy::try_from(x).ok())
        .map(RecommendStrategy::from)
        .unwrap_or_default();

    let query = match strategy {
        RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
        RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
    };

    Ok(query)
}

fn convert_discover_input(
    value: grpc::DiscoverInput,
) -> Result<VectorQuery<VectorInputInternal>, Status> {
    let grpc::DiscoverInput { target, context } = value;

    let target = target.map(convert_vector_input).transpose()?;

    let target =
        target.ok_or_else(|| Status::invalid_argument("DiscoverInput target is missing"))?;

    let grpc::ContextInput { pairs } =
        context.ok_or_else(|| Status::invalid_argument("DiscoverInput context is missing"))?;

    let context = pairs
        .into_iter()
        .map(context_pair_from_grpc)
        .collect::<Result<_, _>>()?;

    Ok(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
}

fn convert_context_input(
    value: grpc::ContextInput,
) -> Result<VectorQuery<VectorInputInternal>, Status> {
    let context_query = context_query_from_grpc(value)?;

    Ok(VectorQuery::Context(context_query))
}

fn convert_vector_input(value: grpc::VectorInput) -> Result<VectorInputInternal, Status> {
    use api::grpc::qdrant::vector_input::Variant;

    let variant = value
        .variant
        .ok_or_else(|| Status::invalid_argument("VectorInput variant is missing"))?;

    let vector_input = match variant {
        Variant::Id(id) => VectorInputInternal::Id(TryFrom::try_from(id)?),
        Variant::Dense(dense) => {
            VectorInputInternal::Vector(VectorInternal::Dense(From::from(dense)))
        }
        Variant::Sparse(sparse) => {
            VectorInputInternal::Vector(VectorInternal::Sparse(From::from(sparse)))
        }
        Variant::MultiDense(multi_dense) => VectorInputInternal::Vector(
            // TODO(universal-query): Validate at API level
            VectorInternal::MultiDense(From::from(multi_dense)),
        ),
        Variant::Document(_) => {
            return Err(Status::invalid_argument(
                "Document inference is not implemented",
            ))
        }
        Variant::Image(_) => {
            return Err(Status::invalid_argument(
                "Image inference is not implemented",
            ))
        }
        Variant::Object(_) => {
            return Err(Status::invalid_argument(
                "Object inference is not implemented",
            ))
        }
    };

    Ok(vector_input)
}

/// Circular dependencies prevents us from implementing `TryFrom` directly
fn context_query_from_grpc(
    value: grpc::ContextInput,
) -> Result<ContextQuery<VectorInputInternal>, Status> {
    let grpc::ContextInput { pairs } = value;

    Ok(ContextQuery {
        pairs: pairs
            .into_iter()
            .map(context_pair_from_grpc)
            .collect::<Result<_, _>>()?,
    })
}

/// Circular dependencies prevents us from implementing `TryFrom` directly
fn context_pair_from_grpc(
    value: grpc::ContextInputPair,
) -> Result<ContextPair<VectorInputInternal>, Status> {
    let grpc::ContextInputPair { positive, negative } = value;

    let positive =
        positive.ok_or_else(|| Status::invalid_argument("ContextPair positive is missing"))?;
    let negative =
        negative.ok_or_else(|| Status::invalid_argument("ContextPair negative is missing"))?;

    Ok(ContextPair {
        positive: convert_vector_input(positive)?,
        negative: convert_vector_input(negative)?,
    })
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
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter: filter.map(TryFrom::try_from).transpose()?,
        score_threshold,
        limit: limit
            .map(|l| l as usize)
            .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params: params.map(From::from),
        lookup_from: lookup_from.map(From::from),
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
        Variant::Sample(sample) => Query::Sample(SampleInternal::try_from(sample)?),
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
        Variant::Id(id) => Ok(VectorInputInternal::Id(TryFrom::try_from(id)?)),
        Variant::Dense(dense) => Ok(VectorInputInternal::Vector(VectorInternal::Dense(
            From::from(dense),
        ))),
        Variant::Sparse(sparse) => Ok(VectorInputInternal::Vector(VectorInternal::Sparse(
            From::from(sparse),
        ))),
        Variant::MultiDense(multi_dense) => Ok(VectorInputInternal::Vector(
            VectorInternal::MultiDense(From::from(multi_dense)),
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
