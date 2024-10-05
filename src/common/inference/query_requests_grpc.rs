use api::grpc::conversions::json_path_from_proto;
use api::grpc::qdrant as grpc;
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

    let prefetch: Result<_, _> = prefetch.into_iter().map(convert_prefetch_query).collect();

    let query = query.map(convert_query).transpose()?;

    let request = CollectionQueryGroupsRequest {
        prefetch: prefetch?,
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

    let prefetch: Result<_, _> = prefetch.into_iter().map(convert_prefetch_query).collect();

    let query = query.map(convert_query).transpose()?;

    let request = CollectionQueryRequest {
        prefetch: prefetch?,
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
    };
    Ok(request)
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
    let grpc::RecommendInput {
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
