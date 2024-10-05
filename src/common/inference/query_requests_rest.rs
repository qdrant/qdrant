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

    let prefetch = prefetch
        .into_iter()
        .flatten()
        .map(convert_collection_prefetch)
        .collect();

    let query = query.map(convert_query);

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
        .into_iter()
        .flatten()
        .map(convert_collection_prefetch)
        .collect();

    let query = query.map(convert_query);

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

fn convert_collection_prefetch(prefetch: rest::Prefetch) -> CollectionPrefetch {
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

    let prefetch = prefetch
        .into_iter()
        .flatten()
        .map(convert_collection_prefetch)
        .collect();

    let query = query.map(convert_query);

    CollectionPrefetch {
        prefetch,
        query,
        using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
        filter,
        score_threshold,
        limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
        params,
        lookup_from,
    }
}

fn convert_query(query: rest::QueryInterface) -> Query {
    let query = rest::Query::from(query);

    match query {
        rest::Query::Nearest(nearest) => {
            Query::Vector(VectorQuery::Nearest(convert_vector_input(nearest.nearest)))
        }
        rest::Query::Recommend(recommend) => {
            Query::Vector(convert_recommend_input(recommend.recommend))
        }
        rest::Query::Discover(discover) => Query::Vector(convert_discover_input(discover.discover)),
        rest::Query::Context(context) => Query::Vector(convert_context_input(context.context)),
        rest::Query::OrderBy(order_by) => Query::OrderBy(OrderBy::from(order_by.order_by)),
        rest::Query::Fusion(fusion) => Query::Fusion(FusionInternal::from(fusion.fusion)),
        rest::Query::Sample(sample) => Query::Sample(SampleInternal::from(sample.sample)),
    }
}

fn convert_recommend_input(recommend: rest::RecommendInput) -> VectorQuery<VectorInputInternal> {
    let rest::RecommendInput {
        positive,
        negative,
        strategy,
    } = recommend;

    let positives = positive
        .into_iter()
        .flatten()
        .map(convert_vector_input)
        .collect();
    let negatives = negative
        .into_iter()
        .flatten()
        .map(convert_vector_input)
        .collect();
    let reco_query = RecoQuery::new(positives, negatives);

    match strategy.unwrap_or_default() {
        rest::RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
        rest::RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
    }
}

fn convert_discover_input(discover: rest::DiscoverInput) -> VectorQuery<VectorInputInternal> {
    let rest::DiscoverInput { target, context } = discover;

    let target = convert_vector_input(target);
    let context = context
        .into_iter()
        .flatten()
        .map(context_pair_from_rest)
        .collect();

    VectorQuery::Discover(DiscoveryQuery::new(target, context))
}

fn convert_context_input(context: rest::ContextInput) -> VectorQuery<VectorInputInternal> {
    let rest::ContextInput(context) = context;

    let context = context
        .into_iter()
        .flatten()
        .map(context_pair_from_rest)
        .collect();

    VectorQuery::Context(ContextQuery::new(context))
}

fn convert_vector_input(vector: rest::VectorInput) -> VectorInputInternal {
    match vector {
        rest::VectorInput::Id(id) => VectorInputInternal::Id(id),
        rest::VectorInput::DenseVector(dense) => {
            VectorInputInternal::Vector(VectorInternal::Dense(dense))
        }
        rest::VectorInput::SparseVector(sparse) => {
            VectorInputInternal::Vector(VectorInternal::Sparse(sparse))
        }
        rest::VectorInput::MultiDenseVector(multi_dense) => VectorInputInternal::Vector(
            // TODO(universal-query): Validate at API level
            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(multi_dense)),
        ),
        rest::VectorInput::Document(_) => {
            // If this is reached, it means validation failed
            unimplemented!("Document inference is not implemented")
        }
        rest::VectorInput::Image(_) => {
            // If this is reached, it means validation failed
            unimplemented!("Image inference is not implemented")
        }
        rest::VectorInput::Object(_) => {
            // If this is reached, it means validation failed
            unimplemented!("Object inference is not implemented")
        }
    }
}

/// Circular dependencies prevents us from implementing `From` directly
fn context_pair_from_rest(value: rest::ContextPair) -> ContextPair<VectorInputInternal> {
    let rest::ContextPair { positive, negative } = value;

    ContextPair {
        positive: convert_vector_input(positive),
        negative: convert_vector_input(negative),
    }
}
