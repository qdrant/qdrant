use api::rest::RecommendStrategy;
use common::types::ScoreType;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{MultiDenseVector, Vector, DEFAULT_VECTOR_NAME};
use segment::types::{Filter, PointIdType, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};

use super::shard_query::Fusion;

/// Internal representation of a query request, used to converge from REST and gRPC. This can have IDs referencing vectors.
pub struct CollectionQueryRequest {
    pub prefetches: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    pub offset: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

pub enum Query {
    /// Score points against some vector(s)
    Vector(VectorQuery),

    /// Reciprocal rank fusion
    Fusion(Fusion),

    /// Order by a payload field
    OrderBy(OrderBy),
}

pub enum VectorInput {
    Id(PointIdType),
    Vector(Vector),
}

pub enum VectorQuery {
    Nearest(VectorInput),
    RecommendAverageVector(RecoQuery<VectorInput>),
    RecommendBestScore(RecoQuery<VectorInput>),
    Discover(DiscoveryQuery<VectorInput>),
    Context(ContextQuery<VectorInput>),
}

pub struct CollectionPrefetch {
    pub prefetch: Vec<CollectionPrefetch>,
    pub query: Option<Query>,
    pub using: String,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
}

mod from_rest {
    use api::rest::schema as rest;
    use super::*;

    impl From<rest::QueryRequest> for CollectionQueryRequest {
        fn from(value: rest::QueryRequest) -> Self {
            let rest::QueryRequest {
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
            } = value;

            Self {
                prefetches: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                limit: limit.unwrap_or(10),
                offset: offset.unwrap_or(0),
                params,
                with_vector: with_vector.unwrap_or(WithVector::Bool(false)),
                with_payload: with_payload.unwrap_or(WithPayloadInterface::Bool(false)),
            }
        }
    }

    impl From<rest::Prefetch> for CollectionPrefetch {
        fn from(value: rest::Prefetch) -> Self {
            let rest::Prefetch {
                prefetch,
                query,
                using,
                filter,
                score_threshold,
                params,
                limit,
            } = value;

            Self {
                prefetch: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                limit: limit.unwrap_or(10),
                params,
            }
        }
    }

    impl From<rest::QueryInterface> for Query {
        fn from(value: rest::QueryInterface) -> Self {
            Query::from(rest::Query::from(value))
        }
    }

    impl From<rest::Query> for Query {
        fn from(value: rest::Query) -> Self {
            match value {
                rest::Query::Nearest(nearest) => {
                    Query::Vector(VectorQuery::Nearest(From::from(nearest)))
                }
                rest::Query::Recommend(recommend) => Query::Vector(From::from(recommend)),
                rest::Query::Discover(discover) => Query::Vector(From::from(discover)),
                rest::Query::Context(context) => Query::Vector(From::from(context)),
                rest::Query::OrderBy(order_by) => Query::OrderBy(OrderBy::from(order_by)),
                rest::Query::Fusion(fusion) => Query::Fusion(Fusion::from(fusion)),
            }
        }
    }

    impl From<rest::RecommendInput> for VectorQuery {
        fn from(value: rest::RecommendInput) -> Self {
            let rest::RecommendInput {
                positives,
                negatives,
                strategy,
            } = value;

            let positives = positives.into_iter().flatten().map(From::from).collect();
            let negatives = negatives.into_iter().flatten().map(From::from).collect();

            let reco_query = RecoQuery::new(positives, negatives);

            match strategy.unwrap_or_default() {
                RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
                RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
            }
        }
    }

    impl From<rest::DiscoverInput> for VectorQuery {
        fn from(value: rest::DiscoverInput) -> Self {
            let rest::DiscoverInput {
                target,
                context_pairs,
            } = value;

            let target = From::from(target);
            let context = context_pairs
                .into_iter()
                .flatten()
                .map(context_pair_from_rest)
                .collect();

            VectorQuery::Discover(DiscoveryQuery::new(target, context))
        }
    }

    impl From<rest::ContextInput> for VectorQuery {
        fn from(value: rest::ContextInput) -> Self {
            let rest::ContextInput { pairs } = value;

            let context = pairs
                .into_iter()
                .flatten()
                .map(context_pair_from_rest)
                .collect();

            VectorQuery::Context(ContextQuery::new(context))
        }
    }

    impl From<rest::VectorInput> for VectorInput {
        fn from(value: rest::VectorInput) -> Self {
            match value {
                rest::VectorInput::Id(id) => VectorInput::Id(id),
                rest::VectorInput::DenseVector(dense) => VectorInput::Vector(Vector::Dense(dense)),
                rest::VectorInput::SparseVector(sparse) => {
                    VectorInput::Vector(Vector::Sparse(sparse))
                }
                rest::VectorInput::MultiDenseVector(multi_dense) => VectorInput::Vector(
                    // TODO(universal-query): Validate at API level
                    Vector::MultiDense(MultiDenseVector::new_unchecked(multi_dense)),
                ),
            }
        }
    }

    /// Circular dependencies prevents us from implementing `From` directly
    fn context_pair_from_rest(value: rest::ContextPair) -> ContextPair<VectorInput> {
        let rest::ContextPair { positive, negative } = value;

        ContextPair {
            positive: VectorInput::from(positive),
            negative: VectorInput::from(negative),
        }
    }

    impl From<rest::Fusion> for Fusion {
        fn from(value: rest::Fusion) -> Self {
            match value {
                rest::Fusion::Rrf => Fusion::Rrf,
            }
        }
    }
}
