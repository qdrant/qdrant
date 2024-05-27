use api::rest::RecommendStrategy;
use common::types::ScoreType;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{MultiDenseVector, Vector, DEFAULT_VECTOR_NAME};
use segment::types::{Filter, PointIdType, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextPair, ContextQuery, DiscoveryQuery, RecoQuery};

use super::shard_query::Fusion;

/// Internal representation of a query request, used to converge from REST and gRPC. This can have IDs referencing vectors.
pub struct CollectionQueryRequest {
    pub prefetch: Vec<CollectionPrefetch>,
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

impl CollectionQueryRequest {
    const DEFAULT_LIMIT: usize = 10;

    const DEFAULT_OFFSET: usize = 0;

    const DEFAULT_WITH_VECTOR: WithVector = WithVector::Bool(false);

    const DEFAULT_WITH_PAYLOAD: WithPayloadInterface = WithPayloadInterface::Bool(false);
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

    impl From<rest::QueryRequestInternal> for CollectionQueryRequest {
        fn from(value: rest::QueryRequestInternal) -> Self {
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
            } = value;

            Self {
                prefetch: prefetch.into_iter().flatten().map(From::from).collect(),
                query: query.map(From::from),
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter,
                score_threshold,
                limit: limit.unwrap_or(Self::DEFAULT_LIMIT),
                offset: offset.unwrap_or(Self::DEFAULT_OFFSET),
                params,
                with_vector: with_vector.unwrap_or(Self::DEFAULT_WITH_VECTOR),
                with_payload: with_payload.unwrap_or(Self::DEFAULT_WITH_PAYLOAD),
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
                limit: limit.unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
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

mod from_grpc {
    use api::grpc::qdrant::{self as grpc};
    use api::rest::ShardKeySelector;
    use tonic::Status;

    use super::*;
    use crate::operations::consistency_params::ReadConsistency;
    use crate::operations::shard_selector_internal::ShardSelectorInternal;

    pub struct IntoCollectionQueryRequest {
        pub request: CollectionQueryRequest,
        pub collection_name: String,
        pub shard_key: ShardSelectorInternal,
        pub read_consistency: Option<ReadConsistency>,
    }

    impl TryFrom<grpc::QueryPoints> for IntoCollectionQueryRequest {
        type Error = Status;

        fn try_from(value: grpc::QueryPoints) -> Result<Self, Self::Error> {
            let grpc::QueryPoints {
                collection_name,
                prefetch,
                query,
                using,
                filter,
                search_params,
                score_threshold,
                limit,
                offset,
                with_payload,
                with_vectors,
                read_consistency,
                shard_key_selector,
            } = value;

            let request = CollectionQueryRequest {
                prefetch: prefetch
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                query: query.map(TryFrom::try_from).transpose()?,
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter: filter.map(TryFrom::try_from).transpose()?,
                score_threshold,
                limit: limit
                    .map(|l| l as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                offset: offset
                    .map(|o| o as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_OFFSET),
                params: search_params.map(From::from),
                with_vector: with_vectors
                    .map(From::from)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_VECTOR),
                with_payload: with_payload
                    .map(TryFrom::try_from)
                    .transpose()?
                    .unwrap_or(CollectionQueryRequest::DEFAULT_WITH_PAYLOAD),
            };

            let shard_key =
                ShardSelectorInternal::from(shard_key_selector.map(ShardKeySelector::from));

            let read_consistency = read_consistency.map(TryFrom::try_from).transpose()?;

            Ok(IntoCollectionQueryRequest {
                request,
                collection_name,
                shard_key,
                read_consistency,
            })
        }
    }

    impl TryFrom<grpc::PrefetchQuery> for CollectionPrefetch {
        type Error = Status;

        fn try_from(value: grpc::PrefetchQuery) -> Result<Self, Self::Error> {
            let grpc::PrefetchQuery {
                prefetch,
                query,
                using,
                filter,
                search_params,
                score_threshold,
                limit,
            } = value;

            let collection_query = Self {
                prefetch: prefetch
                    .into_iter()
                    .map(TryFrom::try_from)
                    .collect::<Result<_, _>>()?,
                query: query.map(TryFrom::try_from).transpose()?,
                using: using.unwrap_or(DEFAULT_VECTOR_NAME.to_string()),
                filter: filter.map(TryFrom::try_from).transpose()?,
                score_threshold,
                limit: limit
                    .map(|l| l as usize)
                    .unwrap_or(CollectionQueryRequest::DEFAULT_LIMIT),
                params: search_params.map(From::from),
            };

            Ok(collection_query)
        }
    }

    impl TryFrom<grpc::Query> for Query {
        type Error = Status;

        fn try_from(value: grpc::Query) -> Result<Self, Self::Error> {
            use api::grpc::qdrant::query::Variant;

            let variant = value
                .variant
                .ok_or_else(|| Status::invalid_argument("Query variant is missing"))?;

            let query = match variant {
                Variant::Nearest(nearest) => {
                    Query::Vector(VectorQuery::Nearest(TryFrom::try_from(nearest)?))
                }
                Variant::Recommend(recommend) => Query::Vector(TryFrom::try_from(recommend)?),
                Variant::Discover(discover) => Query::Vector(TryFrom::try_from(discover)?),
                Variant::Context(context) => Query::Vector(TryFrom::try_from(context)?),
                Variant::OrderBy(order_by) => Query::OrderBy(OrderBy::try_from(order_by)?),
                Variant::Fusion(fusion) => Query::Fusion(Fusion::try_from(fusion)?),
            };

            Ok(query)
        }
    }

    impl TryFrom<grpc::RecommendInput> for VectorQuery {
        type Error = Status;

        fn try_from(value: grpc::RecommendInput) -> Result<Self, Self::Error> {
            let grpc::RecommendInput {
                positives,
                negatives,
                strategy,
            } = value;

            let positives = positives
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            let negatives = negatives
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            let reco_query = RecoQuery::new(positives, negatives);

            let strategy = strategy
                .and_then(grpc::RecommendStrategy::from_i32)
                .map(RecommendStrategy::from)
                .unwrap_or_default();

            let query = match strategy {
                RecommendStrategy::AverageVector => VectorQuery::RecommendAverageVector(reco_query),
                RecommendStrategy::BestScore => VectorQuery::RecommendBestScore(reco_query),
            };

            Ok(query)
        }
    }

    impl TryFrom<grpc::DiscoverInput> for VectorQuery {
        type Error = Status;

        fn try_from(value: grpc::DiscoverInput) -> Result<Self, Self::Error> {
            let grpc::DiscoverInput {
                target,
                context_pairs,
            } = value;

            let target = VectorInput::try_from(
                target
                    .ok_or_else(|| Status::invalid_argument("DiscoverInput target is missing"))?,
            )?;

            let context = context_pairs
                .into_iter()
                .map(context_pair_from_grpc)
                .collect::<Result<_, _>>()?;

            Ok(VectorQuery::Discover(DiscoveryQuery::new(target, context)))
        }
    }

    impl TryFrom<grpc::ContextInput> for VectorQuery {
        type Error = Status;

        fn try_from(value: grpc::ContextInput) -> Result<Self, Self::Error> {
            let grpc::ContextInput { context_pairs } = value;

            Ok(VectorQuery::Context(ContextQuery {
                pairs: context_pairs
                    .into_iter()
                    .map(context_pair_from_grpc)
                    .collect::<Result<_, _>>()?,
            }))
        }
    }

    impl TryFrom<grpc::VectorInput> for VectorInput {
        type Error = Status;

        fn try_from(value: grpc::VectorInput) -> Result<Self, Self::Error> {
            use api::grpc::qdrant::vector_input::Variant;

            let variant = value
                .variant
                .ok_or_else(|| Status::invalid_argument("VectorInput variant is missing"))?;

            let vector_input = match variant {
                Variant::Id(id) => VectorInput::Id(TryFrom::try_from(id)?),
                Variant::Dense(dense) => VectorInput::Vector(Vector::Dense(From::from(dense))),
                Variant::Sparse(sparse) => VectorInput::Vector(Vector::Sparse(From::from(sparse))),
                Variant::MultiDense(multi_dense) => VectorInput::Vector(
                    // TODO(universal-query): Validate at API level
                    Vector::MultiDense(From::from(multi_dense)),
                ),
            };

            Ok(vector_input)
        }
    }

    /// Circular dependencies prevents us from implementing `TryFrom` directly
    fn context_pair_from_grpc(
        value: grpc::ContextPairInput,
    ) -> Result<ContextPair<VectorInput>, Status> {
        let grpc::ContextPairInput { positive, negative } = value;

        let positive =
            positive.ok_or_else(|| Status::invalid_argument("ContextPair positive is missing"))?;
        let negative =
            negative.ok_or_else(|| Status::invalid_argument("ContextPair negative is missing"))?;

        Ok(ContextPair {
            positive: VectorInput::try_from(positive)?,
            negative: VectorInput::try_from(negative)?,
        })
    }
}
