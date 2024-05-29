use api::grpc::qdrant as grpc;
use common::types::ScoreType;
use itertools::Itertools;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{NamedQuery, NamedVectorStruct, Vector, DEFAULT_VECTOR_NAME};
use segment::types::{Filter, ScoredPoint, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use tonic::Status;

use crate::operations::query_enum::QueryEnum;

/// Internal response type for a universal query request.
///
/// Capable of returning multiple intermediate results if needed, like the case of RRF (Reciprocal Rank Fusion)
pub type ShardQueryResponse = Vec<Vec<ScoredPoint>>;

/// Internal representation of a universal query request.
///
/// Direct translation of the user-facing request, but with all point ids substituted with their corresponding vectors.
#[derive(Clone)]
pub struct ShardQueryRequest {
    pub prefetches: Vec<ShardPrefetch>,
    pub query: Option<ScoringQuery>,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
    pub limit: usize,
    pub offset: usize,
    /// Search params for when there is no prefetch
    pub params: Option<SearchParams>,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Fusion {
    Rrf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScoringQuery {
    /// Score points against some vector(s)
    Vector(QueryEnum),

    /// Reciprocal rank fusion
    Fusion(Fusion),

    /// Order by a payload field
    OrderBy(OrderBy),
}

impl ScoringQuery {
    pub fn needs_intermediate_results(&self) -> bool {
        match self {
            ScoringQuery::Fusion(fusion) => match fusion {
                Fusion::Rrf => true,
            },
            ScoringQuery::Vector(_) | ScoringQuery::OrderBy(_) => false,
        }
    }

    /// Get the vector name if it is scored against a vector
    pub fn get_vector_name(&self) -> Option<&str> {
        match self {
            ScoringQuery::Vector(query) => Some(query.get_vector_name()),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct ShardPrefetch {
    pub prefetches: Vec<ShardPrefetch>,
    pub query: Option<ScoringQuery>,
    pub limit: usize,
    pub params: Option<SearchParams>,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
}

impl TryFrom<grpc::QueryShardPoints> for ShardQueryRequest {
    type Error = Status;

    fn try_from(value: grpc::QueryShardPoints) -> Result<Self, Self::Error> {
        let grpc::QueryShardPoints {
            prefetch,
            query,
            using,
            filter,
            limit,
            params,
            score_threshold,
            offset,
            with_payload,
            with_vectors,
        } = value;

        let request = Self {
            prefetches: prefetch
                .into_iter()
                .map(ShardPrefetch::try_from)
                .try_collect()?,
            query: query
                .map(|query| ScoringQuery::try_from_grpc_query(query, using))
                .transpose()?,
            filter: filter.map(Filter::try_from).transpose()?,
            score_threshold,
            limit: limit as usize,
            offset: offset as usize,
            params: params.map(SearchParams::from),
            with_vector: with_vectors
                .map(WithVector::from)
                .unwrap_or(WithVector::Bool(false)),
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?
                .unwrap_or(WithPayloadInterface::Bool(true)),
        };

        Ok(request)
    }
}

impl TryFrom<grpc::query_shard_points::Prefetch> for ShardPrefetch {
    type Error = Status;

    fn try_from(value: grpc::query_shard_points::Prefetch) -> Result<Self, Self::Error> {
        let grpc::query_shard_points::Prefetch {
            prefetch,
            query,
            limit,
            params,
            filter,
            score_threshold,
            using,
        } = value;

        let shard_prefetch = Self {
            prefetches: prefetch
                .into_iter()
                .map(ShardPrefetch::try_from)
                .try_collect()?,
            query: query
                .map(|query| ScoringQuery::try_from_grpc_query(query, using))
                .transpose()?,
            limit: limit as usize,
            params: params.map(SearchParams::from),
            filter: filter.map(Filter::try_from).transpose()?,
            score_threshold,
        };

        Ok(shard_prefetch)
    }
}

impl QueryEnum {
    fn try_from_grpc_raw_query(
        raw_query: grpc::RawQuery,
        using: Option<String>,
    ) -> Result<Self, Status> {
        use grpc::raw_query::Variant;

        let variant = raw_query
            .variant
            .ok_or_else(|| Status::invalid_argument("missing field: variant"))?;

        let query_enum = match variant {
            Variant::Nearest(nearest) => {
                let vector = Vector::try_from(nearest)?;
                let name = match (using, &vector) {
                    (None, Vector::Sparse(_)) => {
                        return Err(Status::invalid_argument("Sparse vector must have a name"))
                    }
                    (Some(name), Vector::MultiDense(_) | Vector::Sparse(_) | Vector::Dense(_)) => {
                        name
                    }
                    (None, Vector::MultiDense(_) | Vector::Dense(_)) => {
                        DEFAULT_VECTOR_NAME.to_string()
                    }
                };
                let named_vector = NamedVectorStruct::new_from_vector(vector, name);
                QueryEnum::Nearest(named_vector)
            }
            Variant::RecommendBestScore(recommend) => QueryEnum::RecommendBestScore(
                NamedQuery::new(RecoQuery::try_from(recommend)?, using),
            ),
            Variant::Discover(discovery) => QueryEnum::Discover(NamedQuery {
                query: DiscoveryQuery::try_from(discovery)?,
                using,
            }),
            Variant::Context(context) => QueryEnum::Context(NamedQuery {
                query: ContextQuery::try_from(context)?,
                using,
            }),
        };

        Ok(query_enum)
    }
}

impl TryFrom<i32> for Fusion {
    type Error = tonic::Status;

    fn try_from(fusion: i32) -> Result<Self, Self::Error> {
        let fusion = api::grpc::qdrant::Fusion::from_i32(fusion).ok_or_else(|| {
            tonic::Status::invalid_argument(format!("invalid read fusion type value {fusion}",))
        })?;

        Ok(Fusion::from(fusion))
    }
}

impl From<api::grpc::qdrant::Fusion> for Fusion {
    fn from(fusion: api::grpc::qdrant::Fusion) -> Self {
        match fusion {
            api::grpc::qdrant::Fusion::Rrf => Fusion::Rrf,
        }
    }
}

impl From<Fusion> for api::grpc::qdrant::Fusion {
    fn from(fusion: Fusion) -> Self {
        match fusion {
            Fusion::Rrf => api::grpc::qdrant::Fusion::Rrf,
        }
    }
}

impl ScoringQuery {
    fn try_from_grpc_query(
        query: grpc::query_shard_points::Query,
        using: Option<String>,
    ) -> Result<Self, Status> {
        let score = query
            .score
            .ok_or_else(|| Status::invalid_argument("missing field: score"))?;
        let scoring_query = match score {
            grpc::query_shard_points::query::Score::Vector(query) => {
                ScoringQuery::Vector(QueryEnum::try_from_grpc_raw_query(query, using)?)
            }
            grpc::query_shard_points::query::Score::Fusion(fusion) => {
                ScoringQuery::Fusion(Fusion::try_from(fusion)?)
            }
            grpc::query_shard_points::query::Score::OrderBy(order_by) => {
                ScoringQuery::OrderBy(OrderBy::try_from(order_by)?)
            }
        };

        Ok(scoring_query)
    }
}

impl From<QueryEnum> for grpc::RawQuery {
    fn from(value: QueryEnum) -> Self {
        use api::grpc::qdrant::raw_query::Variant;

        let variant = match value {
            QueryEnum::Nearest(named) => Variant::Nearest(grpc::RawVector::from(named.to_vector())),
            QueryEnum::RecommendBestScore(named) => {
                Variant::RecommendBestScore(grpc::raw_query::Recommend::from(named.query))
            }
            QueryEnum::Discover(named) => {
                Variant::Discover(grpc::raw_query::Discovery::from(named.query))
            }
            QueryEnum::Context(named) => {
                Variant::Context(grpc::raw_query::Context::from(named.query))
            }
        };

        Self {
            variant: Some(variant),
        }
    }
}

impl From<ScoringQuery> for grpc::query_shard_points::Query {
    fn from(value: ScoringQuery) -> Self {
        use grpc::query_shard_points::query::Score;

        match value {
            ScoringQuery::Vector(query) => Self {
                score: Some(Score::Vector(grpc::RawQuery::from(query))),
            },
            ScoringQuery::Fusion(fusion) => Self {
                score: Some(Score::Fusion(api::grpc::qdrant::Fusion::from(fusion) as i32)),
            },
            ScoringQuery::OrderBy(order_by) => Self {
                score: Some(Score::OrderBy(grpc::OrderBy::from(order_by))),
            },
        }
    }
}

impl From<ShardPrefetch> for grpc::query_shard_points::Prefetch {
    fn from(value: ShardPrefetch) -> Self {
        let ShardPrefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = value;
        Self {
            prefetch: prefetches.into_iter().map(Self::from).collect(),
            using: query
                .as_ref()
                .and_then(|query| query.get_vector_name().map(ToOwned::to_owned)),
            query: query.map(From::from),
            filter: filter.map(grpc::Filter::from),
            params: params.map(grpc::SearchParams::from),
            score_threshold,
            limit: limit as u64,
        }
    }
}

impl From<ShardQueryRequest> for grpc::QueryShardPoints {
    fn from(value: ShardQueryRequest) -> Self {
        let ShardQueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = value;

        Self {
            prefetch: prefetches
                .into_iter()
                .map(grpc::query_shard_points::Prefetch::from)
                .collect(),
            using: query
                .as_ref()
                .and_then(|query| query.get_vector_name().map(ToOwned::to_owned)),
            query: query.map(From::from),
            filter: filter.map(grpc::Filter::from),
            params: params.map(grpc::SearchParams::from),
            score_threshold,
            limit: limit as u64,
            offset: offset as u64,
            with_payload: Some(grpc::WithPayloadSelector::from(with_payload)),
            with_vectors: Some(grpc::WithVectorsSelector::from(with_vector)),
        }
    }
}
