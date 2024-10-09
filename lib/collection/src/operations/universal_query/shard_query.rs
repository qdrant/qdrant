use api::grpc::qdrant as grpc;
use common::types::ScoreType;
use itertools::Itertools;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{
    NamedQuery, NamedVectorStruct, VectorInternal, DEFAULT_VECTOR_NAME,
};
use segment::types::{Filter, Order, ScoredPoint, SearchParams, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{ContextQuery, DiscoveryQuery, RecoQuery};
use tonic::Status;

use crate::config::CollectionParams;
use crate::operations::query_enum::QueryEnum;
use crate::operations::types::CollectionResult;

/// Internal response type for a universal query request.
///
/// Capable of returning multiple intermediate results if needed, like the case of RRF (Reciprocal Rank Fusion)
pub type ShardQueryResponse = Vec<Vec<ScoredPoint>>;

/// Internal representation of a universal query request.
///
/// Direct translation of the user-facing request, but with all point ids substituted with their corresponding vectors.
#[derive(Clone, Debug)]
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

impl ShardQueryRequest {
    pub fn prefetches_depth(&self) -> usize {
        self.prefetches
            .iter()
            .map(ShardPrefetch::depth)
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FusionInternal {
    /// Reciprocal Rank Fusion
    Rrf,
    /// Distribution-based score fusion
    Dbsf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleInternal {
    Random,
}

/// Same as `Query`, but with the resolved vector references.
#[derive(Debug, Clone, PartialEq)]
pub enum ScoringQuery {
    /// Score points against some vector(s)
    Vector(QueryEnum),

    /// Reciprocal rank fusion
    Fusion(FusionInternal),

    /// Order by a payload field
    OrderBy(OrderBy),

    /// Sample points
    Sample(SampleInternal),
}

impl ScoringQuery {
    pub fn needs_intermediate_results(&self) -> bool {
        match self {
            Self::Fusion(fusion) => match fusion {
                FusionInternal::Rrf => true,
                FusionInternal::Dbsf => true,
            },
            Self::Vector(_) | Self::OrderBy(_) | Self::Sample(_) => false,
        }
    }

    /// Get the vector name if it is scored against a vector
    pub fn get_vector_name(&self) -> Option<&str> {
        match self {
            Self::Vector(query) => Some(query.get_vector_name()),
            _ => None,
        }
    }

    /// Returns the expected order of results, depending on the type of query
    pub fn order(
        opt_self: Option<&Self>,
        collection_params: &CollectionParams,
    ) -> CollectionResult<Option<Order>> {
        let order = match opt_self {
            Some(scoring_query) => match scoring_query {
                ScoringQuery::Vector(query_enum) => {
                    if query_enum.is_distance_scored() {
                        Some(
                            collection_params
                                .get_distance(query_enum.get_vector_name())?
                                .distance_order(),
                        )
                    } else {
                        Some(Order::LargeBetter)
                    }
                }
                ScoringQuery::Fusion(fusion) => match fusion {
                    FusionInternal::Rrf | FusionInternal::Dbsf => Some(Order::LargeBetter),
                },
                ScoringQuery::OrderBy(order_by) => Some(Order::from(order_by.direction())),
                // Random sample does not require ordering
                ScoringQuery::Sample(SampleInternal::Random) => None,
            },
            None => {
                // Order by ID
                Some(Order::SmallBetter)
            }
        };
        Ok(order)
    }
}

#[derive(Clone, Debug)]
pub struct ShardPrefetch {
    pub prefetches: Vec<ShardPrefetch>,
    pub query: Option<ScoringQuery>,
    pub limit: usize,
    pub params: Option<SearchParams>,
    pub filter: Option<Filter>,
    pub score_threshold: Option<ScoreType>,
}

impl ShardPrefetch {
    pub fn depth(&self) -> usize {
        let mut depth = 1;
        for prefetch in &self.prefetches {
            depth = depth.max(prefetch.depth() + 1);
        }
        depth
    }
}

impl ShardQueryRequest {
    pub fn filter_refs(&self) -> Vec<Option<&Filter>> {
        let mut filters = vec![];
        filters.push(self.filter.as_ref());

        for prefetch in &self.prefetches {
            filters.extend(prefetch.filter_refs())
        }

        filters
    }
}

impl ShardPrefetch {
    fn filter_refs(&self) -> Vec<Option<&Filter>> {
        let mut filters = vec![];

        filters.push(self.filter.as_ref());

        for prefetch in &self.prefetches {
            filters.extend(prefetch.filter_refs())
        }

        filters
    }
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
                let vector = VectorInternal::try_from(nearest)?;
                let name = match (using, &vector) {
                    (None, VectorInternal::Sparse(_)) => {
                        return Err(Status::invalid_argument("Sparse vector must have a name"))
                    }
                    (
                        Some(name),
                        VectorInternal::MultiDense(_)
                        | VectorInternal::Sparse(_)
                        | VectorInternal::Dense(_),
                    ) => name,
                    (None, VectorInternal::MultiDense(_) | VectorInternal::Dense(_)) => {
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

impl TryFrom<i32> for FusionInternal {
    type Error = tonic::Status;

    fn try_from(fusion: i32) -> Result<Self, Self::Error> {
        let fusion = api::grpc::qdrant::Fusion::try_from(fusion).map_err(|_| {
            tonic::Status::invalid_argument(format!("invalid fusion type value {fusion}",))
        })?;

        Ok(FusionInternal::from(fusion))
    }
}

impl TryFrom<i32> for SampleInternal {
    type Error = tonic::Status;

    fn try_from(sample: i32) -> Result<Self, Self::Error> {
        let sample = api::grpc::qdrant::Sample::try_from(sample).map_err(|_| {
            tonic::Status::invalid_argument(format!("invalid sample type value {sample}",))
        })?;

        Ok(SampleInternal::from(sample))
    }
}

impl From<api::grpc::qdrant::Fusion> for FusionInternal {
    fn from(fusion: api::grpc::qdrant::Fusion) -> Self {
        match fusion {
            api::grpc::qdrant::Fusion::Rrf => FusionInternal::Rrf,
            api::grpc::qdrant::Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

impl From<FusionInternal> for api::grpc::qdrant::Fusion {
    fn from(fusion: FusionInternal) -> Self {
        match fusion {
            FusionInternal::Rrf => api::grpc::qdrant::Fusion::Rrf,
            FusionInternal::Dbsf => api::grpc::qdrant::Fusion::Dbsf,
        }
    }
}

impl From<SampleInternal> for api::grpc::qdrant::Sample {
    fn from(value: SampleInternal) -> Self {
        match value {
            SampleInternal::Random => api::grpc::qdrant::Sample::Random,
        }
    }
}

impl From<api::grpc::qdrant::Sample> for SampleInternal {
    fn from(value: api::grpc::qdrant::Sample) -> Self {
        match value {
            api::grpc::qdrant::Sample::Random => SampleInternal::Random,
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
                ScoringQuery::Fusion(FusionInternal::try_from(fusion)?)
            }
            grpc::query_shard_points::query::Score::OrderBy(order_by) => {
                ScoringQuery::OrderBy(OrderBy::try_from(order_by)?)
            }
            grpc::query_shard_points::query::Score::Sample(sample) => {
                ScoringQuery::Sample(SampleInternal::try_from(sample)?)
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
            ScoringQuery::Sample(sample) => Self {
                score: Some(Score::Sample(api::grpc::qdrant::Sample::from(sample) as i32)),
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
