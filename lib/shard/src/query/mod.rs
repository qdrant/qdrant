pub mod formula;
pub mod planned_query;
pub mod query_enum;
pub mod scroll;

#[cfg(test)]
mod tests;

use api::grpc::qdrant as grpc;
use api::rest::{self, SearchRequestInternal};
use common::types::ScoreType;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use segment::common::reciprocal_rank_fusion::DEFAULT_RRF_K;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, NamedQuery, NamedVectorStruct, VectorInternal,
};
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::*;
use segment::vector_storage::query::{
    ContextQuery, DiscoveryQuery, FeedbackItem, FeedbackQueryInternal, RecoQuery,
    SimpleFeedbackStrategy,
};
use serde::Serialize;
use tonic::Status;

use self::formula::*;
use self::query_enum::*;
use crate::search::CoreSearchRequest;

/// Internal representation of a universal query request.
///
/// Direct translation of the user-facing request, but with all point ids substituted with their corresponding vectors.
///
/// For the case of formula queries, it collects conditions and variables too.
#[derive(Clone, Debug, Hash, Serialize)]
pub struct ShardQueryRequest {
    pub prefetches: Vec<ShardPrefetch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<ScoringQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_threshold: Option<OrderedFloat<ScoreType>>,
    pub limit: usize,
    pub offset: usize,
    /// Search params for when there is no prefetch
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub enum FusionInternal {
    /// Reciprocal Rank Fusion
    RrfK(usize),
    /// Distribution-based score fusion
    Dbsf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Hash)]
pub enum SampleInternal {
    Random,
}

/// Maximal Marginal Relevance configuration
#[derive(Debug, Clone, PartialEq, Hash, Serialize)]
pub struct MmrInternal {
    /// Query vector, used to get the relevance of each point.
    pub vector: VectorInternal,
    /// Vector name to use for similarity computation, defaults to empty string (default vector)
    pub using: VectorNameBuf,
    /// Lambda parameter controlling diversity vs relevance trade-off (0.0 = full diversity, 1.0 = full relevance)
    pub lambda: OrderedFloat<f32>,
    /// Maximum number of candidates to pre-select using nearest neighbors.
    pub candidates_limit: usize,
}

/// Same as `Query`, but with the resolved vector references.
#[derive(Debug, Clone, PartialEq, Hash, Serialize)]
pub enum ScoringQuery {
    /// Score points against some vector(s)
    Vector(QueryEnum),

    /// Reciprocal rank fusion
    Fusion(FusionInternal),

    /// Order by a payload field
    OrderBy(OrderBy),

    /// Score boosting via an arbitrary formula
    Formula(ParsedFormula),

    /// Sample points
    Sample(SampleInternal),

    /// Maximal Marginal Relevance
    ///
    /// This one behaves a little differently than the other scorings, since it is two parts.
    /// It will create one nearest neighbor search in segment space and then try to resolve MMR algorithm higher up.
    ///
    /// E.g. If it is the root query of a request:
    ///   1. Performs search all the way down to segments.
    ///   2. MMR gets calculated once results reach collection level.
    Mmr(MmrInternal),
}

impl ScoringQuery {
    /// Whether the query needs the prefetches results from all shards to compute the final score
    ///
    /// If false, there is a single list of scored points which contain the final score.
    pub fn needs_intermediate_results(&self) -> bool {
        match self {
            Self::Fusion(fusion) => match fusion {
                // We need the ranking information of each prefetch
                FusionInternal::RrfK(_) => true,
                // We need the score distribution information of each prefetch
                FusionInternal::Dbsf => true,
            },
            // MMR is a nearest neighbors search before computing diversity at collection level
            Self::Mmr(_) => false,
            Self::Vector(_) | Self::OrderBy(_) | Self::Formula(_) | Self::Sample(_) => false,
        }
    }

    /// Get the vector name if it is scored against a vector
    pub fn get_vector_name(&self) -> Option<&VectorName> {
        match self {
            Self::Vector(query) => Some(query.get_vector_name()),
            Self::Mmr(mmr) => Some(&mmr.using),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct ShardPrefetch {
    pub prefetches: Vec<ShardPrefetch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<ScoringQuery>,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<SearchParams>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_threshold: Option<OrderedFloat<ScoreType>>,
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
            score_threshold: score_threshold.map(OrderedFloat),
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
            score_threshold: score_threshold.map(OrderedFloat),
        };

        Ok(shard_prefetch)
    }
}

fn query_enum_from_grpc_raw_query(
    raw_query: grpc::RawQuery,
    using: Option<VectorNameBuf>,
) -> Result<QueryEnum, Status> {
    use grpc::raw_query::Variant;

    let variant = raw_query
        .variant
        .ok_or_else(|| Status::invalid_argument("missing field: variant"))?;

    let query_enum = match variant {
        Variant::Nearest(nearest) => {
            let vector = VectorInternal::try_from(nearest)?;
            let name = match (using, &vector) {
                (None, VectorInternal::Sparse(_)) => {
                    return Err(Status::invalid_argument("Sparse vector must have a name"));
                }
                (
                    Some(name),
                    VectorInternal::MultiDense(_)
                    | VectorInternal::Sparse(_)
                    | VectorInternal::Dense(_),
                ) => name,
                (None, VectorInternal::MultiDense(_) | VectorInternal::Dense(_)) => {
                    DEFAULT_VECTOR_NAME.to_owned()
                }
            };
            let named_vector = NamedQuery::new(vector, name);
            QueryEnum::Nearest(named_vector)
        }
        Variant::RecommendBestScore(recommend) => QueryEnum::RecommendBestScore(NamedQuery {
            query: RecoQuery::try_from(recommend)?,
            using,
        }),
        Variant::RecommendSumScores(recommend) => QueryEnum::RecommendSumScores(NamedQuery {
            query: RecoQuery::try_from(recommend)?,
            using,
        }),
        Variant::Discover(discovery) => QueryEnum::Discover(NamedQuery {
            query: DiscoveryQuery::try_from(discovery)?,
            using,
        }),
        Variant::Context(context) => QueryEnum::Context(NamedQuery {
            query: ContextQuery::try_from(context)?,
            using,
        }),
        Variant::Feedback(grpc::raw_query::Feedback {
            target,
            feedback,
            strategy,
        }) => {
            let strategy = strategy
                .and_then(|strategy| strategy.variant)
                .ok_or_else(|| tonic::Status::invalid_argument("feedback strategy is required"))?;

            let target = VectorInternal::try_from(
                target.ok_or_else(|| Status::invalid_argument("No target provided"))?,
            )?;
            let feedback = feedback
                .into_iter()
                .map(<FeedbackItem<VectorInternal>>::try_from)
                .collect::<Result<Vec<_>, _>>()?;

            match strategy {
                grpc::feedback_strategy::Variant::Simple(strategy) => {
                    let feedback_query = FeedbackQueryInternal {
                        target,
                        feedback,
                        strategy: SimpleFeedbackStrategy::from(strategy),
                    };
                    let named = NamedQuery {
                        query: feedback_query,
                        using,
                    };
                    QueryEnum::FeedbackSimple(named)
                }
            }
        }
    };

    Ok(query_enum)
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

impl TryFrom<grpc::Rrf> for FusionInternal {
    type Error = tonic::Status;

    fn try_from(rrf: grpc::Rrf) -> Result<Self, Self::Error> {
        let grpc::Rrf { k } = rrf;
        Ok(FusionInternal::RrfK(
            k.map(|k| k as usize).unwrap_or(DEFAULT_RRF_K),
        ))
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
            api::grpc::qdrant::Fusion::Rrf => FusionInternal::RrfK(DEFAULT_RRF_K),
            api::grpc::qdrant::Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

impl From<FusionInternal> for api::grpc::qdrant::Query {
    fn from(fusion: FusionInternal) -> Self {
        use api::grpc::qdrant::query::Variant as QueryVariant;
        use api::grpc::qdrant::{Fusion, Query, Rrf};

        match fusion {
            // Avoid breaking rolling upgrade by keeping case of k==2 as Fusion::Rrf
            FusionInternal::RrfK(k) if k == DEFAULT_RRF_K => Query {
                variant: Some(QueryVariant::Fusion(i32::from(Fusion::Rrf))),
            },
            FusionInternal::RrfK(k) => Query {
                variant: Some(QueryVariant::Rrf(Rrf { k: Some(k as u32) })),
            },
            FusionInternal::Dbsf => Query {
                variant: Some(QueryVariant::Fusion(i32::from(Fusion::Dbsf))),
            },
        }
    }
}

impl From<FusionInternal> for api::grpc::qdrant::query_shard_points::Query {
    fn from(fusion: FusionInternal) -> Self {
        use api::grpc::qdrant::query_shard_points::Query;
        use api::grpc::qdrant::query_shard_points::query::Score;
        use api::grpc::qdrant::{Fusion, Rrf};

        match fusion {
            // Avoid breaking rolling upgrade by keeping case of k==2 as Fusion::Rrf
            FusionInternal::RrfK(k) if k == DEFAULT_RRF_K => Query {
                score: Some(Score::Fusion(i32::from(Fusion::Rrf))),
            },
            FusionInternal::RrfK(k) => Query {
                score: Some(Score::Rrf(Rrf { k: Some(k as u32) })),
            },
            FusionInternal::Dbsf => Query {
                score: Some(Score::Fusion(i32::from(Fusion::Dbsf))),
            },
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
        using: Option<VectorNameBuf>,
    ) -> Result<Self, Status> {
        let score = query
            .score
            .ok_or_else(|| Status::invalid_argument("missing field: score"))?;
        let scoring_query = match score {
            grpc::query_shard_points::query::Score::Vector(query) => {
                ScoringQuery::Vector(query_enum_from_grpc_raw_query(query, using)?)
            }
            grpc::query_shard_points::query::Score::Fusion(fusion) => {
                ScoringQuery::Fusion(FusionInternal::try_from(fusion)?)
            }
            grpc::query_shard_points::query::Score::Rrf(rrf) => {
                ScoringQuery::Fusion(FusionInternal::try_from(rrf)?)
            }
            grpc::query_shard_points::query::Score::OrderBy(order_by) => {
                ScoringQuery::OrderBy(OrderBy::try_from(order_by)?)
            }
            grpc::query_shard_points::query::Score::Sample(sample) => {
                ScoringQuery::Sample(SampleInternal::try_from(sample)?)
            }
            grpc::query_shard_points::query::Score::Formula(formula) => ScoringQuery::Formula(
                ParsedFormula::try_from(FormulaInternal::try_from(formula)?).map_err(|e| {
                    Status::invalid_argument(format!("failed to parse formula: {e}"))
                })?,
            ),
            grpc::query_shard_points::query::Score::Mmr(grpc::MmrInternal {
                vector,
                lambda,
                candidates_limit,
            }) => {
                let vector =
                    vector.ok_or_else(|| Status::invalid_argument("missing field: mmr.vector"))?;
                let vector = VectorInternal::try_from(vector)?;
                ScoringQuery::Mmr(MmrInternal {
                    vector,
                    using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
                    lambda: OrderedFloat::from(lambda),
                    candidates_limit: candidates_limit as usize,
                })
            }
        };

        Ok(scoring_query)
    }
}

fn query_enum_into_grpc_raw_query(query: QueryEnum) -> grpc::RawQuery {
    use api::grpc::qdrant::raw_query::Variant;

    let variant = match query {
        QueryEnum::Nearest(named) => Variant::Nearest(grpc::RawVector::from(named.query)),
        QueryEnum::RecommendBestScore(named) => {
            Variant::RecommendBestScore(grpc::raw_query::Recommend::from(named.query))
        }
        QueryEnum::RecommendSumScores(named) => {
            Variant::RecommendSumScores(grpc::raw_query::Recommend::from(named.query))
        }
        QueryEnum::Discover(named) => {
            Variant::Discover(grpc::raw_query::Discovery::from(named.query))
        }
        QueryEnum::Context(named) => Variant::Context(grpc::raw_query::Context::from(named.query)),
        QueryEnum::FeedbackSimple(named) => {
            Variant::Feedback(grpc::raw_query::Feedback::from(named.query))
        }
    };

    grpc::RawQuery {
        variant: Some(variant),
    }
}

impl From<ScoringQuery> for grpc::query_shard_points::Query {
    fn from(value: ScoringQuery) -> Self {
        use grpc::query_shard_points::query::Score;

        match value {
            ScoringQuery::Vector(query) => Self {
                score: Some(Score::Vector(query_enum_into_grpc_raw_query(query))),
            },
            ScoringQuery::Fusion(fusion) => Self::from(fusion),
            ScoringQuery::OrderBy(order_by) => Self {
                score: Some(Score::OrderBy(grpc::OrderBy::from(order_by))),
            },
            ScoringQuery::Formula(parsed_formula) => Self {
                score: Some(Score::Formula(grpc::Formula::from_parsed(parsed_formula))),
            },
            ScoringQuery::Sample(sample) => Self {
                score: Some(Score::Sample(api::grpc::qdrant::Sample::from(sample) as i32)),
            },
            ScoringQuery::Mmr(MmrInternal {
                vector,
                using: _,
                lambda,
                candidates_limit,
            }) => Self {
                score: Some(Score::Mmr(grpc::MmrInternal {
                    vector: Some(grpc::RawVector::from(vector)),
                    lambda: lambda.into_inner(),
                    candidates_limit: candidates_limit as u32,
                })),
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
            score_threshold: score_threshold.map(OrderedFloat::into_inner),
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
            score_threshold: score_threshold.map(OrderedFloat::into_inner),
            limit: limit as u64,
            offset: offset as u64,
            with_payload: Some(grpc::WithPayloadSelector::from(with_payload)),
            with_vectors: Some(grpc::WithVectorsSelector::from(with_vector)),
        }
    }
}

impl From<SearchRequestInternal> for ShardQueryRequest {
    fn from(value: SearchRequestInternal) -> Self {
        let SearchRequestInternal {
            vector,
            filter,
            score_threshold,
            limit,
            offset,
            params,
            with_vector,
            with_payload,
        } = value;

        Self {
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::from(
                NamedVectorStruct::from(vector),
            )))),
            filter,
            score_threshold: score_threshold.map(OrderedFloat),
            limit,
            offset: offset.unwrap_or_default(),
            params,
            with_vector: with_vector.unwrap_or_default(),
            with_payload: with_payload.unwrap_or_default(),
        }
    }
}

impl From<CoreSearchRequest> for ShardQueryRequest {
    fn from(value: CoreSearchRequest) -> Self {
        let CoreSearchRequest {
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
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(query)),
            filter,
            score_threshold: score_threshold.map(OrderedFloat),
            limit,
            offset,
            params,
            with_vector: with_vector.unwrap_or_default(),
            with_payload: with_payload.unwrap_or_default(),
        }
    }
}

impl From<rest::Fusion> for FusionInternal {
    fn from(value: rest::Fusion) -> Self {
        match value {
            rest::Fusion::Rrf => FusionInternal::RrfK(DEFAULT_RRF_K),
            rest::Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

impl From<rest::Rrf> for FusionInternal {
    fn from(value: rest::Rrf) -> Self {
        let rest::Rrf { k } = value;
        FusionInternal::RrfK(k.unwrap_or(DEFAULT_RRF_K))
    }
}

impl From<rest::Sample> for SampleInternal {
    fn from(value: rest::Sample) -> Self {
        match value {
            rest::Sample::Random => SampleInternal::Random,
        }
    }
}
