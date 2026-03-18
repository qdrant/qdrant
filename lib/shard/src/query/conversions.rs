use api::{grpc, rest};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use segment::common::reciprocal_rank_fusion::DEFAULT_RRF_K;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::{
    DEFAULT_VECTOR_NAME, NamedQuery, NamedVectorStruct, VectorInternal,
};
use segment::index::query_optimization::rescore_formula::parsed_formula::{
    DecayKind, ParsedFormula,
};
use segment::types::{Filter, SearchParams, VectorNameBuf, WithPayloadInterface, WithVector};
use segment::vector_storage::query::{
    ContextQuery, DiscoverQuery, FeedbackItem, NaiveFeedbackCoefficients, NaiveFeedbackQuery,
    RecoQuery,
};

use crate::query::formula::*;
use crate::query::query_enum::*;
use crate::query::{
    FusionInternal, MmrInternal, SampleInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest,
};

impl From<rest::schema::SearchRequestInternal> for ShardQueryRequest {
    fn from(value: rest::schema::SearchRequestInternal) -> Self {
        let rest::schema::SearchRequestInternal {
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

impl TryFrom<grpc::QueryShardPoints> for ShardQueryRequest {
    type Error = tonic::Status;

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

impl TryFrom<grpc::query_shard_points::Prefetch> for ShardPrefetch {
    type Error = tonic::Status;

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

impl From<rest::Fusion> for FusionInternal {
    fn from(value: rest::Fusion) -> Self {
        match value {
            rest::Fusion::Rrf => FusionInternal::Rrf {
                k: DEFAULT_RRF_K,
                weights: None,
            },
            rest::Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

impl From<rest::Rrf> for FusionInternal {
    fn from(value: rest::Rrf) -> Self {
        let rest::Rrf { k, weights } = value;
        FusionInternal::Rrf {
            k: k.unwrap_or(DEFAULT_RRF_K),
            weights: weights.map(|w| w.into_iter().map(OrderedFloat).collect()),
        }
    }
}

impl From<grpc::Fusion> for FusionInternal {
    fn from(fusion: grpc::Fusion) -> Self {
        match fusion {
            grpc::Fusion::Rrf => FusionInternal::Rrf {
                k: DEFAULT_RRF_K,
                weights: None,
            },
            grpc::Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

impl TryFrom<grpc::Rrf> for FusionInternal {
    type Error = tonic::Status;

    fn try_from(rrf: grpc::Rrf) -> Result<Self, Self::Error> {
        let grpc::Rrf { k, weights } = rrf;
        let weights = if weights.is_empty() {
            None
        } else {
            Some(weights.into_iter().map(OrderedFloat).collect())
        };
        Ok(FusionInternal::Rrf {
            k: k.map(|k| k as usize).unwrap_or(DEFAULT_RRF_K),
            weights,
        })
    }
}

impl TryFrom<i32> for FusionInternal {
    type Error = tonic::Status;

    fn try_from(fusion: i32) -> Result<Self, Self::Error> {
        let fusion = grpc::Fusion::try_from(fusion).map_err(|_| {
            tonic::Status::invalid_argument(format!("invalid fusion type value {fusion}",))
        })?;

        Ok(FusionInternal::from(fusion))
    }
}

impl From<FusionInternal> for grpc::Query {
    fn from(fusion: FusionInternal) -> Self {
        use grpc::query::Variant as QueryVariant;
        use grpc::{Fusion, Query, Rrf};

        match fusion {
            // Avoid breaking rolling upgrade by keeping case of k==2 and no weights as Fusion::Rrf
            FusionInternal::Rrf { k, weights: None } if k == DEFAULT_RRF_K => Query {
                variant: Some(QueryVariant::Fusion(i32::from(Fusion::Rrf))),
            },
            FusionInternal::Rrf { k, weights } => Query {
                variant: Some(QueryVariant::Rrf(Rrf {
                    k: Some(k as u32),
                    weights: weights
                        .map(|w| w.into_iter().map(|f| f.into_inner()).collect())
                        .unwrap_or_default(),
                })),
            },
            FusionInternal::Dbsf => Query {
                variant: Some(QueryVariant::Fusion(i32::from(Fusion::Dbsf))),
            },
        }
    }
}

impl From<FusionInternal> for grpc::query_shard_points::Query {
    fn from(fusion: FusionInternal) -> Self {
        use grpc::query_shard_points::Query;
        use grpc::query_shard_points::query::Score;
        use grpc::{Fusion, Rrf};

        match fusion {
            // Avoid breaking rolling upgrade by keeping case of k==2 and no weights as Fusion::Rrf
            FusionInternal::Rrf { k, weights: None } if k == DEFAULT_RRF_K => Query {
                score: Some(Score::Fusion(i32::from(Fusion::Rrf))),
            },
            FusionInternal::Rrf { k, weights } => Query {
                score: Some(Score::Rrf(Rrf {
                    k: Some(k as u32),
                    weights: weights
                        .map(|w| w.into_iter().map(|f| f.into_inner()).collect())
                        .unwrap_or_default(),
                })),
            },
            FusionInternal::Dbsf => Query {
                score: Some(Score::Fusion(i32::from(Fusion::Dbsf))),
            },
        }
    }
}

impl From<rest::Sample> for SampleInternal {
    fn from(value: rest::Sample) -> Self {
        match value {
            rest::Sample::Random => SampleInternal::Random,
        }
    }
}

impl From<grpc::Sample> for SampleInternal {
    fn from(value: grpc::Sample) -> Self {
        match value {
            grpc::Sample::Random => SampleInternal::Random,
        }
    }
}

impl TryFrom<i32> for SampleInternal {
    type Error = tonic::Status;

    fn try_from(sample: i32) -> Result<Self, Self::Error> {
        let sample = grpc::Sample::try_from(sample).map_err(|_| {
            tonic::Status::invalid_argument(format!("invalid sample type value {sample}",))
        })?;

        Ok(SampleInternal::from(sample))
    }
}

impl From<SampleInternal> for grpc::Sample {
    fn from(value: SampleInternal) -> Self {
        match value {
            SampleInternal::Random => grpc::Sample::Random,
        }
    }
}

impl ScoringQuery {
    fn try_from_grpc_query(
        query: grpc::query_shard_points::Query,
        using: Option<VectorNameBuf>,
    ) -> Result<Self, tonic::Status> {
        let score = query
            .score
            .ok_or_else(|| tonic::Status::invalid_argument("missing field: score"))?;
        let scoring_query = match score {
            grpc::query_shard_points::query::Score::Vector(query) => {
                ScoringQuery::Vector(QueryEnum::from_grpc_raw_query(query, using)?)
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
                    tonic::Status::invalid_argument(format!("failed to parse formula: {e}"))
                })?,
            ),
            grpc::query_shard_points::query::Score::Mmr(grpc::MmrInternal {
                vector,
                lambda,
                candidates_limit,
            }) => {
                let vector = vector
                    .ok_or_else(|| tonic::Status::invalid_argument("missing field: mmr.vector"))?;
                let vector = VectorInternal::try_from(vector)?;
                ScoringQuery::Mmr(MmrInternal {
                    vector,
                    using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
                    lambda: OrderedFloat(lambda),
                    candidates_limit: candidates_limit as usize,
                })
            }
        };

        Ok(scoring_query)
    }
}

impl From<ScoringQuery> for grpc::query_shard_points::Query {
    fn from(value: ScoringQuery) -> Self {
        use grpc::query_shard_points::query::Score;

        match value {
            ScoringQuery::Vector(query) => Self {
                score: Some(Score::Vector(grpc::RawQuery::from(query))),
            },
            ScoringQuery::Fusion(fusion) => Self::from(fusion),
            ScoringQuery::OrderBy(order_by) => Self {
                score: Some(Score::OrderBy(grpc::OrderBy::from(order_by))),
            },
            ScoringQuery::Formula(parsed_formula) => Self {
                score: Some(Score::Formula(grpc::Formula::from_parsed(parsed_formula))),
            },
            ScoringQuery::Sample(sample) => Self {
                score: Some(Score::Sample(grpc::Sample::from(sample) as i32)),
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

impl From<QueryEnum> for grpc::QueryEnum {
    fn from(value: QueryEnum) -> Self {
        match value {
            QueryEnum::Nearest(vector) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::NearestNeighbors(
                    grpc::Vector::from(vector.query),
                )),
            },
            QueryEnum::RecommendBestScore(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::RecommendBestScore(
                    named.query.into(),
                )),
            },
            QueryEnum::RecommendSumScores(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::RecommendSumScores(
                    named.query.into(),
                )),
            },
            QueryEnum::Discover(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::Discover(grpc::DiscoveryQuery {
                    target: Some(named.query.target.into()),
                    context: named
                        .query
                        .pairs
                        .into_iter()
                        .map(|pair| grpc::ContextPair {
                            positive: Some(pair.positive.into()),
                            negative: Some(pair.negative.into()),
                        })
                        .collect(),
                })),
            },
            QueryEnum::Context(named) => grpc::QueryEnum {
                query: Some(grpc::query_enum::Query::Context(grpc::ContextQuery {
                    context: named
                        .query
                        .pairs
                        .into_iter()
                        .map(|pair| grpc::ContextPair {
                            positive: Some(pair.positive.into()),
                            negative: Some(pair.negative.into()),
                        })
                        .collect(),
                })),
            },
            QueryEnum::FeedbackNaive(_named) => {
                // This conversion only happens for search/recommend/discover dedicated endpoints. Feedback does not have one.
                unimplemented!("there is no specialized feedback endpoint")
            }
        }
    }
}

impl QueryEnum {
    pub fn from_grpc_raw_query(
        raw_query: grpc::RawQuery,
        using: Option<VectorNameBuf>,
    ) -> Result<QueryEnum, tonic::Status> {
        use grpc::raw_query::Variant;

        let variant = raw_query
            .variant
            .ok_or_else(|| tonic::Status::invalid_argument("missing field: variant"))?;

        let query_enum = match variant {
            Variant::Nearest(nearest) => {
                let vector = VectorInternal::try_from(nearest)?;
                let name = match (using, &vector) {
                    (None, VectorInternal::Sparse(_)) => {
                        return Err(tonic::Status::invalid_argument(
                            "Sparse vector must have a name",
                        ));
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
            Variant::Discover(discover) => QueryEnum::Discover(NamedQuery {
                query: DiscoverQuery::try_from(discover)?,
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
                    .ok_or_else(|| {
                        tonic::Status::invalid_argument("feedback strategy is required")
                    })?;

                let target = VectorInternal::try_from(
                    target.ok_or_else(|| tonic::Status::invalid_argument("No target provided"))?,
                )?;
                let feedback = feedback
                    .into_iter()
                    .map(<FeedbackItem<VectorInternal>>::try_from)
                    .collect::<Result<Vec<_>, _>>()?;

                match strategy {
                    grpc::feedback_strategy::Variant::Naive(strategy) => {
                        let feedback_query = NaiveFeedbackQuery {
                            target,
                            feedback,
                            coefficients: NaiveFeedbackCoefficients::from(strategy),
                        };
                        let named = NamedQuery {
                            query: feedback_query,
                            using,
                        };
                        QueryEnum::FeedbackNaive(named)
                    }
                }
            }
        };

        Ok(query_enum)
    }
}

impl From<QueryEnum> for grpc::RawQuery {
    fn from(query: QueryEnum) -> Self {
        use grpc::raw_query::Variant;

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
            QueryEnum::Context(named) => {
                Variant::Context(grpc::raw_query::Context::from(named.query))
            }
            QueryEnum::FeedbackNaive(named) => {
                Variant::Feedback(grpc::raw_query::Feedback::from(named.query))
            }
        };

        grpc::RawQuery {
            variant: Some(variant),
        }
    }
}

impl From<rest::FormulaQuery> for FormulaInternal {
    fn from(value: rest::FormulaQuery) -> Self {
        let rest::FormulaQuery { formula, defaults } = value;

        FormulaInternal {
            formula: ExpressionInternal::from(formula),
            defaults,
        }
    }
}

impl TryFrom<grpc::Formula> for FormulaInternal {
    type Error = tonic::Status;

    fn try_from(formula: grpc::Formula) -> Result<Self, Self::Error> {
        let grpc::Formula {
            expression,
            defaults,
        } = formula;

        let expression = expression
            .ok_or_else(|| tonic::Status::invalid_argument("missing field: expression"))?;

        let expression = ExpressionInternal::try_from(expression)?;
        let defaults = defaults
            .into_iter()
            .map(|(key, value)| {
                let value = api::conversions::json::proto_to_json(value)?;
                Result::<_, tonic::Status>::Ok((key, value))
            })
            .try_collect()?;

        Ok(Self {
            formula: expression,
            defaults,
        })
    }
}

impl From<rest::Expression> for ExpressionInternal {
    fn from(value: rest::Expression) -> Self {
        match value {
            rest::Expression::Constant(c) => ExpressionInternal::Constant(c),
            rest::Expression::Variable(key) => ExpressionInternal::Variable(key),
            rest::Expression::Condition(condition) => ExpressionInternal::Condition(condition),
            rest::Expression::GeoDistance(rest::GeoDistance {
                geo_distance: rest::GeoDistanceParams { origin, to },
            }) => ExpressionInternal::GeoDistance { origin, to },
            rest::Expression::Datetime(rest::DatetimeExpression { datetime }) => {
                ExpressionInternal::Datetime(datetime)
            }
            rest::Expression::DatetimeKey(rest::DatetimeKeyExpression { datetime_key }) => {
                ExpressionInternal::DatetimeKey(datetime_key)
            }
            rest::Expression::Mult(rest::MultExpression { mult: exprs }) => {
                ExpressionInternal::Mult(exprs.into_iter().map(ExpressionInternal::from).collect())
            }
            rest::Expression::Sum(rest::SumExpression { sum: exprs }) => {
                ExpressionInternal::Sum(exprs.into_iter().map(ExpressionInternal::from).collect())
            }
            rest::Expression::Neg(rest::NegExpression { neg: expr }) => {
                ExpressionInternal::Neg(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Div(rest::DivExpression {
                div:
                    rest::DivParams {
                        left,
                        right,
                        by_zero_default,
                    },
            }) => {
                let left = Box::new((*left).into());
                let right = Box::new((*right).into());
                ExpressionInternal::Div {
                    left,
                    right,
                    by_zero_default,
                }
            }
            rest::Expression::Sqrt(sqrt_expression) => {
                ExpressionInternal::Sqrt(Box::new(ExpressionInternal::from(*sqrt_expression.sqrt)))
            }
            rest::Expression::Pow(rest::PowExpression { pow }) => ExpressionInternal::Pow {
                base: Box::new(ExpressionInternal::from(*pow.base)),
                exponent: Box::new(ExpressionInternal::from(*pow.exponent)),
            },
            rest::Expression::Exp(rest::ExpExpression { exp: expr }) => {
                ExpressionInternal::Exp(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Log10(rest::Log10Expression { log10: expr }) => {
                ExpressionInternal::Log10(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Ln(rest::LnExpression { ln: expr }) => {
                ExpressionInternal::Ln(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::Abs(rest::AbsExpression { abs: expr }) => {
                ExpressionInternal::Abs(Box::new(ExpressionInternal::from(*expr)))
            }
            rest::Expression::LinDecay(rest::LinDecayExpression {
                lin_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Lin,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
            rest::Expression::ExpDecay(rest::ExpDecayExpression {
                exp_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Exp,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
            rest::Expression::GaussDecay(rest::GaussDecayExpression {
                gauss_decay:
                    rest::DecayParamsExpression {
                        x,
                        target,
                        midpoint,
                        scale,
                    },
            }) => ExpressionInternal::Decay {
                kind: DecayKind::Gauss,
                x: Box::new(ExpressionInternal::from(*x)),
                target: target.map(|t| Box::new(ExpressionInternal::from(*t))),
                midpoint,
                scale,
            },
        }
    }
}

impl TryFrom<grpc::Expression> for ExpressionInternal {
    type Error = tonic::Status;

    fn try_from(expression: grpc::Expression) -> Result<Self, Self::Error> {
        use grpc::expression::Variant;

        let variant = expression
            .variant
            .ok_or_else(|| tonic::Status::invalid_argument("missing field: variant"))?;

        let expression = match variant {
            Variant::Constant(constant) => ExpressionInternal::Constant(constant),
            Variant::Variable(variable) => ExpressionInternal::Variable(variable),
            Variant::Condition(condition) => {
                let condition = grpc::conversions::grpc_condition_into_condition(condition)?
                    .ok_or_else(|| tonic::Status::invalid_argument("missing field: condition"))?;
                ExpressionInternal::Condition(Box::new(condition))
            }
            Variant::GeoDistance(grpc::GeoDistance { origin, to }) => {
                let origin = origin
                    .ok_or_else(|| tonic::Status::invalid_argument("missing field: origin"))?
                    .into();
                let to = to
                    .parse()
                    .map_err(|_| tonic::Status::invalid_argument("invalid payload key"))?;
                ExpressionInternal::GeoDistance { origin, to }
            }
            Variant::Datetime(dt_str) => ExpressionInternal::Datetime(dt_str),
            Variant::DatetimeKey(dt_key) => {
                let json_path = dt_key
                    .parse()
                    .map_err(|_| tonic::Status::invalid_argument("invalid payload key"))?;
                ExpressionInternal::DatetimeKey(json_path)
            }
            Variant::Mult(grpc::MultExpression { mult }) => {
                let mult = mult
                    .into_iter()
                    .map(ExpressionInternal::try_from)
                    .try_collect()?;
                ExpressionInternal::Mult(mult)
            }
            Variant::Sum(grpc::SumExpression { sum }) => {
                let sum = sum
                    .into_iter()
                    .map(ExpressionInternal::try_from)
                    .try_collect()?;
                ExpressionInternal::Sum(sum)
            }
            Variant::Div(div) => {
                let grpc::DivExpression {
                    left,
                    right,
                    by_zero_default,
                } = *div;

                let left =
                    *left.ok_or_else(|| tonic::Status::invalid_argument("missing field: left"))?;
                let right = *right
                    .ok_or_else(|| tonic::Status::invalid_argument("missing field: right"))?;

                ExpressionInternal::Div {
                    left: Box::new(left.try_into()?),
                    right: Box::new(right.try_into()?),
                    by_zero_default,
                }
            }
            Variant::Neg(expression) => {
                ExpressionInternal::Neg(Box::new((*expression).try_into()?))
            }
            Variant::Abs(expression) => {
                ExpressionInternal::Abs(Box::new((*expression).try_into()?))
            }
            Variant::Sqrt(expression) => {
                ExpressionInternal::Sqrt(Box::new((*expression).try_into()?))
            }
            Variant::Pow(pow_expression) => {
                let grpc::PowExpression { base, exponent } = *pow_expression;
                let raw_base =
                    *base.ok_or_else(|| tonic::Status::invalid_argument("missing field: base"))?;
                let raw_exponent = *exponent
                    .ok_or_else(|| tonic::Status::invalid_argument("missing field: exponent"))?;

                ExpressionInternal::Pow {
                    base: Box::new(raw_base.try_into()?),
                    exponent: Box::new(raw_exponent.try_into()?),
                }
            }
            Variant::Exp(expression) => {
                ExpressionInternal::Exp(Box::new((*expression).try_into()?))
            }
            Variant::Log10(expression) => {
                ExpressionInternal::Log10(Box::new((*expression).try_into()?))
            }
            Variant::Ln(expression) => ExpressionInternal::Ln(Box::new((*expression).try_into()?)),
            Variant::LinDecay(decay_params) => {
                try_from_decay_params(*decay_params, DecayKind::Lin)?
            }
            Variant::ExpDecay(decay_params) => {
                try_from_decay_params(*decay_params, DecayKind::Exp)?
            }
            Variant::GaussDecay(decay_params) => {
                try_from_decay_params(*decay_params, DecayKind::Gauss)?
            }
        };

        Ok(expression)
    }
}

fn try_from_decay_params(
    params: grpc::DecayParamsExpression,
    kind: DecayKind,
) -> Result<ExpressionInternal, tonic::Status> {
    let grpc::DecayParamsExpression {
        x,
        target,
        midpoint,
        scale,
    } = params;

    let x = *x.ok_or_else(|| tonic::Status::invalid_argument("missing field: x"))?;

    let target = target.map(|t| (*t).try_into()).transpose()?.map(Box::new);

    Ok(ExpressionInternal::Decay {
        kind,
        x: Box::new(x.try_into()?),
        target,
        midpoint,
        scale,
    })
}
