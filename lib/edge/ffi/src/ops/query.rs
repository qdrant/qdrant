//! [`EdgeShard::query`] and the query building blocks shared by the other
//! read operations: [`Query`], [`SearchParams`] (also used by
//! [`SearchRequest`](crate::ops::search::SearchRequest)) and
//! [`OrderBy`]/[`Direction`] (also used by
//! [`ScrollRequest`](crate::ops::scroll::ScrollRequest)).

use std::collections::HashMap;
use std::sync::Arc;

use segment::data_types::order_by::{
    Direction as SegmentDirection, OrderBy as SegmentOrderBy, StartFrom as SegmentStartFrom,
};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, NamedQuery, VectorInternal};
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::{
    Filter as SegmentFilter, SearchParams as SegmentSearchParams, WithPayloadInterface,
    WithVector as SegmentWithVector,
};
use segment::vector_storage::query::{
    ContextPair as SegmentContextPair, ContextQuery, DiscoverQuery,
    FeedbackItem as SegmentFeedbackItem, NaiveFeedbackCoefficients, NaiveFeedbackQuery, RecoQuery,
};
use shard::query::formula::FormulaInternal;
use shard::query::query_enum::QueryEnum;
use shard::query::*;

use crate::EdgeShard;
use crate::error::Result;
use crate::filter::Filter;
use crate::ops::formula::Expression;
use crate::types::{NamedVector, ScoredPoint, WithPayload, WithVector};

#[uniffi::export]
impl EdgeShard {
    /// Executes a high-level query, supporting fusion, prefetching,
    /// re-ranking, and score thresholds.
    ///
    /// This is the most general search entry point. For simple nearest-neighbor
    /// search use [`EdgeShard::search`] instead.
    ///
    /// # Errors
    ///
    /// Returns [`EdgeError::ShardClosed`](crate::error::EdgeError) if the
    /// shard is unloaded, or
    /// [`EdgeError::OperationError`](crate::error::EdgeError) if the request
    /// is invalid or a required payload index is missing.
    pub fn query(&self, request: QueryRequest) -> Result<Vec<ScoredPoint>> {
        self.with_shard(|shard| {
            let points = shard.query(request.try_into()?)?;
            Ok(points.into_iter().map(ScoredPoint::from).collect())
        })
    }
}

// ── SearchParams ────────────────────────────────────────────────────────────

/// Tuning parameters that affect how a single ANN search is executed.
///
/// These are tuning knobs; the defaults are reasonable for most workloads.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchParams {
    /// HNSW `ef` parameter — the size of the candidate set kept during
    /// graph traversal. Higher values increase recall and latency. `None`
    /// uses the collection default.
    #[uniffi(default = None)]
    pub hnsw_ef: Option<u64>,
    /// If `true`, skip the ANN index and perform an exact brute-force
    /// search. Useful for small shards or recall ground-truthing.
    #[uniffi(default = false)]
    pub exact: bool,
    /// If `true`, skip points whose filter matches reference unindexed
    /// payload fields. Rarely needed on mobile.
    #[uniffi(default = false)]
    pub indexed_only: bool,
}

impl From<SearchParams> for SegmentSearchParams {
    fn from(p: SearchParams) -> Self {
        let SearchParams {
            hnsw_ef,
            exact,
            indexed_only,
        } = p;
        SegmentSearchParams {
            hnsw_ef: hnsw_ef.map(crate::error::clamp_usize),
            exact,
            quantization: None,
            indexed_only,
            acorn: None,
            idf: None,
        }
    }
}

// ── Query (vector queries) ──────────────────────────────────────────────────

/// How [`Query::Recommend`] combines the example vectors into a score.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum RecommendStrategy {
    /// Score each point by its best similarity against any positive example,
    /// penalized by its best similarity against any negative example
    /// (the server default).
    BestScore,
    /// Score each point by the sum of its similarities to all examples
    /// (negatives contribute negatively).
    SumScores,
}

/// A positive/negative example pair used by [`Query::Discover`] and
/// [`Query::Context`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct ContextPair {
    /// Vector the results should be similar to.
    pub positive: NamedVector,
    /// Vector the results should be dissimilar to.
    pub negative: NamedVector,
}

impl TryFrom<ContextPair> for SegmentContextPair<VectorInternal> {
    type Error = crate::error::EdgeError;

    fn try_from(p: ContextPair) -> Result<Self, Self::Error> {
        let ContextPair { positive, negative } = p;
        Ok(SegmentContextPair {
            positive: positive.try_into()?,
            negative: negative.try_into()?,
        })
    }
}

/// One graded relevance judgement used by [`Query::Feedback`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct FeedbackItem {
    /// The judged vector (e.g. of a result the user interacted with).
    pub vector: NamedVector,
    /// Relevance score assigned to `vector`; higher means more relevant.
    pub score: f32,
}

/// Tuning coefficients for [`Query::Feedback`] scoring.
#[derive(Clone, Copy, Debug, uniffi::Record)]
pub struct FeedbackCoefficients {
    /// Weight of the target-similarity component.
    pub a: f32,
    /// Weight of the feedback-context component.
    pub b: f32,
    /// Margin above which a pair of feedback items forms a context pair.
    pub c: f32,
}

/// A primitive vector query.
///
/// Used directly by [`SearchRequest`](crate::ops::search::SearchRequest) and
/// as the leaf of more complex [`ScoringQuery`] expressions. Every variant
/// takes an optional `using` — the vector field to search; `None`/`null`
/// targets the default (unnamed) field.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Query {
    /// Nearest-neighbor search against a vector field. The vector kind
    /// (dense / sparse / multi-vector) must match the field's configuration.
    Nearest {
        /// The query vector. Dimensionality must match the target field.
        vector: NamedVector,
        /// Name of the vector field to search. Pass `None`/`null` to
        /// target the default (unnamed) field.
        using: Option<String>,
    },
    /// Recommendation by example: score points against positive and negative
    /// example vectors.
    Recommend {
        /// Vectors the results should be similar to. Must not be empty.
        positives: Vec<NamedVector>,
        /// Vectors the results should be dissimilar to.
        negatives: Vec<NamedVector>,
        /// Scoring strategy; defaults to `BestScore` when unset.
        strategy: Option<RecommendStrategy>,
        /// Vector field to search; `None`/`null` for the default field.
        using: Option<String>,
    },
    /// Discovery search: guided by a target vector, constrained by context
    /// pairs.
    Discover {
        /// The vector to search around.
        target: NamedVector,
        /// Positive/negative pairs steering the search.
        context: Vec<ContextPair>,
        /// Vector field to search; `None`/`null` for the default field.
        using: Option<String>,
    },
    /// Context search: rank by how well points fit the context pairs alone,
    /// without a target.
    Context {
        /// Positive/negative pairs defining the context.
        context: Vec<ContextPair>,
        /// Vector field to search; `None`/`null` for the default field.
        using: Option<String>,
    },
    /// Feedback search: re-rank around a target using graded relevance
    /// judgements of earlier results.
    Feedback {
        /// The vector to search around.
        target: NamedVector,
        /// Graded relevance judgements.
        feedback: Vec<FeedbackItem>,
        /// Scoring coefficients.
        coefficients: FeedbackCoefficients,
        /// Vector field to search; `None`/`null` for the default field.
        using: Option<String>,
    },
}

/// The engine requires the target vector name to be present; `None` from the
/// host means the default (unnamed) field.
fn using_or_default(using: Option<String>) -> String {
    using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string())
}

fn vectors_to_internal(
    vectors: Vec<NamedVector>,
) -> Result<Vec<VectorInternal>, crate::error::EdgeError> {
    vectors.into_iter().map(VectorInternal::try_from).collect()
}

impl TryFrom<Query> for QueryEnum {
    type Error = crate::error::EdgeError;

    fn try_from(q: Query) -> Result<Self, Self::Error> {
        Ok(match q {
            Query::Nearest { vector, using } => QueryEnum::Nearest(NamedQuery {
                query: vector.try_into()?,
                using: Some(using_or_default(using)),
            }),
            Query::Recommend {
                positives,
                negatives,
                strategy,
                using,
            } => {
                if positives.is_empty() {
                    return Err(crate::error::EdgeError::invalid_argument(
                        "recommend query requires at least one positive example",
                    ));
                }
                let named = NamedQuery {
                    query: RecoQuery {
                        positives: vectors_to_internal(positives)?,
                        negatives: vectors_to_internal(negatives)?,
                    },
                    using: Some(using_or_default(using)),
                };
                match strategy.unwrap_or(RecommendStrategy::BestScore) {
                    RecommendStrategy::BestScore => QueryEnum::RecommendBestScore(named),
                    RecommendStrategy::SumScores => QueryEnum::RecommendSumScores(named),
                }
            }
            Query::Discover {
                target,
                context,
                using,
            } => QueryEnum::Discover(NamedQuery {
                query: DiscoverQuery::new(
                    target.try_into()?,
                    context
                        .into_iter()
                        .map(SegmentContextPair::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                using: Some(using_or_default(using)),
            }),
            Query::Context { context, using } => QueryEnum::Context(NamedQuery {
                query: ContextQuery::new(
                    context
                        .into_iter()
                        .map(SegmentContextPair::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                using: Some(using_or_default(using)),
            }),
            Query::Feedback {
                target,
                feedback,
                coefficients,
                using,
            } => {
                let FeedbackCoefficients { a, b, c } = coefficients;
                for (name, v) in [("a", a), ("b", b), ("c", c)] {
                    if !v.is_finite() {
                        return Err(crate::error::EdgeError::invalid_argument(format!(
                            "feedback coefficient {name} must be finite"
                        )));
                    }
                }
                let feedback = feedback
                    .into_iter()
                    .map(|FeedbackItem { vector, score }| {
                        if !score.is_finite() {
                            return Err(crate::error::EdgeError::invalid_argument(
                                "feedback score must be finite",
                            ));
                        }
                        Ok(SegmentFeedbackItem {
                            vector: vector.try_into()?,
                            score: ordered_float::OrderedFloat(score),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                QueryEnum::FeedbackNaive(NamedQuery {
                    query: NaiveFeedbackQuery {
                        target: target.try_into()?,
                        feedback,
                        coefficients: NaiveFeedbackCoefficients {
                            a: ordered_float::OrderedFloat(a),
                            b: ordered_float::OrderedFloat(b),
                            c: ordered_float::OrderedFloat(c),
                        },
                    },
                    using: Some(using_or_default(using)),
                })
            }
        })
    }
}

// ── ScoringQuery ────────────────────────────────────────────────────────────

/// The scoring strategy applied by [`QueryRequest`].
///
/// `Vector` is the most common case — score by vector similarity. `Fusion`
/// and `OrderBy` operate on pre-scored prefetch results.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum ScoringQuery {
    /// Score results by vector similarity (the typical search case).
    Vector { query: Query },
    /// Fuse scores from multiple prefetch branches. Requires a non-empty
    /// `QueryRequest.prefetches`.
    Fusion { fusion: Fusion },
    /// Rank results by a payload field instead of vector similarity.
    OrderBy { order_by: OrderBy },
    /// Re-score prefetch results with an arbitrary formula over prefetch
    /// scores and payload values. Requires a non-empty
    /// `QueryRequest.prefetches`.
    Formula {
        /// Root of the formula tree; build it via the [`Expression`]
        /// constructors.
        expression: Arc<Expression>,
        /// Fallback values for variables missing from a point's payload,
        /// keyed by variable name, each encoded as a JSON value string.
        defaults: HashMap<String, String>,
    },
    /// Maximal Marginal Relevance: nearest-neighbor search re-ranked to
    /// trade off relevance against diversity of the result set.
    Mmr {
        /// The query vector relevance is measured against.
        vector: NamedVector,
        /// Vector field to search; `None`/`null` for the default field.
        using: Option<String>,
        /// Diversity/relevance trade-off in `[0, 1]`: `0` = full diversity,
        /// `1` = full relevance.
        lambda: f32,
        /// Number of nearest-neighbor candidates preselected for re-ranking.
        candidates_limit: u64,
    },
    /// Sample results at random.
    Sample { sample: Sample },
}

impl TryFrom<ScoringQuery> for shard::query::ScoringQuery {
    type Error = crate::error::EdgeError;

    fn try_from(q: ScoringQuery) -> Result<Self, Self::Error> {
        match q {
            ScoringQuery::Vector { query } => Ok(shard::query::ScoringQuery::Vector(
                QueryEnum::try_from(query)?,
            )),
            ScoringQuery::Fusion { fusion } => Ok(shard::query::ScoringQuery::Fusion(
                FusionInternal::try_from(fusion)?,
            )),
            ScoringQuery::OrderBy { order_by } => Ok(shard::query::ScoringQuery::OrderBy(
                SegmentOrderBy::try_from(order_by)?,
            )),
            ScoringQuery::Formula {
                expression,
                defaults,
            } => {
                let defaults = defaults
                    .into_iter()
                    .map(|(k, v)| {
                        let value: serde_json::Value = serde_json::from_str(&v).map_err(|e| {
                            crate::error::EdgeError::invalid_argument(format!(
                                "formula default {k:?} is not valid JSON: {e}"
                            ))
                        })?;
                        Ok((k, value))
                    })
                    .collect::<Result<HashMap<_, _>, crate::error::EdgeError>>()?;
                let formula = FormulaInternal {
                    formula: expression.inner.clone(),
                    defaults,
                };
                let parsed = ParsedFormula::try_from(formula).map_err(|e| {
                    crate::error::EdgeError::invalid_argument(format!("invalid formula: {e}"))
                })?;
                Ok(shard::query::ScoringQuery::Formula(parsed))
            }
            ScoringQuery::Mmr {
                vector,
                using,
                lambda,
                candidates_limit,
            } => {
                if !lambda.is_finite() || !(0.0..=1.0).contains(&lambda) {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "MMR lambda ({lambda}) must be within [0, 1]"
                    )));
                }
                Ok(shard::query::ScoringQuery::Mmr(MmrInternal {
                    vector: vector.try_into()?,
                    using: using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string()),
                    lambda: ordered_float::OrderedFloat(lambda),
                    candidates_limit: crate::error::bounded_limit(
                        "MMR candidates_limit",
                        candidates_limit,
                    )?,
                }))
            }
            ScoringQuery::Sample { sample } => Ok(shard::query::ScoringQuery::Sample(
                SampleInternal::from(sample),
            )),
        }
    }
}

// ── Fusion ──────────────────────────────────────────────────────────────────

/// Fusion strategies for combining multiple prefetch branches into one
/// ranked result set.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Fusion {
    /// Reciprocal Rank Fusion. `k` is the smoothing constant (60 is a
    /// common default in the literature).
    Rrf {
        k: u64,
        /// Per-prefetch weights, aligned with `QueryRequest.prefetches`.
        /// Higher weight = more influence on the fused ranking; `None`/`null`
        /// weighs all branches equally.
        weights: Option<Vec<f32>>,
    },
    /// Distribution-based Score Fusion.
    Dbsf,
}

impl TryFrom<Fusion> for FusionInternal {
    type Error = crate::error::EdgeError;

    fn try_from(f: Fusion) -> Result<Self, Self::Error> {
        Ok(match f {
            Fusion::Rrf { k, weights } => {
                if let Some(weights) = &weights
                    && let Some(pos) = weights.iter().position(|w| !w.is_finite())
                {
                    return Err(crate::error::EdgeError::invalid_argument(format!(
                        "RRF weight at index {pos} must be finite"
                    )));
                }
                FusionInternal::Rrf {
                    k: crate::error::clamp_usize(k),
                    weights: weights
                        .map(|ws| ws.into_iter().map(ordered_float::OrderedFloat).collect()),
                }
            }
            Fusion::Dbsf => FusionInternal::Dbsf,
        })
    }
}

// ── OrderBy ─────────────────────────────────────────────────────────────────

/// The value an [`OrderBy`] iteration starts from.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum StartFrom {
    /// Start from this integer value of the ordering key.
    Integer { value: i64 },
    /// Start from this float value of the ordering key.
    Float { value: f64 },
    /// Start from this RFC 3339 datetime value of the ordering key.
    Datetime { value: String },
}

impl TryFrom<StartFrom> for SegmentStartFrom {
    type Error = crate::error::EdgeError;

    fn try_from(s: StartFrom) -> Result<Self, Self::Error> {
        Ok(match s {
            StartFrom::Integer { value } => SegmentStartFrom::Integer(value),
            StartFrom::Float { value } => SegmentStartFrom::Float(value),
            StartFrom::Datetime { value } => {
                SegmentStartFrom::Datetime(value.parse().map_err(|e| {
                    crate::error::EdgeError::invalid_argument(format!(
                        "invalid order-by start_from datetime ({value:?}): {e}"
                    ))
                })?)
            }
        })
    }
}

/// Orders results by a payload field rather than vector similarity.
///
/// Requires the payload `key` to have a numeric index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct OrderBy {
    /// Payload key to sort by. JSON-path syntax is supported.
    pub key: String,
    /// Sort direction. Defaults to ascending when unset.
    #[uniffi(default = None)]
    pub direction: Option<Direction>,
    /// Value of `key` to start the ordered iteration from (inclusive).
    #[uniffi(default = None)]
    pub start_from: Option<StartFrom>,
}

impl TryFrom<OrderBy> for SegmentOrderBy {
    type Error = crate::error::EdgeError;

    fn try_from(o: OrderBy) -> Result<Self, Self::Error> {
        let OrderBy {
            key,
            direction,
            start_from,
        } = o;
        Ok(SegmentOrderBy {
            key: crate::error::parse_json_path(&key)?,
            direction: direction.map(SegmentDirection::from),
            start_from: start_from.map(SegmentStartFrom::try_from).transpose()?,
        })
    }
}

// ── Direction ───────────────────────────────────────────────────────────────

/// Sort direction used by [`OrderBy`].
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Direction {
    /// Ascending (smallest first).
    Asc,
    /// Descending (largest first).
    Desc,
}

impl From<Direction> for SegmentDirection {
    fn from(d: Direction) -> Self {
        match d {
            Direction::Asc => SegmentDirection::Asc,
            Direction::Desc => SegmentDirection::Desc,
        }
    }
}

// ── Sample ──────────────────────────────────────────────────────────────────

/// Sampling strategies for [`ScoringQuery::Sample`].
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum Sample {
    /// Uniform random sampling.
    Random,
}

impl From<Sample> for SampleInternal {
    fn from(s: Sample) -> Self {
        match s {
            Sample::Random => SampleInternal::Random,
        }
    }
}

// ── Prefetch ────────────────────────────────────────────────────────────────

/// A single branch of a multi-stage query pipeline.
///
/// Prefetches produce candidate sets that are then combined (e.g. fused,
/// reranked) by the outer [`QueryRequest`]. Prefetches can themselves
/// contain nested prefetches for more complex pipelines.
#[derive(Clone, Debug, uniffi::Record)]
pub struct Prefetch {
    /// Maximum number of candidates this branch contributes.
    pub limit: u64,
    /// Scoring strategy for this branch. When `None`/`null`, the branch is a
    /// pass-through over its nested `prefetches`; with no prefetches either, it
    /// degrades to a scroll-by-id of `limit` points (engine behavior). A
    /// `Fusion` query with no nested `prefetches` is rejected by the engine
    /// ("cannot apply Fusion without prefetches").
    #[uniffi(default = None)]
    pub query: Option<ScoringQuery>,
    /// Nested prefetch branches (for recursive fusion / reranking).
    #[uniffi(default)]
    pub prefetches: Vec<Prefetch>,
    /// Optional filter applied to this branch only.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// Minimum score threshold; candidates scoring below are dropped.
    #[uniffi(default = None)]
    pub score_threshold: Option<f32>,
    /// Branch-specific search parameters.
    #[uniffi(default = None)]
    pub params: Option<SearchParams>,
}

impl TryFrom<Prefetch> for edge::Prefetch {
    type Error = crate::error::EdgeError;

    fn try_from(p: Prefetch) -> Result<Self, Self::Error> {
        prefetch_to_edge(p, 0)
    }
}

/// Convert a `Prefetch` at nesting `depth`, rejecting trees deeper than
/// [`MAX_QUERY_NESTING_DEPTH`](crate::error::MAX_QUERY_NESTING_DEPTH) before the
/// recursion over nested `prefetches` can overflow the stack.
fn prefetch_to_edge(p: Prefetch, depth: u32) -> Result<edge::Prefetch, crate::error::EdgeError> {
    crate::error::check_nesting_depth("prefetch", depth)?;
    let Prefetch {
        limit,
        query,
        prefetches,
        filter,
        score_threshold,
        params,
    } = p;
    Ok(edge::Prefetch {
        prefetches: prefetches
            .into_iter()
            .map(|pp| prefetch_to_edge(pp, depth + 1))
            .collect::<Result<Vec<_>, _>>()?,
        limit: crate::error::bounded_limit("prefetch limit", limit)?,
        query: query
            .map(shard::query::ScoringQuery::try_from)
            .transpose()?,
        params: params.map(SegmentSearchParams::from),
        filter: filter.map(SegmentFilter::try_from).transpose()?,
        score_threshold,
    })
}

// ── QueryRequest ────────────────────────────────────────────────────────────

/// The general-purpose query request supporting prefetching, fusion, and
/// re-ranking.
///
/// For a plain nearest-neighbor search use
/// [`SearchRequest`](crate::ops::search::SearchRequest) instead — it has a
/// smaller surface area.
#[derive(Clone, Debug, uniffi::Record)]
pub struct QueryRequest {
    /// Maximum number of results to return.
    pub limit: u64,
    /// Number of results to skip (for pagination).
    #[uniffi(default = None)]
    pub offset: Option<u64>,
    /// Scoring strategy. When `None`/`null` with `prefetches`, the request is a
    /// pass-through over the prefetch stage; with no `prefetches` either, it
    /// degrades to a scroll-by-id of `limit` points (engine behavior, matching
    /// the REST contract). A `Fusion` query with no `prefetches` is rejected by
    /// the engine ("cannot apply Fusion without prefetches").
    #[uniffi(default = None)]
    pub query: Option<ScoringQuery>,
    /// Optional prefetch branches used for multi-stage retrieval / fusion.
    #[uniffi(default)]
    pub prefetches: Vec<Prefetch>,
    /// Include vectors in the response.
    #[uniffi(default = None)]
    pub with_vector: Option<WithVector>,
    /// Include payload in the response.
    #[uniffi(default = None)]
    pub with_payload: Option<WithPayload>,
    /// Optional filter applied to all candidates.
    #[uniffi(default = None)]
    pub filter: Option<Filter>,
    /// Minimum score threshold; candidates scoring below are dropped.
    #[uniffi(default = None)]
    pub score_threshold: Option<f32>,
    /// Search tuning parameters.
    #[uniffi(default = None)]
    pub params: Option<SearchParams>,
}

impl TryFrom<QueryRequest> for edge::QueryRequest {
    type Error = crate::error::EdgeError;

    fn try_from(r: QueryRequest) -> Result<Self, Self::Error> {
        let QueryRequest {
            limit,
            offset,
            query,
            prefetches,
            with_vector,
            with_payload,
            filter,
            score_threshold,
            params,
        } = r;
        Ok(edge::QueryRequest {
            prefetches: prefetches
                .into_iter()
                .map(edge::Prefetch::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            limit: crate::error::bounded_limit("limit", limit)?,
            offset: crate::error::bounded_limit("offset", offset.unwrap_or(0))?,
            with_vector: with_vector.map(SegmentWithVector::from).unwrap_or_default(),
            with_payload: with_payload
                .map(WithPayloadInterface::try_from)
                .transpose()?
                .unwrap_or_default(),
            query: query
                .map(shard::query::ScoringQuery::try_from)
                .transpose()?,
            filter: filter.map(SegmentFilter::try_from).transpose()?,
            score_threshold,
            params: params.map(SegmentSearchParams::from),
        })
    }
}

// ── Coverage map ────────────────────────────────────────────────────────────

/// Compile-time map of the engine's scoring-query tree onto the FFI
/// [`ScoringQuery`] / [`Query`] surface above.
///
/// Same contract as the update-operation map in [`crate::update`]: an
/// exhaustive match with no wildcard arms, so a variant added to the engine's
/// [`shard::query::ScoringQuery`], [`QueryEnum`], [`FusionInternal`], or
/// [`SampleInternal`] stops this function from compiling, forcing an explicit
/// decision about the FFI surface.
///
/// Never called; it exists only for the exhaustiveness check.
#[allow(dead_code)]
fn assert_every_scoring_query_is_mapped(q: shard::query::ScoringQuery) {
    match q {
        shard::query::ScoringQuery::Vector(q) => match q {
            // [`Query::Nearest`]
            QueryEnum::Nearest(_) => {}
            // [`Query::Recommend`] with `strategy` unset or `BestScore`
            QueryEnum::RecommendBestScore(_) => {}
            // [`Query::Recommend`] with `strategy: SumScores`
            QueryEnum::RecommendSumScores(_) => {}
            // [`Query::Discover`]
            QueryEnum::Discover(_) => {}
            // [`Query::Context`]
            QueryEnum::Context(_) => {}
            // [`Query::Feedback`]
            QueryEnum::FeedbackNaive(_) => {}
        },
        shard::query::ScoringQuery::Fusion(f) => match f {
            // [`Fusion::Rrf`], including `weights`
            FusionInternal::Rrf { .. } => {}
            // [`Fusion::Dbsf`]
            FusionInternal::Dbsf => {}
        },
        // [`ScoringQuery::OrderBy`]
        shard::query::ScoringQuery::OrderBy(o) => {
            if let Some(start_from) = o.start_from {
                match start_from {
                    // [`StartFrom::Integer`]
                    SegmentStartFrom::Integer(_) => {}
                    // [`StartFrom::Float`]
                    SegmentStartFrom::Float(_) => {}
                    // [`StartFrom::Datetime`]
                    SegmentStartFrom::Datetime(_) => {}
                }
            }
        }
        // [`ScoringQuery::Formula`], built from [`Expression`] — the
        // expression tree itself is covered by construction: `Expression`
        // constructors are the only way to produce its `inner`.
        shard::query::ScoringQuery::Formula(_) => {}
        // [`ScoringQuery::Mmr`]
        shard::query::ScoringQuery::Mmr(_) => {}
        shard::query::ScoringQuery::Sample(s) => match s {
            // [`Sample::Random`]
            SampleInternal::Random => {}
        },
    }
}
