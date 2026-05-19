use ordered_float::OrderedFloat;
use segment::data_types::order_by::{
    Direction as SegmentDirection, OrderBy as SegmentOrderBy, OrderByInterface,
};
use segment::data_types::vectors::{DEFAULT_VECTOR_NAME, VectorInternal};
use segment::types::{
    Filter as SegmentFilter, PointIdType,
    SearchParams as SegmentSearchParams, WithPayloadInterface,
    WithVector as SegmentWithVector,
};
use shard::count::CountRequestInternal;
use shard::facet::FacetRequestInternal;
use shard::query::*;
use shard::query::query_enum::QueryEnum;
use shard::scroll::ScrollRequestInternal;
use shard::search::CoreSearchRequest;

use crate::filter::Filter;
use crate::types::{PointId, WithPayload, WithVector};

// ── SearchParams ────────────────────────────────────────────────────────────

/// Tuning parameters that affect how a single ANN search is executed.
///
/// These are tuning knobs; the defaults are reasonable for most workloads.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchParams {
    /// HNSW `ef` parameter — the size of the candidate set kept during
    /// graph traversal. Higher values increase recall and latency. `None`
    /// uses the collection default.
    pub hnsw_ef: Option<u64>,
    /// If `true`, skip the ANN index and perform an exact brute-force
    /// search. Useful for small shards or recall ground-truthing.
    pub exact: bool,
    /// If `true`, skip points whose filter matches reference unindexed
    /// payload fields. Rarely needed on mobile.
    pub indexed_only: bool,
}

impl From<SearchParams> for SegmentSearchParams {
    fn from(p: SearchParams) -> Self {
        SegmentSearchParams {
            hnsw_ef: p.hnsw_ef.map(|v| v as usize),
            exact: p.exact,
            quantization: None,
            indexed_only: p.indexed_only,
            acorn: None,
        }
    }
}

// ── Query (nearest vector query) ────────────────────────────────────────────

/// A primitive query expressed as a single dense vector.
///
/// Used directly by [`SearchRequest`] and as the leaf of more complex
/// [`ScoringQuery`] expressions.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum Query {
    /// Nearest-neighbor search against a dense vector field.
    Nearest {
        /// The query vector. Dimensionality must match the target field.
        vector: Vec<f32>,
        /// Name of the dense vector field to search. Pass `None`/`null` to
        /// target the default (unnamed) field.
        using: Option<String>,
    },
}

impl From<Query> for QueryEnum {
    fn from(q: Query) -> Self {
        match q {
            Query::Nearest { vector, using } => {
                let using = using.unwrap_or_else(|| DEFAULT_VECTOR_NAME.to_string());
                QueryEnum::Nearest(segment::data_types::vectors::NamedQuery {
                    query: VectorInternal::Dense(vector),
                    using: Some(using),
                })
            }
        }
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
    /// Sample results at random.
    Sample { sample: Sample },
}

impl From<ScoringQuery> for shard::query::ScoringQuery {
    fn from(q: ScoringQuery) -> Self {
        match q {
            ScoringQuery::Vector { query } => {
                shard::query::ScoringQuery::Vector(QueryEnum::from(query))
            }
            ScoringQuery::Fusion { fusion } => {
                shard::query::ScoringQuery::Fusion(FusionInternal::from(fusion))
            }
            ScoringQuery::OrderBy { order_by } => {
                shard::query::ScoringQuery::OrderBy(SegmentOrderBy::from(order_by))
            }
            ScoringQuery::Sample { sample } => {
                shard::query::ScoringQuery::Sample(SampleInternal::from(sample))
            }
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
    Rrf { k: u64 },
    /// Distribution-based Score Fusion.
    Dbsf,
}

impl From<Fusion> for FusionInternal {
    fn from(f: Fusion) -> Self {
        match f {
            Fusion::Rrf { k } => FusionInternal::Rrf {
                k: k as usize,
                weights: None,
            },
            Fusion::Dbsf => FusionInternal::Dbsf,
        }
    }
}

// ── OrderBy ─────────────────────────────────────────────────────────────────

/// Orders results by a payload field rather than vector similarity.
///
/// Requires the payload `key` to have a numeric index.
#[derive(Clone, Debug, uniffi::Record)]
pub struct OrderBy {
    /// Payload key to sort by. JSON-path syntax is supported.
    pub key: String,
    /// Sort direction. Defaults to ascending when unset.
    pub direction: Option<Direction>,
}

impl From<OrderBy> for SegmentOrderBy {
    fn from(o: OrderBy) -> Self {
        SegmentOrderBy {
            key: o.key.parse().expect("valid json path"),
            direction: o.direction.map(SegmentDirection::from),
            start_from: None,
        }
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
    /// Scoring strategy for this branch. `None`/`null` means "pass-through"
    /// if there are nested `prefetches`.
    pub query: Option<ScoringQuery>,
    /// Nested prefetch branches (for recursive fusion / reranking).
    pub prefetches: Vec<Prefetch>,
    /// Optional filter applied to this branch only.
    pub filter: Option<Filter>,
    /// Minimum score threshold; candidates scoring below are dropped.
    pub score_threshold: Option<f32>,
    /// Branch-specific search parameters.
    pub params: Option<SearchParams>,
}

impl From<Prefetch> for ShardPrefetch {
    fn from(p: Prefetch) -> Self {
        ShardPrefetch {
            prefetches: p.prefetches.into_iter().map(ShardPrefetch::from).collect(),
            limit: p.limit as usize,
            query: p.query.map(shard::query::ScoringQuery::from),
            params: p.params.map(SegmentSearchParams::from),
            filter: p.filter.map(SegmentFilter::from),
            score_threshold: p.score_threshold.map(OrderedFloat),
        }
    }
}

// ── QueryRequest ────────────────────────────────────────────────────────────

/// The general-purpose query request supporting prefetching, fusion, and
/// re-ranking.
///
/// For a plain nearest-neighbor search use [`SearchRequest`] instead — it
/// has a smaller surface area.
#[derive(Clone, Debug, uniffi::Record)]
pub struct QueryRequest {
    /// Maximum number of results to return.
    pub limit: u64,
    /// Number of results to skip (for pagination).
    pub offset: Option<u64>,
    /// Scoring strategy. If `None`/`null`, the request must have
    /// `prefetches` and relies on fusion implicit in the prefetch stage.
    pub query: Option<ScoringQuery>,
    /// Optional prefetch branches used for multi-stage retrieval / fusion.
    pub prefetches: Vec<Prefetch>,
    /// Include vectors in the response.
    pub with_vector: Option<WithVector>,
    /// Include payload in the response.
    pub with_payload: Option<WithPayload>,
    /// Optional filter applied to all candidates.
    pub filter: Option<Filter>,
    /// Minimum score threshold; candidates scoring below are dropped.
    pub score_threshold: Option<f32>,
    /// Search tuning parameters.
    pub params: Option<SearchParams>,
}

impl From<QueryRequest> for ShardQueryRequest {
    fn from(r: QueryRequest) -> Self {
        ShardQueryRequest {
            prefetches: r.prefetches.into_iter().map(ShardPrefetch::from).collect(),
            limit: r.limit as usize,
            offset: r.offset.unwrap_or(0) as usize,
            with_vector: r.with_vector.map(SegmentWithVector::from).unwrap_or_default(),
            with_payload: r
                .with_payload
                .map(WithPayloadInterface::from)
                .unwrap_or_default(),
            query: r.query.map(shard::query::ScoringQuery::from),
            filter: r.filter.map(SegmentFilter::from),
            score_threshold: r.score_threshold.map(OrderedFloat),
            params: r.params.map(SegmentSearchParams::from),
        }
    }
}

// ── SearchRequest ───────────────────────────────────────────────────────────

/// A plain nearest-neighbor search request.
///
/// Use this for the common case of "find the K most similar points to this
/// vector, optionally filtered". For more advanced pipelines (fusion,
/// multi-stage retrieval) use [`QueryRequest`].
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchRequest {
    /// Vector query describing what to search for.
    pub query: Query,
    /// Maximum number of results to return.
    pub limit: u64,
    /// Number of results to skip (for pagination).
    pub offset: Option<u64>,
    /// Optional filter applied to all candidates.
    pub filter: Option<Filter>,
    /// Search tuning parameters.
    pub params: Option<SearchParams>,
    /// Include vectors in the response.
    pub with_vector: Option<WithVector>,
    /// Include payload in the response.
    pub with_payload: Option<WithPayload>,
    /// Minimum score threshold; candidates scoring below are dropped.
    pub score_threshold: Option<f32>,
}

impl From<SearchRequest> for CoreSearchRequest {
    fn from(r: SearchRequest) -> Self {
        CoreSearchRequest {
            query: QueryEnum::from(r.query),
            limit: r.limit as usize,
            offset: r.offset.unwrap_or(0) as usize,
            filter: r.filter.map(SegmentFilter::from),
            params: r.params.map(SegmentSearchParams::from),
            with_vector: r.with_vector.map(SegmentWithVector::from),
            with_payload: r.with_payload.map(WithPayloadInterface::from),
            score_threshold: r.score_threshold,
        }
    }
}

// ── ScrollRequest ───────────────────────────────────────────────────────────

/// A batched iteration request.
///
/// Pass the returned `next_offset` from
/// [`ScrollResponse`](crate::ScrollResponse) as `offset` on the next call to
/// page through all matching points.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ScrollRequest {
    /// Opaque cursor from a previous scroll, or `None`/`null` to start
    /// from the beginning.
    pub offset: Option<PointId>,
    /// Maximum points per page. Defaults to a server-side value when unset.
    pub limit: Option<u64>,
    /// Optional filter applied to the iteration.
    pub filter: Option<Filter>,
    /// Include payload in each record.
    pub with_payload: Option<WithPayload>,
    /// Include vectors in each record.
    pub with_vector: Option<WithVector>,
    /// Iterate in payload-key order instead of ID order. Requires a
    /// payload index on the key.
    pub order_by: Option<OrderBy>,
}

impl From<ScrollRequest> for ScrollRequestInternal {
    fn from(r: ScrollRequest) -> Self {
        ScrollRequestInternal {
            offset: r.offset.map(PointIdType::from),
            limit: r.limit.map(|v| v as usize),
            filter: r.filter.map(SegmentFilter::from),
            with_payload: r.with_payload.map(WithPayloadInterface::from),
            with_vector: r.with_vector.map(SegmentWithVector::from).unwrap_or_default(),
            order_by: r.order_by.map(|o| OrderByInterface::Struct(SegmentOrderBy::from(o))),
        }
    }
}

// ── CountRequest ────────────────────────────────────────────────────────────

/// A point-counting request.
#[derive(Clone, Debug, uniffi::Record)]
pub struct CountRequest {
    /// Optional filter. `None`/`null` counts all points in the shard.
    pub filter: Option<Filter>,
    /// If `true`, return an exact count (scans every matching point);
    /// otherwise, the shard is free to return a fast estimate.
    pub exact: bool,
}

impl From<CountRequest> for CountRequestInternal {
    fn from(r: CountRequest) -> Self {
        CountRequestInternal {
            filter: r.filter.map(SegmentFilter::from),
            exact: r.exact,
        }
    }
}

// ── FacetRequest ────────────────────────────────────────────────────────────

/// A facet (grouped count) request.
///
/// Facets aggregate distinct payload values with counts, useful for
/// building UI filter sidebars.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetRequest {
    /// Payload key to facet on. Must have a payload index.
    pub key: String,
    /// Maximum number of distinct values to return.
    pub limit: u64,
    /// If `true`, count every matching point; otherwise a fast estimate
    /// may be returned.
    pub exact: bool,
    /// Optional filter restricting which points participate in the facet.
    pub filter: Option<Filter>,
}

impl From<FacetRequest> for FacetRequestInternal {
    fn from(r: FacetRequest) -> Self {
        FacetRequestInternal {
            key: r.key.parse().expect("valid json path"),
            limit: r.limit as usize,
            filter: r.filter.map(SegmentFilter::from),
            exact: r.exact,
        }
    }
}

// ── FacetResponse ───────────────────────────────────────────────────────────

/// A single (value, count) row in a facet response.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetHit {
    /// Facet value, stringified. Integer/UUID/bool values are rendered as
    /// their canonical string form.
    pub value: String,
    /// Number of points with this facet value.
    pub count: u64,
}

/// The result of a facet aggregation.
#[derive(Clone, Debug, uniffi::Record)]
pub struct FacetResponse {
    /// Hits in descending count order, up to `FacetRequest.limit` entries.
    pub hits: Vec<FacetHit>,
}

// ── ShardInfo ───────────────────────────────────────────────────────────────

/// Summary information about a shard's on-disk state.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ShardInfo {
    /// Number of segments in the shard.
    pub segments_count: u64,
    /// Total number of points across all segments.
    pub points_count: u64,
    /// Number of vectors that have been indexed (i.e. reachable via ANN).
    pub indexed_vectors_count: u64,
}
