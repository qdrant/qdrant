#[cfg(feature = "api")]
mod conversions;
pub mod formula;
pub mod mmr;
pub mod planned_query;
pub mod query_enum;
pub mod scroll;
mod validation;

pub mod query_context;
#[cfg(test)]
mod tests;

use common::types::ScoreType;
use ordered_float::OrderedFloat;
use segment::data_types::order_by::OrderBy;
use segment::data_types::vectors::VectorInternal;
use segment::index::query_optimization::rescore_formula::parsed_formula::ParsedFormula;
use segment::types::*;
use serde::Serialize;

use self::query_enum::*;
use crate::search::CoreSearchRequest;

/// Internal response type for a universal query request.
///
/// Capable of returning multiple intermediate results if needed, like the case of RRF (Reciprocal Rank Fusion)
pub type ShardQueryResponse = Vec<Vec<ScoredPoint>>;

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

    pub fn filter_refs(&self) -> Vec<Option<&Filter>> {
        let mut filters = vec![];
        filters.push(self.filter.as_ref());

        for prefetch in &self.prefetches {
            filters.extend(prefetch.filter_refs())
        }

        filters
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

    fn filter_refs(&self) -> Vec<Option<&Filter>> {
        let mut filters = vec![];

        filters.push(self.filter.as_ref());

        for prefetch in &self.prefetches {
            filters.extend(prefetch.filter_refs())
        }

        filters
    }
}

/// Same as `Query`, but with the resolved vector references.
#[derive(Clone, Debug, PartialEq, Hash, Serialize)]
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
                FusionInternal::Rrf { k: _, weights: _ } => true,
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

#[derive(Clone, Debug, PartialEq, Hash, Serialize)]
pub enum FusionInternal {
    /// Reciprocal Rank Fusion with optional weights per prefetch
    Rrf {
        k: usize,
        /// Weights for each prefetch source. Higher weight = more influence on final ranking.
        /// If None, all sources are weighted equally.
        weights: Option<Vec<ordered_float::OrderedFloat<f32>>>,
    },
    /// Distribution-based score fusion
    Dbsf,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize)]
pub enum SampleInternal {
    Random,
}

/// Maximal Marginal Relevance configuration
#[derive(Clone, Debug, PartialEq, Hash, Serialize)]
pub struct MmrInternal {
    /// Query vector, used to get the relevance of each point.
    pub vector: VectorInternal,
    /// Vector name to use for similarity computation, defaults to empty string (default vector)
    pub using: VectorNameBuf,
    /// Lambda parameter controlling diversity vs relevance trade-off (0.0 = full diversity, 1.0 = full relevance)
    pub lambda: OrderedFloat<f32>,
    /// Maximum number of candidates to preselect using nearest neighbors.
    pub candidates_limit: usize,
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
