use common::types::ScoreType;
use ordered_float::OrderedFloat;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::vectors::NamedQuery;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};

use super::query_enum::QueryEnum;
use super::scroll::{QueryScrollRequestInternal, ScrollOrder};
use super::*;
use crate::search::CoreSearchRequest;

const MAX_PREFETCH_DEPTH: usize = 64;

/// The planned representation of multiple [ShardQueryRequest]s, which flattens all the
/// leaf queries into a batch of searches and scrolls.
#[derive(Debug, Default)]
pub struct PlannedQuery {
    /// References to the searches and scrolls, and how to merge them.
    /// This retains the recursive structure of the original queries.
    ///
    /// One per each query in the batch
    pub root_plans: Vec<RootPlan>,

    /// All the leaf core searches
    pub searches: Vec<CoreSearchRequest>,

    /// All the leaf scrolls
    pub scrolls: Vec<QueryScrollRequestInternal>,
}

/// Defines how to merge multiple [sources](Source)
#[derive(Debug, PartialEq)]
pub struct RescoreParams {
    /// Alter the scores before selecting the best limit
    pub rescore: ScoringQuery,

    /// Keep this many points from the top
    pub limit: usize,

    /// Keep only points with better score than this threshold
    pub score_threshold: Option<OrderedFloat<ScoreType>>,

    /// Parameters for the rescore search request
    pub params: Option<SearchParams>,
}

#[derive(Debug, PartialEq)]
pub enum Source {
    /// A reference offset into the main search batch
    SearchesIdx(usize),

    /// A reference offset into the scrolls list
    ScrollsIdx(usize),

    /// A nested prefetch
    Prefetch(Box<MergePlan>),
}

#[derive(Debug, PartialEq)]
pub struct MergePlan {
    /// Gather all these sources
    pub sources: Vec<Source>,

    /// How to merge the sources
    ///
    /// If this is [None], then it means one of two things:
    /// 1. It is a top-level query without prefetches, so sources must be of length 1.
    /// 2. It is a top-level fusion query, so sources will be returned as-is. They will be merged later at collection level
    pub rescore_params: Option<RescoreParams>,
}

#[derive(Debug, PartialEq)]
pub struct RootPlan {
    pub merge_plan: MergePlan,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

impl PlannedQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, request: ShardQueryRequest) -> OperationResult<()> {
        let depth = request.prefetches_depth();
        if depth > MAX_PREFETCH_DEPTH {
            return Err(OperationError::validation_error(format!(
                "prefetches depth {depth} exceeds max depth {MAX_PREFETCH_DEPTH}"
            )));
        }

        let ShardQueryRequest {
            prefetches,
            query,
            filter,
            score_threshold,
            limit,
            offset,
            with_vector,
            with_payload,
            params,
        } = request;

        // Adjust limit so that we have enough results when we cut off the offset at a higher level
        let limit = limit + offset;

        // Adjust with_vector based on the root query variant
        let with_vector = match &query {
            None
            | Some(ScoringQuery::Vector(_))
            | Some(ScoringQuery::Fusion(_))
            | Some(ScoringQuery::OrderBy(_))
            | Some(ScoringQuery::Formula(_))
            | Some(ScoringQuery::Sample(_)) => with_vector,
            Some(ScoringQuery::Mmr(mmr)) => with_vector.merge(&WithVector::from(mmr.using.clone())),
        };

        let root_plan = if prefetches.is_empty() {
            self.root_plan_without_prefetches(
                query,
                filter,
                score_threshold.map(OrderedFloat::into_inner),
                with_vector,
                with_payload,
                params,
                limit,
            )?
        } else {
            self.root_plan_with_prefetches(
                prefetches,
                query,
                filter,
                score_threshold.map(OrderedFloat::into_inner),
                with_vector,
                with_payload,
                params,
                limit,
            )?
        };

        self.root_plans.push(root_plan);

        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    fn root_plan_without_prefetches(
        &mut self,
        query: Option<ScoringQuery>,
        filter: Option<Filter>,
        score_threshold: Option<f32>,
        with_vector: WithVector,
        with_payload: WithPayloadInterface,
        params: Option<SearchParams>,
        limit: usize,
    ) -> OperationResult<RootPlan> {
        // Everything must come from a single source.
        let sources = vec![leaf_source_from_scoring_query(
            &mut self.searches,
            &mut self.scrolls,
            query,
            limit,
            params,
            score_threshold,
            filter,
        )?];

        let merge_plan = MergePlan {
            sources,
            // Root-level query without prefetches means we won't do any extra rescoring
            rescore_params: None,
        };
        Ok(RootPlan {
            merge_plan,
            with_vector,
            with_payload,
        })
    }

    #[expect(clippy::too_many_arguments)]
    fn root_plan_with_prefetches(
        &mut self,
        prefetches: Vec<ShardPrefetch>,
        query: Option<ScoringQuery>,
        filter: Option<Filter>,
        score_threshold: Option<f32>,
        with_vector: WithVector,
        with_payload: WithPayloadInterface,
        params: Option<SearchParams>,
        limit: usize,
    ) -> OperationResult<RootPlan> {
        let rescoring_query = query.ok_or_else(|| {
            OperationError::validation_error("cannot have prefetches without a query".to_string())
        })?;

        let sources =
            recurse_prefetches(&mut self.searches, &mut self.scrolls, prefetches, &filter)?;

        let rescore_params = match rescoring_query {
            ScoringQuery::Mmr(MmrInternal {
                vector,
                using,
                lambda: _,
                candidates_limit,
            }) => {
                let rescore_params = RescoreParams {
                    rescore: ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new_from_vector(
                        vector, using,
                    ))),
                    limit: candidates_limit,
                    score_threshold: score_threshold.map(OrderedFloat),
                    params,
                };
                // Although MMR gets computed at collection level, we select top candidates via a nearest rescoring first
                Some(rescore_params)
            }
            rescore @ (ScoringQuery::Vector(_)
            | ScoringQuery::OrderBy(_)
            | ScoringQuery::Formula(_)
            | ScoringQuery::Sample(_)) => Some(RescoreParams {
                rescore,
                limit,
                score_threshold: score_threshold.map(OrderedFloat),
                params,
            }),
            // We will propagate the intermediate results. Fusion will take place at collection level.
            // It is fine if this is None here.
            ScoringQuery::Fusion(_) => None,
        };

        let merge_plan = MergePlan {
            sources,
            rescore_params,
        };

        Ok(RootPlan {
            merge_plan,
            with_vector,
            with_payload,
        })
    }

    pub fn scrolls(&self) -> &Vec<QueryScrollRequestInternal> {
        &self.scrolls
    }
}

/// Recursively construct a merge_plan for prefetches
fn recurse_prefetches(
    core_searches: &mut Vec<CoreSearchRequest>,
    scrolls: &mut Vec<QueryScrollRequestInternal>,
    prefetches: Vec<ShardPrefetch>,
    propagate_filter: &Option<Filter>, // Global filter to apply to all prefetches
) -> OperationResult<Vec<Source>> {
    let mut sources = Vec::with_capacity(prefetches.len());

    for prefetch in prefetches {
        let ShardPrefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = prefetch;

        // Filters are propagated into the leaves
        let filter = Filter::merge_opts(propagate_filter.clone(), filter);

        let source = if prefetches.is_empty() {
            // This is a leaf prefetch. Fetch this info from the segments
            leaf_source_from_scoring_query(
                core_searches,
                scrolls,
                query,
                limit,
                params,
                score_threshold.map(OrderedFloat::into_inner),
                filter,
            )?
        } else {
            // This has nested prefetches. Recurse into them
            let inner_sources = recurse_prefetches(core_searches, scrolls, prefetches, &filter)?;

            let rescore = query.ok_or_else(|| {
                OperationError::validation_error(
                    "cannot have prefetches without a query".to_string(),
                )
            })?;

            let merge_plan = MergePlan {
                sources: inner_sources,
                rescore_params: Some(RescoreParams {
                    rescore,
                    limit,
                    score_threshold,
                    params,
                }),
            };

            Source::Prefetch(Box::new(merge_plan))
        };
        sources.push(source);
    }

    Ok(sources)
}

/// Crafts a "leaf source" from a scoring query. This means that the scoring query
/// does not act over prefetched points and will be executed over the segments directly.
///
/// Only `Source::SearchesIdx` or `Source::ScrollsIdx` variants are returned.
fn leaf_source_from_scoring_query(
    core_searches: &mut Vec<CoreSearchRequest>,
    scrolls: &mut Vec<QueryScrollRequestInternal>,
    query: Option<ScoringQuery>,
    limit: usize,
    params: Option<SearchParams>,
    score_threshold: Option<f32>,
    filter: Option<Filter>,
) -> OperationResult<Source> {
    let source = match query {
        Some(ScoringQuery::Vector(query_enum)) => {
            let core_search = CoreSearchRequest {
                query: query_enum,
                filter,
                params,
                limit,
                offset: 0,
                with_vector: Some(WithVector::from(false)),
                with_payload: Some(WithPayloadInterface::from(false)),
                score_threshold,
            };

            let idx = core_searches.len();
            core_searches.push(core_search);

            Source::SearchesIdx(idx)
        }
        Some(ScoringQuery::Fusion(_)) => {
            return Err(OperationError::validation_error(
                "cannot apply Fusion without prefetches".to_string(),
            ));
        }
        Some(ScoringQuery::OrderBy(order_by)) => {
            let scroll = QueryScrollRequestInternal {
                scroll_order: ScrollOrder::ByField(order_by),
                filter,
                with_vector: WithVector::from(false),
                with_payload: WithPayloadInterface::from(false),
                limit,
            };

            let idx = scrolls.len();
            scrolls.push(scroll);

            Source::ScrollsIdx(idx)
        }
        Some(ScoringQuery::Formula(_)) => {
            return Err(OperationError::validation_error(
                "cannot apply Formula without prefetches".to_string(),
            ));
        }
        Some(ScoringQuery::Sample(SampleInternal::Random)) => {
            let scroll = QueryScrollRequestInternal {
                scroll_order: ScrollOrder::Random,
                filter,
                with_vector: WithVector::from(false),
                with_payload: WithPayloadInterface::from(false),
                limit,
            };

            let idx = scrolls.len();
            scrolls.push(scroll);

            Source::ScrollsIdx(idx)
        }
        Some(ScoringQuery::Mmr(MmrInternal {
            vector,
            using,
            lambda: _,
            candidates_limit,
        })) => {
            let query = QueryEnum::Nearest(NamedQuery::new_from_vector(vector, using));

            let core_search = CoreSearchRequest {
                query,
                filter,
                score_threshold,
                with_vector: Some(WithVector::from(false)),
                with_payload: Some(WithPayloadInterface::from(false)),
                offset: 0,
                params,
                limit: candidates_limit,
            };

            let idx = core_searches.len();
            core_searches.push(core_search);

            Source::SearchesIdx(idx)
        }
        None => {
            let scroll = QueryScrollRequestInternal {
                scroll_order: Default::default(),
                filter,
                with_vector: WithVector::from(false),
                with_payload: WithPayloadInterface::from(false),
                limit,
            };

            let idx = scrolls.len();
            scrolls.push(scroll);

            Source::ScrollsIdx(idx)
        }
    };

    Ok(source)
}

impl TryFrom<Vec<ShardQueryRequest>> for PlannedQuery {
    type Error = OperationError;

    fn try_from(requests: Vec<ShardQueryRequest>) -> Result<Self, Self::Error> {
        let mut planned_query = Self::new();
        for request in requests {
            planned_query.add(request)?;
        }
        Ok(planned_query)
    }
}
