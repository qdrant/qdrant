//! Types used within `LocalShard` to represent a planned `ShardQueryRequest`

use common::types::ScoreType;
use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};

use super::shard_query::{SampleInternal, ScoringQuery, ShardPrefetch, ShardQueryRequest};
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, QueryScrollRequestInternal, ScrollOrder,
};

const MAX_PREFETCH_DEPTH: usize = 64;

/// The planned representation of multiple [ShardQueryRequest]s, which flattens all the
/// leaf queries into a batch of searches and scrolls.
#[derive(Debug, Default)]
pub struct PlannedQuery {
    /// References to the searches and scrolls, and how to merge them.
    /// This retains the recursive structure of the original queries.
    ///
    /// One per each query in the batch
    pub root_plans: Vec<MergePlan>,

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
    pub score_threshold: Option<ScoreType>,

    /// The vector(s) to return
    pub with_vector: WithVector,

    /// The payload to return
    pub with_payload: WithPayloadInterface,

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

impl PlannedQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, request: ShardQueryRequest) -> CollectionResult<()> {
        let depth = request.prefetches_depth();
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
        // Final offset is handled at collection level
        let limit = limit + offset;

        let merge_plan = if !prefetches.is_empty() {
            if depth > MAX_PREFETCH_DEPTH {
                return Err(CollectionError::bad_request(format!(
                    "prefetches depth {depth} exceeds max depth {MAX_PREFETCH_DEPTH}"
                )));
            }

            let rescore = query.ok_or_else(|| {
                CollectionError::bad_request("cannot have prefetches without a query".to_string())
            })?;

            if rescore.needs_intermediate_results() {
                // pass `with_vector` and `with_payload` down one level, as the sources will be sent as intermediate results to the collection
                let sources = recurse_prefetches(
                    &mut self.searches,
                    &mut self.scrolls,
                    prefetches,
                    offset,
                    &filter,
                    Some((with_payload, with_vector)),
                )?;

                MergePlan {
                    sources,
                    // We will propagate the intermediate results, the fusion will take place at collection level.
                    // It is fine to lose this rescore information here.
                    rescore_params: None,
                }
            } else {
                let sources = recurse_prefetches(
                    &mut self.searches,
                    &mut self.scrolls,
                    prefetches,
                    offset,
                    &filter,
                    None,
                )?;

                MergePlan {
                    sources,
                    rescore_params: Some(RescoreParams {
                        rescore,
                        limit,
                        score_threshold,
                        with_vector,
                        with_payload,
                        params,
                    }),
                }
            }
        } else {
            let sources = match query {
                Some(ScoringQuery::Vector(query)) => {
                    // Everything should come from 1 core search
                    let core_search = CoreSearchRequest {
                        query,
                        filter,
                        score_threshold,
                        with_vector: Some(with_vector),
                        with_payload: Some(with_payload),
                        offset: 0, // offset is handled at collection level
                        params,
                        limit,
                    };

                    let idx = self.searches.len();
                    self.searches.push(core_search);

                    vec![Source::SearchesIdx(idx)]
                }
                Some(ScoringQuery::Fusion(_)) => {
                    return Err(CollectionError::bad_request(
                        "cannot apply Fusion without prefetches".to_string(),
                    ))
                }
                Some(ScoringQuery::OrderBy(order_by)) => {
                    // Everything should come from 1 scroll
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: ScrollOrder::ByField(order_by),
                        limit,
                        filter,
                        with_vector,
                        with_payload,
                    };

                    let idx = self.scrolls.len();
                    self.scrolls.push(scroll);

                    vec![Source::ScrollsIdx(idx)]
                }
                Some(ScoringQuery::Sample(SampleInternal::Random)) => {
                    // Everything should come from 1 scroll
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: ScrollOrder::Random,
                        limit,
                        filter,
                        with_vector,
                        with_payload,
                    };

                    let idx = self.scrolls.len();
                    self.scrolls.push(scroll);

                    vec![Source::ScrollsIdx(idx)]
                }
                None => {
                    // Everything should come from 1 scroll
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: ScrollOrder::ById,
                        limit,
                        filter,
                        with_vector,
                        with_payload,
                    };

                    let idx = self.scrolls.len();
                    self.scrolls.push(scroll);

                    vec![Source::ScrollsIdx(idx)]
                }
            };

            // Root-level query without prefetches is the only case where merge is `None`
            MergePlan {
                sources,
                rescore_params: None,
            }
        };

        self.root_plans.push(merge_plan);

        Ok(())
    }
}

/// Recursively construct a merge_plan for prefetches
fn recurse_prefetches(
    core_searches: &mut Vec<CoreSearchRequest>,
    scrolls: &mut Vec<QueryScrollRequestInternal>,
    prefetches: Vec<ShardPrefetch>,
    root_offset: usize, // Offset is added to all prefetches, so we make sure we have enough
    propagate_filter: &Option<Filter>, // Global filter to apply to all prefetches
    // Top-level fusion requests won't be merged on shard level, so we pass these params down one level to fetch on the sources.
    // Otherwise we would miss to fetch the payload and vector.
    with_payload_and_vector: Option<(WithPayloadInterface, WithVector)>,
) -> CollectionResult<Vec<Source>> {
    let mut sources = Vec::with_capacity(prefetches.len());

    let (with_payload, with_vector) = with_payload_and_vector
        .unwrap_or((WithPayloadInterface::Bool(false), WithVector::Bool(false)));

    for prefetch in prefetches {
        let ShardPrefetch {
            prefetches,
            query,
            limit: prefetch_limit,
            params,
            filter,
            score_threshold,
        } = prefetch;

        // Offset is replicated at each step from the root to the leaves
        let limit = prefetch_limit + root_offset;

        // Filters are propagated into the leaves
        let filter = Filter::merge_opts(propagate_filter.clone(), filter);

        let source = if !prefetches.is_empty() {
            // This has nested prefetches. Recurse into them
            let inner_sources = recurse_prefetches(
                core_searches,
                scrolls,
                prefetches,
                root_offset,
                &filter,
                None,
            )?;

            let rescore = query.ok_or_else(|| {
                CollectionError::bad_request("cannot have prefetches without a query".to_string())
            })?;

            let merge_plan = MergePlan {
                sources: inner_sources,
                rescore_params: Some(RescoreParams {
                    rescore,
                    limit,
                    score_threshold,
                    with_vector: with_vector.clone(),
                    with_payload: with_payload.clone(),
                    params,
                }),
            };

            Source::Prefetch(Box::new(merge_plan))
        } else {
            // This is a leaf prefetch. Fetch this info from the segments
            match query {
                Some(ScoringQuery::Vector(query_enum)) => {
                    let core_search = CoreSearchRequest {
                        query: query_enum,
                        filter,
                        params,
                        limit,
                        offset: 0,
                        with_vector: Some(with_vector.clone()),
                        with_payload: Some(with_payload.clone()),
                        score_threshold,
                    };

                    let idx = core_searches.len();
                    core_searches.push(core_search);

                    Source::SearchesIdx(idx)
                }
                Some(ScoringQuery::Fusion(_)) => {
                    return Err(CollectionError::bad_request(
                        "cannot apply Fusion without prefetches".to_string(),
                    ))
                }
                Some(ScoringQuery::OrderBy(order_by)) => {
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: ScrollOrder::ByField(order_by),
                        filter,
                        with_vector: with_vector.clone(),
                        with_payload: with_payload.clone(),
                        limit,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    Source::ScrollsIdx(idx)
                }
                Some(ScoringQuery::Sample(SampleInternal::Random)) => {
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: ScrollOrder::Random,
                        filter,
                        with_vector: with_vector.clone(),
                        with_payload: with_payload.clone(),
                        limit,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    Source::ScrollsIdx(idx)
                }
                None => {
                    let scroll = QueryScrollRequestInternal {
                        scroll_order: Default::default(),
                        filter,
                        with_vector: with_vector.clone(),
                        with_payload: with_payload.clone(),
                        limit,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    Source::ScrollsIdx(idx)
                }
            }
        };
        sources.push(source);
    }

    Ok(sources)
}

impl TryFrom<Vec<ShardQueryRequest>> for PlannedQuery {
    type Error = CollectionError;

    fn try_from(requests: Vec<ShardQueryRequest>) -> Result<Self, Self::Error> {
        let mut planned_query = Self::new();
        for request in requests {
            planned_query.add(request)?;
        }
        Ok(planned_query)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use segment::data_types::vectors::{
        MultiDenseVectorInternal, NamedVectorStruct, VectorInternal,
    };
    use segment::json_path::JsonPath;
    use segment::types::{
        Condition, FieldCondition, Filter, Match, SearchParams, WithPayloadInterface, WithVector,
    };
    use sparse::common::sparse_vector::SparseVector;

    use super::*;
    use crate::operations::query_enum::QueryEnum;
    use crate::operations::universal_query::shard_query::FusionInternal;

    #[test]
    fn test_try_from_double_rescore() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let filter_inner_inner = Filter::new_must_not(Condition::IsNull(
            JsonPath::try_from("apples").unwrap().into(),
        ));
        let filter_inner = Filter::new_must(Condition::Field(FieldCondition::new_match(
            "has_oranges".try_into().unwrap(),
            true.into(),
        )));
        let filter_outer =
            Filter::new_must(Condition::HasId(HashSet::from([1.into(), 2.into()]).into()));

        let request = ShardQueryRequest {
            prefetches: vec![ShardPrefetch {
                prefetches: vec![ShardPrefetch {
                    prefetches: Default::default(),
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            VectorInternal::Dense(dummy_vector.clone()),
                            "byte",
                        ),
                    ))),
                    limit: 1000,
                    params: None,
                    filter: Some(filter_inner_inner.clone()),
                    score_threshold: None,
                }],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(
                        VectorInternal::Dense(dummy_vector.clone()),
                        "full",
                    ),
                ))),
                limit: 100,
                params: None,
                filter: Some(filter_inner.clone()),
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(vec![
                        dummy_vector.clone(),
                    ])),
                    "multi",
                ),
            ))),
            filter: Some(filter_outer.clone()),
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: Some(SearchParams {
                exact: true,
                ..Default::default()
            }),
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(vec![request]).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(dummy_vector.clone()),
                    "byte",
                )),
                filter: Some(
                    filter_outer
                        .merge_owned(filter_inner)
                        .merge_owned(filter_inner_inner)
                ),
                params: None,
                limit: 1000,
                offset: 0,
                with_payload: Some(WithPayloadInterface::Bool(false)),
                with_vector: Some(WithVector::Bool(false)),
                score_threshold: None,
            }]
        );

        assert_eq!(
            planned_query.root_plans,
            vec![MergePlan {
                sources: vec![Source::Prefetch(Box::from(MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedVectorStruct::new_from_vector(
                                VectorInternal::Dense(dummy_vector.clone()),
                                "full",
                            )
                        )),
                        limit: 100,
                        score_threshold: None,
                        with_vector: WithVector::Bool(false),
                        with_payload: WithPayloadInterface::Bool(false),
                        params: None,
                    })
                }))],
                rescore_params: Some(RescoreParams {
                    rescore: ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(
                                vec![dummy_vector]
                            )),
                            "multi"
                        )
                    )),
                    limit: 10,
                    score_threshold: None,
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(true),
                    params: Some(SearchParams {
                        exact: true,
                        ..Default::default()
                    })
                })
            }]
        );
    }

    #[test]
    fn test_try_from_no_prefetch() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let request = ShardQueryRequest {
            prefetches: vec![], // No prefetch
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(dummy_vector.clone()),
                    "full",
                ),
            ))),
            filter: Some(Filter::default()),
            score_threshold: Some(0.5),
            limit: 10,
            offset: 12,
            params: Some(SearchParams::default()),
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(vec![request]).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(dummy_vector),
                    "full",
                )),
                filter: Some(Filter::default()),
                params: Some(SearchParams::default()),
                limit: 22,
                offset: 0,
                with_vector: Some(WithVector::Bool(true)),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                score_threshold: Some(0.5),
            }]
        );

        assert_eq!(
            planned_query.root_plans,
            vec![MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: None,
            }]
        );
    }

    #[test]
    fn test_try_from_hybrid_query() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let dummy_sparse = SparseVector::new(vec![100, 123, 2000], vec![0.2, 0.3, 0.4]).unwrap();

        let filter_inner1 = Filter::new_must(Condition::Field(FieldCondition::new_match(
            "city".try_into().unwrap(),
            "Berlin".to_string().into(),
        )));
        let filter_inner2 = Filter::new_must(Condition::Field(FieldCondition::new_match(
            "city".try_into().unwrap(),
            "Munich".to_string().into(),
        )));
        let filter_outer = Filter::new_must(Condition::Field(FieldCondition::new_match(
            "country".try_into().unwrap(),
            "Germany".to_string().into(),
        )));

        let request = ShardQueryRequest {
            prefetches: vec![
                ShardPrefetch {
                    prefetches: Vec::new(),
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            VectorInternal::Dense(dummy_vector.clone()),
                            "dense",
                        ),
                    ))),
                    limit: 100,
                    params: None,
                    filter: Some(filter_inner1.clone()),
                    score_threshold: None,
                },
                ShardPrefetch {
                    prefetches: Vec::new(),
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            VectorInternal::Sparse(dummy_sparse.clone()),
                            "sparse",
                        ),
                    ))),
                    limit: 100,
                    params: None,
                    filter: Some(filter_inner2.clone()),
                    score_threshold: None,
                },
            ],
            query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
            filter: Some(filter_outer.clone()),
            score_threshold: None,
            limit: 50,
            offset: 0,
            params: None,
            with_payload: WithPayloadInterface::Bool(false),
            with_vector: WithVector::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(vec![request]).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![
                CoreSearchRequest {
                    query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                        VectorInternal::Dense(dummy_vector),
                        "dense",
                    )),
                    filter: Some(filter_outer.merge(&filter_inner1)),
                    params: None,
                    limit: 100,
                    offset: 0,
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: Some(WithVector::Bool(true)),
                    score_threshold: None,
                },
                CoreSearchRequest {
                    query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                        VectorInternal::Sparse(dummy_sparse),
                        "sparse",
                    )),
                    filter: Some(filter_outer.merge(&filter_inner2)),
                    params: None,
                    limit: 100,
                    offset: 0,
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: Some(WithVector::Bool(true)),
                    score_threshold: None,
                }
            ]
        );

        assert_eq!(
            planned_query.root_plans,
            vec![MergePlan {
                sources: vec![Source::SearchesIdx(0), Source::SearchesIdx(1)],
                rescore_params: None
            }]
        );
    }

    #[test]
    fn test_try_from_rrf_without_source() {
        let request = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
            filter: Some(Filter::default()),
            score_threshold: None,
            limit: 50,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };

        let planned_query = PlannedQuery::try_from(vec![request]);

        assert!(planned_query.is_err())
    }

    #[test]
    fn test_base_params_mapping_in_try_from() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let dummy_params = Some(SearchParams {
            indexed_only: true,
            ..Default::default()
        });
        let dummy_filter = Some(Filter::new_must(Condition::Field(
            FieldCondition::new_match(
                "my_key".try_into().unwrap(),
                Match::new_value(segment::types::ValueVariants::String("hello".to_string())),
            ),
        )));

        let request = ShardQueryRequest {
            prefetches: vec![ShardPrefetch {
                prefetches: Vec::new(),
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(
                        VectorInternal::Dense(dummy_vector.clone()),
                        "dense",
                    ),
                ))),
                limit: 37,
                params: dummy_params,
                filter: dummy_filter.clone(),
                score_threshold: Some(0.1),
            }],
            query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
            filter: Some(Filter::default()),
            score_threshold: Some(0.666),
            limit: 50,
            offset: 49,

            // these params will be ignored because we have a prefetch
            params: Some(SearchParams {
                exact: true,
                ..Default::default()
            }),
            with_payload: WithPayloadInterface::Bool(true),
            with_vector: WithVector::Bool(false),
        };

        let planned_query = PlannedQuery::try_from(vec![request]).unwrap();

        assert_eq!(
            planned_query.root_plans,
            vec![MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: None
            }]
        );

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(dummy_vector),
                    "dense",
                ),),
                filter: dummy_filter,
                params: dummy_params,
                limit: 37 + 49, // limit + offset
                offset: 0,
                with_payload: Some(WithPayloadInterface::Bool(true)),
                with_vector: Some(WithVector::Bool(false)),
                score_threshold: Some(0.1)
            }]
        )
    }

    pub fn make_prefetches_at_depth(depth: usize) -> ShardPrefetch {
        // recursive helper for accumulation
        pub fn make_prefetches_at_depth_acc(depth: usize, acc: ShardPrefetch) -> ShardPrefetch {
            if depth == 0 {
                acc
            } else {
                make_prefetches_at_depth_acc(
                    depth - 1,
                    ShardPrefetch {
                        prefetches: vec![acc],
                        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedVectorStruct::new_from_vector(
                                VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                                "dense",
                            ),
                        ))),
                        limit: 10,
                        params: None,
                        filter: None,
                        score_threshold: None,
                    },
                )
            }
        }
        // lowest prefetch
        let prefetch = ShardPrefetch {
            prefetches: Vec::new(),
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                    "dense",
                ),
            ))),
            limit: 100,
            params: None,
            filter: None,
            score_threshold: None,
        };
        make_prefetches_at_depth_acc(depth - 1, prefetch)
    }

    #[test]
    fn test_detect_max_depth() {
        // depth 0
        let mut request = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                    "dense",
                ),
            ))),
            filter: None,
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };
        assert_eq!(request.prefetches_depth(), 0);

        // depth 3
        request.prefetches = vec![ShardPrefetch {
            prefetches: vec![ShardPrefetch {
                prefetches: vec![ShardPrefetch {
                    prefetches: vec![],
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                            "dense",
                        ),
                    ))),
                    limit: 10,
                    params: None,
                    filter: None,
                    score_threshold: None,
                }],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(
                        VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                        "dense",
                    ),
                ))),
                limit: 10,
                params: None,
                filter: None,
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                    "dense",
                ),
            ))),
            limit: 10,
            params: None,
            filter: None,
            score_threshold: None,
        }];
        assert_eq!(request.prefetches_depth(), 3);

        // use with helper for less boilerplate
        request.prefetches = vec![make_prefetches_at_depth(3)];
        assert_eq!(request.prefetches_depth(), 3);
        let _planned_query = PlannedQuery::try_from(vec![request.clone()]).unwrap();

        request.prefetches = vec![make_prefetches_at_depth(64)];
        assert_eq!(request.prefetches_depth(), 64);
        let _planned_query = PlannedQuery::try_from(vec![request.clone()]).unwrap();

        request.prefetches = vec![make_prefetches_at_depth(65)];
        assert_eq!(request.prefetches_depth(), 65);
        // assert error
        let err_description = "prefetches depth 65 exceeds max depth 64".to_string();
        matches!(PlannedQuery::try_from(vec![request]), Err(CollectionError::BadRequest { description}) if description == err_description);
    }

    fn dummy_core_prefetch(limit: usize) -> ShardPrefetch {
        ShardPrefetch {
            prefetches: vec![],
            query: Some(nearest_query()),
            filter: None,
            params: None,
            limit,
            score_threshold: None,
        }
    }

    fn dummy_scroll_prefetch(limit: usize) -> ShardPrefetch {
        ShardPrefetch {
            prefetches: vec![],
            query: None,
            limit,
            params: None,
            filter: None,
            score_threshold: None,
        }
    }

    fn nearest_query() -> ScoringQuery {
        ScoringQuery::Vector(QueryEnum::Nearest(NamedVectorStruct::Default(vec![
            0.1, 0.2, 0.3, 0.4,
        ])))
    }

    #[test]
    fn test_from_batch_of_requests() {
        let requests = vec![
            // A no-prefetch core_search query
            ShardQueryRequest {
                prefetches: vec![],
                query: Some(nearest_query()),
                filter: None,
                score_threshold: None,
                limit: 10,
                offset: 0,
                params: None,
                with_payload: WithPayloadInterface::Bool(false),
                with_vector: WithVector::Bool(false),
            },
            // A no-prefetch scroll query
            ShardQueryRequest {
                prefetches: vec![],
                query: None,
                filter: None,
                score_threshold: None,
                limit: 20,
                offset: 0,
                params: None,
                with_payload: WithPayloadInterface::Bool(false),
                with_vector: WithVector::Bool(false),
            },
            // A double fusion query
            ShardQueryRequest {
                prefetches: vec![
                    ShardPrefetch {
                        prefetches: vec![dummy_core_prefetch(30), dummy_core_prefetch(40)],
                        query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
                        filter: None,
                        params: None,
                        score_threshold: None,
                        limit: 10,
                    },
                    dummy_scroll_prefetch(50),
                ],
                query: Some(ScoringQuery::Fusion(FusionInternal::Rrf)),
                filter: None,
                score_threshold: None,
                limit: 10,
                offset: 0,
                params: None,
                with_payload: WithPayloadInterface::Bool(true),
                with_vector: WithVector::Bool(true),
            },
        ];

        let planned_query = PlannedQuery::try_from(requests).unwrap();
        assert_eq!(planned_query.searches.len(), 3);
        assert_eq!(planned_query.scrolls.len(), 2);
        assert_eq!(planned_query.root_plans.len(), 3);

        assert_eq!(
            planned_query.root_plans,
            vec![
                MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: None,
                },
                MergePlan {
                    sources: vec![Source::ScrollsIdx(0)],
                    rescore_params: None,
                },
                MergePlan {
                    sources: vec![
                        Source::Prefetch(Box::from(MergePlan {
                            sources: vec![Source::SearchesIdx(1), Source::SearchesIdx(2),],
                            rescore_params: Some(RescoreParams {
                                rescore: ScoringQuery::Fusion(FusionInternal::Rrf),
                                limit: 10,
                                score_threshold: None,
                                with_vector: WithVector::Bool(true),
                                with_payload: WithPayloadInterface::Bool(true),
                                params: None,
                            }),
                        })),
                        Source::ScrollsIdx(1),
                    ],
                    rescore_params: None,
                },
            ]
        );
        // assert the payload and vector settings were propagated to the other source too
        assert_eq!(planned_query.scrolls[1].with_vector, WithVector::Bool(true));
        assert_eq!(
            planned_query.scrolls[1].with_payload,
            WithPayloadInterface::Bool(true)
        );

        assert_eq!(planned_query.searches[0].limit, 10);
        assert_eq!(planned_query.searches[1].limit, 30);
        assert_eq!(planned_query.searches[2].limit, 40);

        assert_eq!(planned_query.scrolls[0].limit, 20);
        assert_eq!(planned_query.scrolls[1].limit, 50);
    }
}
