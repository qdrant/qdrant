//! Types used within `LocalShard` to represent a planned `ShardQueryRequest`

use api::rest::OrderByInterface;
use common::types::ScoreType;
use segment::types::{Filter, WithPayloadInterface, WithVector};

use super::shard_query::{ScoringQuery, ShardPrefetch, ShardQueryRequest};
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, QueryScrollRequestInternal,
};

const MAX_PREFETCH_DEPTH: usize = 64;

/// The planned representation of a [ShardQueryRequest], which flattens all the
/// leaf queries into a batch of searches and scrolls
#[derive(Debug)]
pub struct PlannedQuery {
    /// References to the searches and scrolls, and how to merge them.
    /// This retains the recursive structure of the original query.
    pub merge_plan: MergePlan,

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

    /// How many points to skip in the final result set, after re-scoring
    pub offset: usize,

    /// Keep only points with better score than this threshold
    pub score_threshold: Option<ScoreType>,

    /// The vector(s) to return
    pub with_vector: WithVector,

    /// The payload to return
    pub with_payload: WithPayloadInterface,
}

#[derive(Debug, PartialEq)]
pub enum Source {
    /// A reference offset into the main search batch
    SearchesIdx(usize),

    /// A reference offset into the scrolls list
    ScrollsIdx(usize),

    /// A nested prefetch
    Prefetch(MergePlan),
}

#[derive(Debug, PartialEq)]
pub struct MergePlan {
    /// Gather all these sources
    pub sources: Vec<Source>,

    /// How to merge the sources
    ///
    /// If this is [None], then sources must be of length 1,
    /// and it is a root query without prefetches
    pub rescore_params: Option<RescoreParams>,
}

impl TryFrom<ShardQueryRequest> for PlannedQuery {
    type Error = CollectionError;

    fn try_from(request: ShardQueryRequest) -> CollectionResult<Self> {
        let depth = request.prefetches_depth();
        let ShardQueryRequest {
            prefetches,
            query,
            filter: req_filter,
            score_threshold: req_score_threshold,
            limit,
            offset: req_offset,
            with_vector: req_with_vector,
            with_payload: req_with_payload,
            params,
        } = request;

        let mut searches = Vec::new();
        let mut scrolls = Vec::new();

        let merge_plan = if !prefetches.is_empty() {
            if depth > MAX_PREFETCH_DEPTH {
                return Err(CollectionError::bad_request(format!(
                    "prefetches depth {} exceeds max depth {}",
                    depth, MAX_PREFETCH_DEPTH
                )));
            }

            let sources = recurse_prefetches(
                &mut searches,
                &mut scrolls,
                prefetches,
                req_offset,
                req_filter,
            )?;
            let rescore = query.ok_or_else(|| {
                CollectionError::bad_request("cannot have prefetches without a query".to_string())
            })?;

            MergePlan {
                sources,
                rescore_params: Some(RescoreParams {
                    rescore,
                    limit,
                    offset: req_offset,
                    score_threshold: req_score_threshold,
                    with_vector: req_with_vector,
                    with_payload: req_with_payload,
                }),
            }
        } else {
            let sources = match query {
                Some(ScoringQuery::Vector(query)) => {
                    // Everything should come from 1 core search
                    let core_search = CoreSearchRequest {
                        query,
                        filter: req_filter,
                        score_threshold: req_score_threshold,
                        with_vector: Some(req_with_vector),
                        with_payload: Some(req_with_payload),
                        offset: req_offset,
                        params,
                        limit,
                    };

                    searches.push(core_search);

                    vec![Source::SearchesIdx(0)]
                }
                Some(ScoringQuery::Fusion(_)) => {
                    return Err(CollectionError::bad_request(
                        "cannot apply Fusion without prefetches".to_string(),
                    ))
                }
                Some(ScoringQuery::OrderBy(order_by)) => {
                    // Everything should come from 1 scroll
                    let scroll = QueryScrollRequestInternal {
                        order_by: Some(OrderByInterface::Struct(order_by)),
                        filter: req_filter,
                        with_vector: req_with_vector,
                        with_payload: Some(req_with_payload),
                        limit: Some(limit),
                        offset: Some(req_offset),
                    };

                    scrolls.push(scroll);

                    vec![Source::ScrollsIdx(0)]
                }
                None => {
                    // Everything should come from 1 scroll
                    let scroll = QueryScrollRequestInternal {
                        order_by: None,
                        filter: req_filter,
                        with_vector: req_with_vector,
                        with_payload: Some(req_with_payload),
                        limit: Some(limit),
                        offset: Some(req_offset),
                    };

                    scrolls.push(scroll);

                    vec![Source::ScrollsIdx(0)]
                }
            };

            // Root-level query without prefetches is the only case where merge is `None`
            MergePlan {
                sources,
                rescore_params: None,
            }
        };

        Ok(Self {
            merge_plan,
            searches,
            scrolls,
        })
    }
}

/// Recursively construct a merge_plan for prefetches
fn recurse_prefetches(
    core_searches: &mut Vec<CoreSearchRequest>,
    scrolls: &mut Vec<QueryScrollRequestInternal>,
    prefetches: Vec<ShardPrefetch>,
    offset: usize, // Offset is added to all prefetches, so we make sure we have enough
    propagate_filter: Option<Filter>, // Global filter to apply to all prefetches
) -> CollectionResult<Vec<Source>> {
    let mut sources = Vec::with_capacity(prefetches.len());

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
        let limit = prefetch_limit + offset;

        // Filters are propagated into the leaves
        let filter = Filter::merge_opts(propagate_filter.clone(), filter);

        let source = if prefetches.is_empty() {
            // This is a leaf prefetch. Fetch this info from the segments
            match query {
                Some(ScoringQuery::Vector(query_enum)) => {
                    let core_search = CoreSearchRequest {
                        query: query_enum,
                        filter,
                        params,
                        limit,
                        offset: 0,
                        with_payload: Some(WithPayloadInterface::Bool(false)),
                        with_vector: Some(WithVector::Bool(false)),
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
                        order_by: Some(OrderByInterface::Struct(order_by)),
                        filter,
                        with_vector: WithVector::Bool(false),
                        with_payload: Some(WithPayloadInterface::Bool(false)),
                        limit: Some(limit),
                        offset: None,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    Source::ScrollsIdx(idx)
                }
                None => {
                    let scroll = QueryScrollRequestInternal {
                        order_by: None,
                        filter,
                        with_vector: WithVector::Bool(false),
                        with_payload: Some(WithPayloadInterface::Bool(false)),
                        limit: Some(limit),
                        offset: None,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    Source::ScrollsIdx(idx)
                }
            }
        } else {
            // This has nested prefetches. Recurse into them
            let inner_sources =
                recurse_prefetches(core_searches, scrolls, prefetches, offset, filter)?;

            let rescore = query.ok_or_else(|| {
                CollectionError::bad_request("cannot have prefetches without a query".to_string())
            })?;

            let merge_plan = MergePlan {
                sources: inner_sources,
                rescore_params: Some(RescoreParams {
                    rescore,
                    limit,
                    offset: 0, // Only apply offset at the root
                    score_threshold,
                    // We never need payloads and vectors for prefetch queries with re-scores
                    with_vector: WithVector::Bool(false),
                    with_payload: WithPayloadInterface::Bool(false),
                }),
            };

            Source::Prefetch(merge_plan)
        };
        sources.push(source);
    }

    Ok(sources)
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use segment::data_types::vectors::{MultiDenseVector, NamedVectorStruct, Vector};
    use segment::json_path::JsonPath;
    use segment::types::{
        Condition, FieldCondition, Filter, Match, SearchParams, WithPayloadInterface, WithVector,
    };
    use sparse::common::sparse_vector::SparseVector;

    use super::*;
    use crate::operations::query_enum::QueryEnum;
    use crate::operations::universal_query::shard_query::Fusion;

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
                            Vector::Dense(dummy_vector.clone()),
                            "byte",
                        ),
                    ))),
                    limit: 1000,
                    params: None,
                    filter: Some(filter_inner_inner.clone()),
                    score_threshold: None,
                }],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(Vector::Dense(dummy_vector.clone()), "full"),
                ))),
                limit: 100,
                params: None,
                filter: Some(filter_inner.clone()),
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    Vector::MultiDense(MultiDenseVector::new_unchecked(vec![dummy_vector.clone()])),
                    "multi",
                ),
            ))),
            filter: Some(filter_outer.clone()),
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(request).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    Vector::Dense(dummy_vector.clone()),
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
            planned_query.merge_plan,
            MergePlan {
                sources: vec![Source::Prefetch(MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedVectorStruct::new_from_vector(
                                Vector::Dense(dummy_vector.clone()),
                                "full",
                            )
                        )),
                        limit: 100,
                        offset: 0,
                        score_threshold: None,
                        with_vector: WithVector::Bool(false),
                        with_payload: WithPayloadInterface::Bool(false),
                    })
                })],
                rescore_params: Some(RescoreParams {
                    rescore: ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            Vector::MultiDense(MultiDenseVector::new_unchecked(vec![
                                dummy_vector.clone()
                            ])),
                            "multi"
                        )
                    )),
                    limit: 10,
                    offset: 0,
                    score_threshold: None,
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(true),
                })
            }
        );
    }

    #[test]
    fn test_try_from_no_prefetch() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let request = ShardQueryRequest {
            prefetches: vec![], // No prefetch
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(Vector::Dense(dummy_vector.clone()), "full"),
            ))),
            filter: Some(Filter::default()),
            score_threshold: Some(0.5),
            limit: 10,
            offset: 12,
            params: Some(SearchParams::default()),
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(request).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    Vector::Dense(dummy_vector.clone()),
                    "full",
                )),
                filter: Some(Filter::default()),
                params: Some(SearchParams::default()),
                limit: 10,
                offset: 12,
                with_vector: Some(WithVector::Bool(true)),
                with_payload: Some(WithPayloadInterface::Bool(true)),
                score_threshold: Some(0.5),
            }]
        );

        assert_eq!(
            planned_query.merge_plan,
            MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: None,
            }
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
                            Vector::Dense(dummy_vector.clone()),
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
                            Vector::Sparse(dummy_sparse.clone()),
                            "sparse",
                        ),
                    ))),
                    limit: 100,
                    params: None,
                    filter: Some(filter_inner2.clone()),
                    score_threshold: None,
                },
            ],
            query: Some(ScoringQuery::Fusion(Fusion::Rrf)),
            filter: Some(filter_outer.clone()),
            score_threshold: None,
            limit: 50,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };

        let planned_query = PlannedQuery::try_from(request).unwrap();

        assert_eq!(
            planned_query.searches,
            vec![
                CoreSearchRequest {
                    query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                        Vector::Dense(dummy_vector.clone()),
                        "dense",
                    )),
                    filter: Some(filter_outer.merge(&filter_inner1)),
                    params: None,
                    limit: 100,
                    offset: 0,
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: Some(WithVector::Bool(false)),
                    score_threshold: None,
                },
                CoreSearchRequest {
                    query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                        Vector::Sparse(dummy_sparse.clone()),
                        "sparse",
                    )),
                    filter: Some(filter_outer.merge(&filter_inner2)),
                    params: None,
                    limit: 100,
                    offset: 0,
                    with_payload: Some(WithPayloadInterface::Bool(false)),
                    with_vector: Some(WithVector::Bool(false)),
                    score_threshold: None,
                }
            ]
        );

        assert_eq!(
            planned_query.merge_plan,
            MergePlan {
                sources: vec![Source::SearchesIdx(0), Source::SearchesIdx(1)],
                rescore_params: Some(RescoreParams {
                    rescore: ScoringQuery::Fusion(Fusion::Rrf),
                    limit: 50,
                    offset: 0,
                    score_threshold: None,
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(false),
                })
            }
        );
    }

    #[test]
    fn test_try_from_rrf_without_source() {
        let request = ShardQueryRequest {
            prefetches: vec![],
            query: Some(ScoringQuery::Fusion(Fusion::Rrf)),
            filter: Some(Filter::default()),
            score_threshold: None,
            limit: 50,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };

        let planned_query = PlannedQuery::try_from(request);

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
                Match::new_value(segment::types::ValueVariants::Keyword("hello".to_string())),
            ),
        )));

        let request = ShardQueryRequest {
            prefetches: vec![ShardPrefetch {
                prefetches: Vec::new(),
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(
                        Vector::Dense(dummy_vector.clone()),
                        "dense",
                    ),
                ))),
                limit: 37,
                params: dummy_params,
                filter: dummy_filter.clone(),
                score_threshold: Some(0.1),
            }],
            query: Some(ScoringQuery::Fusion(Fusion::Rrf)),
            filter: Some(Filter::default()),
            score_threshold: Some(0.666),
            limit: 50,
            offset: 49,

            // these params will be ignored because we have a prefetch
            params: Some(SearchParams {
                exact: true,
                ..Default::default()
            }),
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };

        let planned_query = PlannedQuery::try_from(request).unwrap();

        assert_eq!(
            planned_query.merge_plan,
            MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: Some(RescoreParams {
                    rescore: ScoringQuery::Fusion(Fusion::Rrf),
                    limit: 50,
                    offset: 49,
                    score_threshold: Some(0.666),
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(false),
                })
            }
        );

        assert_eq!(
            planned_query.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    Vector::Dense(dummy_vector.clone()),
                    "dense",
                ),),
                filter: dummy_filter,
                params: dummy_params,
                limit: 37 + 49, // limit + offset
                offset: 0,
                with_payload: Some(WithPayloadInterface::Bool(false)),
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
                                Vector::Dense(vec![1.0, 2.0, 3.0]),
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
                NamedVectorStruct::new_from_vector(Vector::Dense(vec![1.0, 2.0, 3.0]), "dense"),
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
                NamedVectorStruct::new_from_vector(Vector::Dense(vec![1.0, 2.0, 3.0]), "dense"),
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
                            Vector::Dense(vec![1.0, 2.0, 3.0]),
                            "dense",
                        ),
                    ))),
                    limit: 10,
                    params: None,
                    filter: None,
                    score_threshold: None,
                }],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(Vector::Dense(vec![1.0, 2.0, 3.0]), "dense"),
                ))),
                limit: 10,
                params: None,
                filter: None,
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(Vector::Dense(vec![1.0, 2.0, 3.0]), "dense"),
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
        let _planned_query = PlannedQuery::try_from(request.clone()).unwrap();

        request.prefetches = vec![make_prefetches_at_depth(64)];
        assert_eq!(request.prefetches_depth(), 64);
        let _planned_query = PlannedQuery::try_from(request.clone()).unwrap();

        request.prefetches = vec![make_prefetches_at_depth(65)];
        assert_eq!(request.prefetches_depth(), 65);
        // assert error
        let err_description = "prefetches depth 65 exceeds max depth 64".to_string();
        matches!(PlannedQuery::try_from(request), Err(CollectionError::BadRequest { description}) if description == err_description);
    }
}
