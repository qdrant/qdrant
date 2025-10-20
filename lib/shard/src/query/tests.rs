use ahash::AHashSet;
use ordered_float::OrderedFloat;
use segment::common::operation_error::OperationError;
use segment::common::reciprocal_rank_fusion::DEFAULT_RRF_K;
use segment::data_types::vectors::{MultiDenseVectorInternal, NamedQuery, VectorInternal};
use segment::json_path::JsonPath;
use segment::types::*;
use sparse::common::sparse_vector::SparseVector;

use super::planned_query::*;
use super::*;

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
    let filter_outer = Filter::new_must(Condition::HasId(
        AHashSet::from([1.into(), 2.into()]).into(),
    ));

    let request = ShardQueryRequest {
        prefetches: vec![ShardPrefetch {
            prefetches: vec![ShardPrefetch {
                prefetches: Default::default(),
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Dense(dummy_vector.clone()),
                    "byte",
                )))),
                limit: 1000,
                params: None,
                filter: Some(filter_inner_inner.clone()),
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::Dense(dummy_vector.clone()),
                "full",
            )))),
            limit: 100,
            params: None,
            filter: Some(filter_inner.clone()),
            score_threshold: None,
        }],
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(vec![
                dummy_vector.clone(),
            ])),
            "multi",
        )))),
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
            query: QueryEnum::Nearest(NamedQuery::new(
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
        vec![RootPlan {
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
            merge_plan: MergePlan {
                sources: vec![Source::Prefetch(Box::from(MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                            VectorInternal::Dense(dummy_vector.clone()),
                            "full",
                        ))),
                        limit: 100,
                        score_threshold: None,
                        params: None,
                    })
                }))],
                rescore_params: Some(RescoreParams {
                    rescore: ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                        VectorInternal::MultiDense(MultiDenseVectorInternal::new_unchecked(vec![
                            dummy_vector
                        ])),
                        "multi"
                    ))),
                    limit: 10,
                    score_threshold: None,
                    params: Some(SearchParams {
                        exact: true,
                        ..Default::default()
                    })
                })
            }
        }]
    );
}

#[test]
fn test_try_from_no_prefetch() {
    let dummy_vector = vec![1.0, 2.0, 3.0];
    let request = ShardQueryRequest {
        prefetches: vec![], // No prefetch
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::Dense(dummy_vector.clone()),
            "full",
        )))),
        filter: Some(Filter::default()),
        score_threshold: Some(OrderedFloat(0.5)),
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
            query: QueryEnum::Nearest(
                NamedQuery::new(VectorInternal::Dense(dummy_vector), "full",)
            ),
            filter: Some(Filter::default()),
            params: Some(SearchParams::default()),
            limit: 22,
            offset: 0,
            with_vector: Some(WithVector::Bool(false)),
            with_payload: Some(WithPayloadInterface::Bool(false)),
            score_threshold: Some(0.5),
        }]
    );

    assert_eq!(
        planned_query.root_plans,
        vec![RootPlan {
            with_payload: WithPayloadInterface::Bool(true),
            with_vector: WithVector::Bool(true),
            merge_plan: MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: None,
            },
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
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Dense(dummy_vector.clone()),
                    "dense",
                )))),
                limit: 100,
                params: None,
                filter: Some(filter_inner1.clone()),
                score_threshold: None,
            },
            ShardPrefetch {
                prefetches: Vec::new(),
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Sparse(dummy_sparse.clone()),
                    "sparse",
                )))),
                limit: 100,
                params: None,
                filter: Some(filter_inner2.clone()),
                score_threshold: None,
            },
        ],
        query: Some(ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K))),
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
                query: QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Dense(dummy_vector),
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
                query: QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Sparse(dummy_sparse),
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
        planned_query.root_plans,
        vec![RootPlan {
            with_payload: WithPayloadInterface::Bool(false),
            with_vector: WithVector::Bool(true),
            merge_plan: MergePlan {
                sources: vec![Source::SearchesIdx(0), Source::SearchesIdx(1)],
                rescore_params: None
            }
        }]
    );
}

#[test]
fn test_try_from_rrf_without_source() {
    let request = ShardQueryRequest {
        prefetches: vec![],
        query: Some(ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K))),
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
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::Dense(dummy_vector.clone()),
                "dense",
            )))),
            limit: 37,
            params: dummy_params,
            filter: dummy_filter.clone(),
            score_threshold: Some(OrderedFloat(0.1)),
        }],
        query: Some(ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K))),
        filter: Some(Filter::default()),
        score_threshold: Some(OrderedFloat(0.666)),
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
        vec![RootPlan {
            with_payload: WithPayloadInterface::Bool(true),
            with_vector: WithVector::Bool(false),
            merge_plan: MergePlan {
                sources: vec![Source::SearchesIdx(0)],
                rescore_params: None
            }
        }]
    );

    assert_eq!(
        planned_query.searches,
        vec![CoreSearchRequest {
            query: QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::Dense(dummy_vector),
                "dense",
            ),),
            filter: dummy_filter,
            params: dummy_params,
            limit: 37,
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
                    query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                        VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                        "dense",
                    )))),
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
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
            "dense",
        )))),
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
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
            "dense",
        )))),
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
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                    VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                    "dense",
                )))),
                limit: 10,
                params: None,
                filter: None,
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
                "dense",
            )))),
            limit: 10,
            params: None,
            filter: None,
            score_threshold: None,
        }],
        query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
            VectorInternal::Dense(vec![1.0, 2.0, 3.0]),
            "dense",
        )))),
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
    assert!(matches!(
        PlannedQuery::try_from(vec![request]),
        Err(OperationError::ValidationError { description }) if description == "prefetches depth 65 exceeds max depth 64",
    ));
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
    ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::default_dense(vec![
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
                    query: Some(ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K))),
                    filter: None,
                    params: None,
                    score_threshold: None,
                    limit: 10,
                },
                dummy_scroll_prefetch(50),
            ],
            query: Some(ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K))),
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
            RootPlan {
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(false),
                merge_plan: MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: None,
                },
            },
            RootPlan {
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(false),
                merge_plan: MergePlan {
                    sources: vec![Source::ScrollsIdx(0)],
                    rescore_params: None,
                },
            },
            RootPlan {
                with_vector: WithVector::Bool(true),
                with_payload: WithPayloadInterface::Bool(true),
                merge_plan: MergePlan {
                    sources: vec![
                        Source::Prefetch(Box::from(MergePlan {
                            sources: vec![Source::SearchesIdx(1), Source::SearchesIdx(2),],
                            rescore_params: Some(RescoreParams {
                                rescore: ScoringQuery::Fusion(FusionInternal::RrfK(DEFAULT_RRF_K)),
                                limit: 10,
                                score_threshold: None,
                                params: None,
                            }),
                        })),
                        Source::ScrollsIdx(1),
                    ],
                    rescore_params: None,
                },
            },
        ]
    );

    assert_eq!(planned_query.searches[0].limit, 10);
    assert_eq!(planned_query.searches[1].limit, 30);
    assert_eq!(planned_query.searches[2].limit, 40);

    assert_eq!(planned_query.scrolls[0].limit, 20);
    assert_eq!(planned_query.scrolls[1].limit, 50);
}
