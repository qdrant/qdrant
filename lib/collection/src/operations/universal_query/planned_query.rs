//! Types used within `LocalShard` to represent a planned `ShardQueryRequest`

use std::sync::Arc;

use api::rest::OrderByInterface;
use common::types::ScoreType;
use segment::types::{Filter, Order, WithPayloadInterface, WithVector};

use super::shard_query::{ScoringQuery, ShardPrefetch, ShardQueryRequest};
use crate::config::CollectionParams;
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
    ScrollRequestInternal,
};

#[derive(Debug)]
pub struct PlannedQuery {
    pub merge_plan: MergePlan,
    pub searches: Arc<CoreSearchRequestBatch>,
    pub scrolls: Arc<Vec<ScrollRequestInternal>>,
    pub offset: usize,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

/// Defines how to merge multiple [prefetch sources](PrefetchSource)
#[derive(Debug, PartialEq)]
pub struct ResultsMerge {
    /// Alter the scores before selecting the best limit
    pub rescore: Option<ScoringQuery>,

    /// Which order the results from segments should merge in
    pub order: Order,

    /// Use this filter
    pub filter: Option<Filter>,

    /// Keep this many points from the top
    pub limit: usize,

    /// Keep only points with better score than this threshold
    pub score_threshold: Option<ScoreType>,
}

#[derive(Debug, PartialEq)]
pub enum PrefetchSource {
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
    pub sources: Vec<PrefetchSource>,

    /// How to merge the sources
    pub merge: ResultsMerge,
}

// TODO(universal-query): Maybe just return a CoreSearchRequest if there is no prefetch?
impl PlannedQuery {
    pub fn from_shard_request(
        request: ShardQueryRequest,
        collection_params: &CollectionParams,
    ) -> CollectionResult<Self> {
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

        let mut core_searches = Vec::new();
        let mut scrolls = Vec::new();
        let sources;
        let rescore;
        let filter;
        let offset;
        let score_threshold;
        let with_vector;
        let with_payload;
        let order = ScoringQuery::order(query.as_ref(), collection_params)?;

        if !prefetches.is_empty() {
            sources = recurse_prefetches(
                &mut core_searches,
                &mut scrolls,
                prefetches,
                collection_params,
            )?;
            rescore = query;
            filter = req_filter;
            offset = req_offset;
            score_threshold = req_score_threshold;
            with_vector = req_with_vector;
            with_payload = req_with_payload;
        } else {
            match query {
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

                    core_searches.push(core_search);

                    sources = vec![PrefetchSource::SearchesIdx(0)];
                }
                Some(ScoringQuery::Fusion(_)) => {
                    return Err(CollectionError::bad_request(
                        "cannot apply Fusion without prefetches".to_string(),
                    ))
                }
                Some(ScoringQuery::OrderBy(order_by)) => {
                    // Everything should come from 1 scroll
                    let scroll = ScrollRequestInternal {
                        order_by: Some(OrderByInterface::Struct(order_by)),
                        filter: req_filter,
                        with_vector: req_with_vector,
                        with_payload: Some(req_with_payload),
                        limit: Some(limit),
                        offset: None,
                    };

                    scrolls.push(scroll);

                    sources = vec![PrefetchSource::ScrollsIdx(0)];
                }
                None => {
                    // Everything should come from 1 scroll
                    let scroll = ScrollRequestInternal {
                        order_by: None,
                        filter: req_filter,
                        with_vector: req_with_vector,
                        with_payload: Some(req_with_payload),
                        limit: Some(limit),
                        offset: None,
                    };

                    scrolls.push(scroll);

                    sources = vec![PrefetchSource::ScrollsIdx(0)];
                }
            };

            rescore = None;
            filter = None;
            offset = 0;
            score_threshold = None;
            with_vector = WithVector::Bool(false);
            with_payload = WithPayloadInterface::Bool(false);
        }

        Ok(Self {
            merge_plan: MergePlan {
                sources,
                merge: ResultsMerge {
                    rescore,
                    order,
                    limit,
                    filter,
                    score_threshold,
                },
            },
            searches: Arc::new(CoreSearchRequestBatch {
                searches: core_searches,
            }),
            scrolls: Arc::new(scrolls),
            offset,
            with_vector,
            with_payload,
        })
    }
}

fn recurse_prefetches(
    core_searches: &mut Vec<CoreSearchRequest>,
    scrolls: &mut Vec<ScrollRequestInternal>,
    prefetches: Vec<ShardPrefetch>,
    collection_params: &CollectionParams,
) -> CollectionResult<Vec<PrefetchSource>> {
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

        let order = ScoringQuery::order(query.as_ref(), collection_params)?;

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

                    PrefetchSource::SearchesIdx(idx)
                }
                Some(ScoringQuery::Fusion(_)) => {
                    return Err(CollectionError::bad_request(
                        "cannot apply Fusion without prefetches".to_string(),
                    ))
                }
                Some(ScoringQuery::OrderBy(order_by)) => {
                    let scroll = ScrollRequestInternal {
                        order_by: Some(OrderByInterface::Struct(order_by)),
                        filter,
                        with_vector: WithVector::Bool(false),
                        with_payload: Some(WithPayloadInterface::Bool(false)),
                        limit: Some(limit),
                        offset: None,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    PrefetchSource::ScrollsIdx(idx)
                }
                None => {
                    let scroll = ScrollRequestInternal {
                        order_by: None,
                        filter,
                        with_vector: WithVector::Bool(false),
                        with_payload: Some(WithPayloadInterface::Bool(false)),
                        limit: Some(limit),
                        offset: None,
                    };

                    let idx = scrolls.len();
                    scrolls.push(scroll);

                    PrefetchSource::ScrollsIdx(idx)
                }
            }
        } else {
            // This is a nested prefetch. Recurse into it
            let inner_sources =
                recurse_prefetches(core_searches, scrolls, prefetches, collection_params)?;

            let prefetch_plan = MergePlan {
                sources: inner_sources,
                merge: ResultsMerge {
                    rescore: query,
                    order,
                    filter,
                    limit,
                    score_threshold,
                },
            };
            PrefetchSource::Prefetch(prefetch_plan)
        };
        sources.push(source);
    }

    Ok(sources)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::{NonZeroU32, NonZeroU64};

    use segment::data_types::vectors::{MultiDenseVector, NamedVectorStruct, Vector};
    use segment::types::{
        Condition, Distance, FieldCondition, Filter, Match, SearchParams, WithPayloadInterface,
        WithVector,
    };
    use sparse::common::sparse_vector::SparseVector;

    use super::*;
    use crate::operations::query_enum::QueryEnum;
    use crate::operations::types::{VectorParams, VectorsConfig};
    use crate::operations::universal_query::shard_query::Fusion;

    fn collection_params() -> CollectionParams {
        let cosine_params = VectorParams {
            size: NonZeroU64::new(4u64).unwrap(),
            distance: Distance::Cosine,
            hnsw_config: None,
            quantization_config: None,
            on_disk: None,
            datatype: None,
            multivec_config: None,
        };
        let vectors = BTreeMap::from([
            ("dense".to_string(), cosine_params.clone()),
            ("byte".to_string(), cosine_params.clone()),
            ("full".to_string(), cosine_params.clone()),
            ("multi".to_string(), cosine_params.clone()),
            // this is incorrect, but we don't care for this test
            ("sparse".to_string(), cosine_params.clone()),
        ]);

        CollectionParams {
            vectors: VectorsConfig::Multi(vectors),
            shard_number: NonZeroU32::new(1).unwrap(),
            sharding_method: None,
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            read_fan_out_factor: None,
            on_disk_payload: false,
            sparse_vectors: None,
        }
    }

    #[test]
    fn test_try_from_double_rescore() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
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
                    filter: None,
                    score_threshold: None,
                }],
                query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(Vector::Dense(dummy_vector.clone()), "full"),
                ))),
                limit: 100,
                params: None,
                filter: None,
                score_threshold: None,
            }],
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                NamedVectorStruct::new_from_vector(
                    Vector::MultiDense(MultiDenseVector::new_unchecked(vec![dummy_vector.clone()])),
                    "multi",
                ),
            ))),
            filter: Some(Filter::default()),
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query =
            PlannedQuery::from_shard_request(request, &collection_params()).unwrap();

        assert_eq!(
            planned_query.searches.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    Vector::Dense(dummy_vector.clone()),
                    "byte",
                )),
                filter: None,
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
                sources: vec![PrefetchSource::Prefetch(MergePlan {
                    sources: vec![PrefetchSource::SearchesIdx(0)],
                    merge: ResultsMerge {
                        rescore: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedVectorStruct::new_from_vector(
                                Vector::Dense(dummy_vector.clone()),
                                "full",
                            )
                        ))),
                        order: Order::LargeBetter,
                        filter: None,
                        limit: 100,
                        score_threshold: None
                    }
                })],
                merge: ResultsMerge {
                    rescore: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            Vector::MultiDense(MultiDenseVector::new_unchecked(vec![
                                dummy_vector.clone()
                            ])),
                            "multi"
                        )
                    ))),
                    order: Order::LargeBetter,
                    filter: Some(Filter::default()),
                    limit: 10,
                    score_threshold: None,
                }
            }
        );

        assert_eq!(planned_query.offset, 0);
        assert_eq!(planned_query.with_vector, WithVector::Bool(true));
        assert_eq!(planned_query.with_payload, WithPayloadInterface::Bool(true));
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

        let planned_query =
            PlannedQuery::from_shard_request(request, &collection_params()).unwrap();

        assert_eq!(
            planned_query.searches.searches,
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
                sources: vec![PrefetchSource::SearchesIdx(0)],
                merge: ResultsMerge {
                    rescore: None,
                    order: Order::LargeBetter,
                    filter: None,
                    limit: 10,
                    score_threshold: None,
                }
            }
        );

        assert_eq!(planned_query.offset, 0);
        assert_eq!(planned_query.with_vector, WithVector::Bool(false));
        assert_eq!(
            planned_query.with_payload,
            WithPayloadInterface::Bool(false)
        );
    }

    #[test]
    fn test_try_from_hybrid_query() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let dummy_sparse = SparseVector::new(vec![100, 123, 2000], vec![0.2, 0.3, 0.4]).unwrap();
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
                    filter: None,
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
                    filter: None,
                    score_threshold: None,
                },
            ],
            query: Some(ScoringQuery::Fusion(Fusion::Rrf)),
            filter: Some(Filter::default()),
            score_threshold: None,
            limit: 50,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(false),
        };

        let planned_query =
            PlannedQuery::from_shard_request(request, &collection_params()).unwrap();

        assert_eq!(
            planned_query.searches.searches,
            vec![
                CoreSearchRequest {
                    query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                        Vector::Dense(dummy_vector.clone()),
                        "dense",
                    )),
                    filter: None,
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
                    filter: None,
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
                sources: vec![
                    PrefetchSource::SearchesIdx(0),
                    PrefetchSource::SearchesIdx(1)
                ],
                merge: ResultsMerge {
                    rescore: Some(ScoringQuery::Fusion(Fusion::Rrf)),
                    order: Order::LargeBetter,
                    filter: Some(Filter::default()),
                    limit: 50,
                    score_threshold: None
                }
            }
        );

        assert_eq!(planned_query.offset, 0);
        assert_eq!(planned_query.with_vector, WithVector::Bool(true));
        assert_eq!(
            planned_query.with_payload,
            WithPayloadInterface::Bool(false)
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

        let planned_query = PlannedQuery::from_shard_request(request, &collection_params());

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

        let planned_query =
            PlannedQuery::from_shard_request(request, &collection_params()).unwrap();

        assert_eq!(planned_query.offset, 49);
        assert_eq!(
            planned_query.with_payload,
            WithPayloadInterface::Bool(false)
        );
        assert_eq!(planned_query.with_vector, WithVector::Bool(true));

        assert_eq!(
            planned_query.merge_plan,
            MergePlan {
                sources: vec![PrefetchSource::SearchesIdx(0)],
                merge: ResultsMerge {
                    rescore: Some(ScoringQuery::Fusion(Fusion::Rrf)),
                    order: Order::LargeBetter,
                    filter: Some(Filter::default()),
                    limit: 50,
                    score_threshold: Some(0.666)
                }
            }
        );

        assert_eq!(
            planned_query.searches.searches,
            vec![CoreSearchRequest {
                query: QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                    Vector::Dense(dummy_vector.clone()),
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
}
