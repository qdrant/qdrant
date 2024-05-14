//! Types used within `LocalShard` to represent a planned `ShardQueryRequest`

use std::sync::Arc;

use common::types::ScoreType;
use segment::types::{Filter, WithPayloadInterface, WithVector};

use super::shard_query::{ScoringQuery, ShardPrefetch, ShardQueryRequest};
use crate::operations::types::{
    CollectionError, CollectionResult, CoreSearchRequest, CoreSearchRequestBatch,
};

pub struct PlannedQuery {
    pub merge_plan: PrefetchPlan,
    pub batch: Arc<CoreSearchRequestBatch>,
    pub offset: usize,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}

/// Defines how to merge multiple [prefetch sources](PrefetchSource)
#[derive(Debug, PartialEq)]
pub struct PrefetchMerge {
    /// Alter the scores before selecting the best limit
    pub rescore: Option<ScoringQuery>,

    /// Use this filter
    pub filter: Option<Filter>,

    /// Keep this much points from the top
    pub limit: usize,

    /// Keep only points with better score than this threshold
    pub score_threshold: Option<ScoreType>,
}

#[derive(Debug, PartialEq)]
pub enum PrefetchSource {
    /// A reference offset into the main search batch
    BatchIdx(usize),

    /// A nested prefetch
    Prefetch(PrefetchPlan),
}

#[derive(Debug, PartialEq)]
pub struct PrefetchPlan {
    /// Gather all these sources
    pub sources: Vec<PrefetchSource>,

    /// How to merge the sources
    pub merge: PrefetchMerge,
}

// TODO(universal-query): Maybe just return a CoreSearchRequest if there is no prefetch?
impl TryFrom<ShardQueryRequest> for PlannedQuery {
    type Error = CollectionError;

    fn try_from(request: ShardQueryRequest) -> CollectionResult<Self> {
        let ShardQueryRequest {
            query,
            filter,
            score_threshold,
            limit,
            offset: req_offset,
            with_vector: req_with_vector,
            with_payload: req_with_payload,
            prefetches: prefetch,
            params,
        } = request;

        let mut core_searches = Vec::new();
        let sources;
        let rescore;
        let offset;
        let with_vector;
        let with_payload;

        if !prefetch.is_empty() {
            sources = recurse_prefetches(&mut core_searches, prefetch);
            rescore = Some(query);
            offset = req_offset;
            with_vector = req_with_vector;
            with_payload = req_with_payload;
        } else {
            // Everything should come from 1 core search
            #[allow(clippy::infallible_destructuring_match)]
            // TODO: remove once there are more variants
            let query = match query {
                ScoringQuery::Vector(query) => query,
                // TODO: return error for fusion queries without prefetch
            };
            let core_search = CoreSearchRequest {
                query,
                filter,
                score_threshold,
                with_vector: Some(req_with_vector),
                with_payload: Some(req_with_payload),
                offset: req_offset,
                params,
                limit,
            };
            core_searches.push(core_search);

            sources = vec![PrefetchSource::BatchIdx(0)];
            rescore = None;
            offset = 0;
            with_vector = WithVector::Bool(false);
            with_payload = WithPayloadInterface::Bool(false);
        }

        Ok(Self {
            merge_plan: PrefetchPlan {
                sources,
                merge: PrefetchMerge {
                    rescore,
                    limit,
                    filter: None,
                    score_threshold: None,
                },
            },
            batch: Arc::new(CoreSearchRequestBatch {
                searches: core_searches,
            }),
            offset,
            with_vector,
            with_payload,
        })
    }
}

fn recurse_prefetches(
    core_searches: &mut Vec<CoreSearchRequest>,
    prefetches: Vec<ShardPrefetch>,
) -> Vec<PrefetchSource> {
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

        let source = if prefetches.is_empty() {
            match query {
                ScoringQuery::Vector(query_enum) => {
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

                    PrefetchSource::BatchIdx(idx)
                }
            }
        } else {
            let sources = recurse_prefetches(core_searches, prefetches);

            let prefetch_plan = PrefetchPlan {
                sources,
                merge: PrefetchMerge {
                    rescore: Some(query),
                    filter,
                    limit,
                    score_threshold,
                },
            };
            PrefetchSource::Prefetch(prefetch_plan)
        };
        sources.push(source);
    }

    sources
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::{MultiDenseVector, NamedVectorStruct, Vector};
    use segment::types::{Filter, SearchParams, WithPayloadInterface, WithVector};

    use super::*;
    use crate::operations::query_enum::QueryEnum;

    #[test]
    fn test_try_from_double_rescore() {
        let dummy_vector = vec![1.0, 2.0, 3.0];
        let request = ShardQueryRequest {
            prefetches: vec![ShardPrefetch {
                prefetches: vec![ShardPrefetch {
                    prefetches: Default::default(),
                    query: ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            Vector::Dense(dummy_vector.clone()),
                            "byte",
                        ),
                    )),
                    limit: 1000,
                    params: None,
                    filter: None,
                    score_threshold: None,
                }],
                query: ScoringQuery::Vector(QueryEnum::Nearest(
                    NamedVectorStruct::new_from_vector(Vector::Dense(dummy_vector.clone()), "full"),
                )),
                limit: 100,
                params: None,
                filter: None,
                score_threshold: None,
            }],
            query: ScoringQuery::Vector(QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                Vector::MultiDense(MultiDenseVector::new_unchecked(vec![dummy_vector.clone()])),
                "multi",
            ))),
            filter: Some(Filter::default()),
            score_threshold: None,
            limit: 10,
            offset: 0,
            params: None,
            with_vector: WithVector::Bool(true),
            with_payload: WithPayloadInterface::Bool(true),
        };

        let planned_query = PlannedQuery::try_from(request).unwrap();

        assert_eq!(
            planned_query.batch.searches,
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
            PrefetchPlan {
                sources: vec![PrefetchSource::Prefetch(PrefetchPlan {
                    sources: vec![PrefetchSource::BatchIdx(0)],
                    merge: PrefetchMerge {
                        rescore: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                            NamedVectorStruct::new_from_vector(
                                Vector::Dense(dummy_vector.clone()),
                                "full",
                            )
                        ))),
                        filter: None,
                        limit: 100,
                        score_threshold: None
                    }
                })],
                merge: PrefetchMerge {
                    rescore: Some(ScoringQuery::Vector(QueryEnum::Nearest(
                        NamedVectorStruct::new_from_vector(
                            Vector::MultiDense(MultiDenseVector::new_unchecked(vec![
                                dummy_vector.clone()
                            ])),
                            "multi"
                        )
                    ))),
                    filter: None,
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
            query: ScoringQuery::Vector(QueryEnum::Nearest(NamedVectorStruct::new_from_vector(
                Vector::Dense(dummy_vector.clone()),
                "full",
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
            planned_query.batch.searches,
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
            PrefetchPlan {
                sources: vec![PrefetchSource::BatchIdx(0)],
                merge: PrefetchMerge {
                    rescore: None,
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
}
