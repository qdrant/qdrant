use segment::types::{WithPayloadInterface, WithVector};

use super::planned_query::{MergeSources, PlannedQuery, Source};
use crate::operations::types::{CoreSearchRequest, ScrollRequestInternal};

/// Same as a [PlannedQuery], but without the scrolls and searches.
/// The sources in merge_sources have been updated to point to the scrolls and searches in the batch.
#[derive(Debug, PartialEq)]
pub struct WeakPlannedQuery {
    /// References to the searches and scrolls in the batch, and how to merge them.
    /// This retains the recursive structure of the original query.
    pub merge_sources: MergeSources,

    /// The offset into the final results. Skip this many points before returning
    ///
    /// This is not used inside of local shard, as this part acts at collection level, but we keep it here for completeness
    pub offset: usize,

    /// The vector(s) to return
    pub with_vector: WithVector,

    /// The payload to return
    pub with_payload: WithPayloadInterface,
}

#[derive(Debug)]
pub struct PlannedQueryBatch {
    pub searches: Vec<CoreSearchRequest>,
    pub scrolls: Vec<ScrollRequestInternal>,
    pub root_queries: Vec<WeakPlannedQuery>,
}

impl MergeSources {
    /// Offsets the sources of the merge plan by the given offsets.
    fn offset_sources(&mut self, searches_idx_offset: usize, scrolls_idx_offset: usize) {
        self.sources.iter_mut().for_each(|source| match source {
            Source::SearchesIdx(idx) => *idx += searches_idx_offset,
            Source::ScrollsIdx(idx) => *idx += scrolls_idx_offset,
            Source::Prefetch(merge_sources) => {
                merge_sources.offset_sources(searches_idx_offset, scrolls_idx_offset)
            }
        })
    }
}

impl From<Vec<PlannedQuery>> for PlannedQueryBatch {
    fn from(planned_queries: Vec<PlannedQuery>) -> Self {
        let searches_capacity = planned_queries.iter().map(|pq| pq.searches.len()).sum();
        let scrolls_capacity = planned_queries.iter().map(|pq| pq.scrolls.len()).sum();

        debug_assert!(
            searches_capacity > 0 || scrolls_capacity > 0,
            "No searches or scrolls in the planned queries"
        );

        let mut batch = Self {
            searches: Vec::with_capacity(searches_capacity),
            scrolls: Vec::with_capacity(scrolls_capacity),
            root_queries: Vec::with_capacity(planned_queries.len()),
        };

        for planned_query in planned_queries {
            let PlannedQuery {
                mut merge_sources,
                searches,
                scrolls,
                offset,
                with_vector,
                with_payload,
            } = planned_query;

            // Offset the indices of the sources in the merge plan.
            let searches_idx_offset = batch.searches.len();
            let scrolls_idx_offset = batch.scrolls.len();

            merge_sources.offset_sources(searches_idx_offset, scrolls_idx_offset);

            // Extend the searches and scrolls with the ones from the planned query.
            batch.searches.extend(searches);
            batch.scrolls.extend(scrolls);

            batch.root_queries.push(WeakPlannedQuery {
                merge_sources,
                offset,
                with_vector,
                with_payload,
            });
        }

        batch
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::NamedVectorStruct;

    use super::*;
    use crate::operations::query_enum::QueryEnum;
    use crate::operations::universal_query::planned_query::RescoreParams;
    use crate::operations::universal_query::shard_query::{Fusion, ScoringQuery};

    fn dummy_core_search(limit: usize) -> CoreSearchRequest {
        CoreSearchRequest {
            query: QueryEnum::Nearest(NamedVectorStruct::Default(vec![0.1, 0.2, 0.3, 0.4])),
            filter: None,
            params: None,
            limit,
            offset: 0,
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: Some(WithVector::Bool(false)),
            score_threshold: None,
        }
    }

    fn dummy_scroll(limit: usize) -> ScrollRequestInternal {
        ScrollRequestInternal {
            offset: None,
            limit: Some(limit),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(false)),
            with_vector: WithVector::Bool(false),
            order_by: None,
        }
    }

    #[test]
    fn test_from_vec_of_planned_queries() {
        let planned_queries = vec![
            // A no-prefetch core_search query
            PlannedQuery {
                searches: vec![dummy_core_search(10)],
                scrolls: vec![],
                merge_sources: MergeSources {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: None,
                },
                offset: 0,
                with_vector: WithVector::Bool(true),
                with_payload: WithPayloadInterface::Bool(false),
            },
            // A no-prefetch scroll query
            PlannedQuery {
                searches: vec![],
                scrolls: vec![dummy_scroll(20)],
                merge_sources: MergeSources {
                    sources: vec![Source::ScrollsIdx(0)],
                    rescore_params: None,
                },
                offset: 0,
                with_vector: WithVector::Bool(false),
                with_payload: WithPayloadInterface::Bool(true),
            },
            // A double fusion query
            PlannedQuery {
                searches: vec![dummy_core_search(30), dummy_core_search(40)],
                scrolls: vec![dummy_scroll(50)],
                merge_sources: MergeSources {
                    sources: vec![
                        Source::Prefetch(MergeSources {
                            sources: vec![Source::SearchesIdx(0), Source::SearchesIdx(1)],
                            rescore_params: Some(RescoreParams {
                                rescore: ScoringQuery::Fusion(Fusion::Rrf),
                                limit: 10,
                                score_threshold: None,
                            }),
                        }),
                        Source::ScrollsIdx(0),
                    ],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Fusion(Fusion::Rrf),
                        limit: 10,
                        score_threshold: None,
                    }),
                },
                offset: 0,
                with_vector: WithVector::Bool(true),
                with_payload: WithPayloadInterface::Bool(true),
            },
        ];

        let planned_batch_query = PlannedQueryBatch::from(planned_queries);
        assert_eq!(planned_batch_query.searches.len(), 3);
        assert_eq!(planned_batch_query.scrolls.len(), 2);
        assert_eq!(planned_batch_query.root_queries.len(), 3);

        assert_eq!(
            planned_batch_query.root_queries,
            vec![
                WeakPlannedQuery {
                    merge_sources: MergeSources {
                        sources: vec![Source::SearchesIdx(0)],
                        rescore_params: None,
                    },
                    offset: 0,
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(false),
                },
                WeakPlannedQuery {
                    merge_sources: MergeSources {
                        sources: vec![Source::ScrollsIdx(0)],
                        rescore_params: None,
                    },
                    offset: 0,
                    with_vector: WithVector::Bool(false),
                    with_payload: WithPayloadInterface::Bool(true),
                },
                WeakPlannedQuery {
                    merge_sources: MergeSources {
                        sources: vec![
                            Source::Prefetch(MergeSources {
                                sources: vec![Source::SearchesIdx(1), Source::SearchesIdx(2),],
                                rescore_params: Some(RescoreParams {
                                    rescore: ScoringQuery::Fusion(Fusion::Rrf),
                                    limit: 10,
                                    score_threshold: None,
                                }),
                            }),
                            Source::ScrollsIdx(1),
                        ],
                        rescore_params: Some(RescoreParams {
                            rescore: ScoringQuery::Fusion(Fusion::Rrf),
                            limit: 10,
                            score_threshold: None,
                        }),
                    },
                    offset: 0,
                    with_vector: WithVector::Bool(true),
                    with_payload: WithPayloadInterface::Bool(true),
                }
            ]
        );

        assert_eq!(planned_batch_query.searches[0].limit, 10);
        assert_eq!(planned_batch_query.searches[1].limit, 30);
        assert_eq!(planned_batch_query.searches[2].limit, 40);

        assert_eq!(planned_batch_query.scrolls[0].limit, Some(20));
        assert_eq!(planned_batch_query.scrolls[1].limit, Some(50));
    }
}
