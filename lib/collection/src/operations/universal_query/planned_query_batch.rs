use super::planned_query::{MergePlan, PlannedQuery, Source};
use crate::operations::types::{CoreSearchRequest, QueryScrollRequestInternal};

#[derive(Debug)]
pub struct PlannedQueryBatch {
    pub searches: Vec<CoreSearchRequest>,
    pub scrolls: Vec<QueryScrollRequestInternal>,

    /// One per query in the batch.
    pub root_plans: Vec<MergePlan>,
}

impl MergePlan {
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
            root_plans: Vec::with_capacity(planned_queries.len()),
        };

        for planned_query in planned_queries {
            let PlannedQuery {
                mut merge_plan,
                searches,
                scrolls,
            } = planned_query;

            // Offset the indices of the sources in the merge plan.
            let searches_idx_offset = batch.searches.len();
            let scrolls_idx_offset = batch.scrolls.len();

            merge_plan.offset_sources(searches_idx_offset, scrolls_idx_offset);

            // Extend the searches and scrolls with the ones from the planned query.
            batch.searches.extend(searches);
            batch.scrolls.extend(scrolls);

            batch.root_plans.push(merge_plan);
        }

        batch
    }
}

#[cfg(test)]
mod tests {
    use segment::data_types::vectors::NamedVectorStruct;
    use segment::types::{WithPayloadInterface, WithVector};

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

    fn dummy_scroll(limit: usize) -> QueryScrollRequestInternal {
        QueryScrollRequestInternal {
            offset: 0,
            limit,
            filter: None,
            with_payload: WithPayloadInterface::Bool(false),
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
                merge_plan: MergePlan {
                    sources: vec![Source::SearchesIdx(0)],
                    rescore_params: None,
                },
            },
            // A no-prefetch scroll query
            PlannedQuery {
                searches: vec![],
                scrolls: vec![dummy_scroll(20)],
                merge_plan: MergePlan {
                    sources: vec![Source::ScrollsIdx(0)],
                    rescore_params: None,
                },
            },
            // A double fusion query
            PlannedQuery {
                searches: vec![dummy_core_search(30), dummy_core_search(40)],
                scrolls: vec![dummy_scroll(50)],
                merge_plan: MergePlan {
                    sources: vec![
                        Source::Prefetch(MergePlan {
                            sources: vec![Source::SearchesIdx(0), Source::SearchesIdx(1)],
                            rescore_params: Some(RescoreParams {
                                rescore: ScoringQuery::Fusion(Fusion::Rrf),
                                limit: 10,
                                offset: 0,
                                score_threshold: None,
                                with_vector: WithVector::Bool(true),
                                with_payload: WithPayloadInterface::Bool(true),
                            }),
                        }),
                        Source::ScrollsIdx(0),
                    ],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Fusion(Fusion::Rrf),
                        limit: 10,
                        offset: 0,
                        score_threshold: None,
                        with_vector: WithVector::Bool(true),
                        with_payload: WithPayloadInterface::Bool(true),
                    }),
                },
            },
        ];

        let planned_batch_query = PlannedQueryBatch::from(planned_queries);
        assert_eq!(planned_batch_query.searches.len(), 3);
        assert_eq!(planned_batch_query.scrolls.len(), 2);
        assert_eq!(planned_batch_query.root_plans.len(), 3);

        assert_eq!(
            planned_batch_query.root_plans,
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
                        Source::Prefetch(MergePlan {
                            sources: vec![Source::SearchesIdx(1), Source::SearchesIdx(2),],
                            rescore_params: Some(RescoreParams {
                                rescore: ScoringQuery::Fusion(Fusion::Rrf),
                                limit: 10,
                                offset: 0,
                                score_threshold: None,
                                with_vector: WithVector::Bool(true),
                                with_payload: WithPayloadInterface::Bool(true),
                            }),
                        }),
                        Source::ScrollsIdx(1),
                    ],
                    rescore_params: Some(RescoreParams {
                        rescore: ScoringQuery::Fusion(Fusion::Rrf),
                        limit: 10,
                        offset: 0,
                        score_threshold: None,
                        with_vector: WithVector::Bool(true),
                        with_payload: WithPayloadInterface::Bool(true),
                    }),
                },
            ]
        );

        assert_eq!(planned_batch_query.searches[0].limit, 10);
        assert_eq!(planned_batch_query.searches[1].limit, 30);
        assert_eq!(planned_batch_query.searches[2].limit, 40);

        assert_eq!(planned_batch_query.scrolls[0].limit, 20);
        assert_eq!(planned_batch_query.scrolls[1].limit, 50);
    }
}
