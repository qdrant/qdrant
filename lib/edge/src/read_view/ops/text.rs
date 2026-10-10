use common::ambient;
use common::reason::Reason;
use segment::common::operation_error::{OperationError, OperationResult};
use segment::data_types::index::TextIndexParams;
use segment::entry::ReadSegmentEntry;
use segment::json_path::JsonPath;
use segment::types::{PayloadFieldSchema, PayloadSchemaParams, ScoredPoint, WithPayload};
use shard::query::text::{
    TextScoringQuery, TextSearchRequestInternal, cut_at_threshold, init_text_query_context,
};
use shard::search_result_aggregator::BatchResultAggregator;

use super::search::fill_query_context_over;
use crate::read_view::{EdgeReadView, ReadSegmentHandle};

impl<H: ReadSegmentHandle> EdgeReadView<H> {
    /// Run the BM25 leaves of a planned query, one result list per leaf, each
    /// cut at its score threshold.
    pub(crate) fn text_search_batch(
        &self,
        texts: &[TextSearchRequestInternal],
    ) -> OperationResult<Vec<Vec<ScoredPoint>>> {
        let _scope = ambient::unmeasured_guard(Reason::EDGE_UNMEASURED);
        texts.iter().map(|text| self.text_search(text)).collect()
    }

    /// Rank the points by BM25 over the text index of the leaf's field and
    /// return the `limit` best, highest first.
    fn text_search(&self, text: &TextSearchRequestInternal) -> OperationResult<Vec<ScoredPoint>> {
        self.check_stopped()?;
        let TextSearchRequestInternal {
            query,
            filter,
            idf_corpus,
            limit,
            score_threshold,
            with_vector,
            with_payload,
        } = text;

        let terms = self.text_query_terms(query)?;
        let query_context = init_text_query_context(
            &query.field,
            idf_corpus.as_ref(),
            &terms,
            self.is_stopped.clone(),
        );
        let Some(context) =
            fill_query_context_over(query_context, &self.segments, &self.is_stopped)?
        else {
            // No segments to score
            return Ok(Vec::new());
        };

        let with_payload = WithPayload::from(with_payload);
        let points_by_segment = self.par_map_segments(|segment| {
            let segment_query_context = context.get_segment_query_context();
            segment.read_segment().score_bm25(
                &query.field,
                &terms,
                query.params,
                &with_payload,
                with_vector,
                filter.as_ref(),
                *limit,
                &segment_query_context,
            )
        })?;

        let mut aggregator = BatchResultAggregator::new(std::iter::once(*limit));
        aggregator.update_point_versions(points_by_segment.iter().flatten());
        aggregator.update_batch_results(0, points_by_segment.into_iter().flatten());
        let mut points =
            aggregator.into_topk().into_iter().next().ok_or_else(|| {
                OperationError::service_error("expected first result of aggregator")
            })?;

        cut_at_threshold(&mut points, *score_threshold);
        Ok(points)
    }

    /// The query terms, after the checks the server runs on a text query
    /// before any shard does: the field must have a text index that scores,
    /// and the parameters must be in range. The scorer checks the parameters
    /// too, but only in a segment holding a query term, so without this the
    /// same query would fail on some data and return nothing on the rest.
    fn text_query_terms(&self, query: &TextScoringQuery) -> OperationResult<Vec<String>> {
        let field_schema = self.indexed_field_schema(&query.field);
        if let Some(field_schema) = &field_schema
            && let PayloadSchemaParams::Text(TextIndexParams { scoring: None, .. }) =
                field_schema.expand().as_ref()
        {
            return Err(OperationError::validation_error(format!(
                "The text index on `{}` does not score: set `scoring` on it to query it",
                query.field,
            )));
        }
        query.params.validate()?;
        query.tokenize_field(field_schema.as_ref())
    }

    /// The index schema of `field`, as the first segment indexing it holds it.
    /// Every segment of an edge shard gets the same field indexes.
    fn indexed_field_schema(&self, field: &JsonPath) -> Option<PayloadFieldSchema> {
        self.segments
            .iter()
            .find_map(|segment| segment.read_segment().get_indexed_fields().remove(field))
    }
}

#[cfg(test)]
mod tests {
    use common::types::ScoreType;
    use segment::data_types::index::TextScoringParams;
    use segment::data_types::query_context::fancy_idf;
    use segment::data_types::vectors::{NamedQuery, VectorInternal};
    use segment::index::field_index::full_text_index::Bm25Params;
    use segment::types::{ExtendedPointId, Payload, PayloadSchemaParams, PayloadSchemaType};
    use shard::operations::CollectionUpdateOperations::FieldIndexOperation;
    use shard::operations::point_ops::PointStructPersisted;
    use shard::operations::{CreateIndex, FieldIndexOperations};
    use shard::query::query_enum::QueryEnum;
    use shard::query::{FusionInternal, ScoringQuery};

    use super::*;
    use crate::test_helpers::{VECTOR_NAME, point, test_config, upsert};
    use crate::{EdgeShard, Prefetch, QueryRequest};

    const FIELD: &str = "body";
    const POINTS: u64 = 35;

    /// One document per pair of `alpha` (1 to 5) and `gamma` (0 to 6) counts,
    /// so no two documents score the same for a query of those terms.
    fn text_of(i: u64) -> String {
        ["alpha"; 5][..(i % 5 + 1) as usize].join(" ") + &" gamma".repeat((i / 5) as usize)
    }

    fn create_index(shard: &EdgeShard, field_schema: PayloadFieldSchema) {
        shard
            .update(FieldIndexOperation(FieldIndexOperations::CreateIndex(
                CreateIndex {
                    field_name: FIELD.parse().unwrap(),
                    field_schema: Some(field_schema),
                },
            )))
            .unwrap();
    }

    fn scored_text_index() -> PayloadFieldSchema {
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(TextIndexParams {
            scoring: Some(TextScoringParams::default()),
            ..TextIndexParams::default()
        }))
    }

    fn point_with_text(i: u64) -> PointStructPersisted {
        let payload = serde_json::json!({ FIELD: text_of(i) });
        PointStructPersisted {
            payload: Some(Payload::from(payload.as_object().unwrap().clone())),
            ..point(i)
        }
    }

    fn text_shard(dir: &std::path::Path, field_schema: PayloadFieldSchema) -> EdgeShard {
        let shard = EdgeShard::new(dir, test_config()).unwrap();
        create_index(&shard, field_schema);
        upsert(&shard, (0..POINTS).map(point_with_text).collect());
        shard
    }

    fn text_query(text: &str, params: Bm25Params) -> ScoringQuery {
        ScoringQuery::Text(TextScoringQuery {
            field: FIELD.parse().unwrap(),
            text: text.to_owned(),
            params,
        })
    }

    fn query_request(query: ScoringQuery, limit: usize) -> QueryRequest {
        QueryRequest {
            query: Some(query),
            ..QueryRequest::new(limit)
        }
    }

    fn id_of(point: &ScoredPoint) -> u64 {
        match point.id {
            ExtendedPointId::NumId(id) => id,
            ExtendedPointId::Uuid(_) => panic!("the fixture uses numeric ids"),
        }
    }

    /// Every score equals BM25 by definition over the whole shard, with the
    /// lengths the scoring index records.
    #[test]
    fn scores_bm25_by_definition() {
        let dir = tempfile::tempdir().unwrap();
        let shard = text_shard(dir.path(), scored_text_index());

        let points = shard
            .query(query_request(
                text_query("alpha gamma", Bm25Params::default()),
                POINTS as usize,
            ))
            .unwrap();

        let Bm25Params { k1, b } = Bm25Params::default();
        let counts = |i: u64| {
            let text = text_of(i);
            let count = |term| text.split(' ').filter(|t| *t == term).count() as ScoreType;
            (count("alpha"), count("gamma"))
        };
        let n = POINTS as ScoreType;
        let avgdl = (0..POINTS)
            .map(|i| {
                let (alpha, gamma) = counts(i);
                alpha + gamma
            })
            .sum::<ScoreType>()
            / n;
        let df_gamma = (0..POINTS).filter(|&i| counts(i).1 > 0.0).count() as ScoreType;
        let expected = |i: u64| {
            let (alpha, gamma) = counts(i);
            let norm = k1 * (1.0 - b + b * (alpha + gamma) / avgdl);
            let term = |tf: ScoreType, df: ScoreType| {
                if tf == 0.0 {
                    0.0
                } else {
                    fancy_idf(n, df).max(0.0) * tf * (k1 + 1.0) / (tf + norm)
                }
            };
            term(alpha, n) + term(gamma, df_gamma)
        };

        assert_eq!(points.len(), POINTS as usize);
        assert!(points.is_sorted_by(|a, b| a.score >= b.score));
        for point in &points {
            let reference = expected(id_of(point));
            assert!(
                (point.score - reference).abs() <= 1e-4 * reference.max(1.0),
                "point {}: engine {}, reference {reference}",
                id_of(point),
                point.score,
            );
        }
    }

    /// The limit, offset and score threshold cut the ranking the way they cut
    /// a vector search's.
    #[test]
    fn limit_offset_and_threshold_cut_the_ranking() {
        let dir = tempfile::tempdir().unwrap();
        let shard = text_shard(dir.path(), scored_text_index());
        let query = || text_query("alpha gamma", Bm25Params::default());

        let all = shard
            .query(query_request(query(), POINTS as usize))
            .unwrap();
        assert_eq!(all.len(), POINTS as usize);

        let page = shard
            .query(QueryRequest {
                offset: 3,
                ..query_request(query(), 4)
            })
            .unwrap();
        assert_eq!(page, all[3..7]);

        let threshold = all[9].score;
        let above = shard
            .query(QueryRequest {
                score_threshold: Some(threshold),
                ..query_request(query(), POINTS as usize)
            })
            .unwrap();
        assert!(!above.is_empty());
        assert!(above.iter().all(|point| point.score > threshold));
        assert_eq!(above, all[..above.len()]);
    }

    /// A text prefetch fuses with a dense one.
    #[test]
    fn fuses_with_a_dense_prefetch() {
        let dir = tempfile::tempdir().unwrap();
        let shard = text_shard(dir.path(), scored_text_index());

        let dense = Prefetch {
            query: Some(ScoringQuery::Vector(QueryEnum::Nearest(NamedQuery::new(
                VectorInternal::from(vec![1.0]),
                VECTOR_NAME,
            )))),
            ..Prefetch::new(5)
        };
        let text = Prefetch {
            query: Some(text_query("alpha gamma", Bm25Params::default())),
            ..Prefetch::new(5)
        };
        let top_of = |prefetch: &Prefetch| {
            shard
                .query(query_request(
                    prefetch.query.clone().unwrap(),
                    prefetch.limit,
                ))
                .unwrap()
        };
        let mut expected: Vec<_> = top_of(&dense)
            .iter()
            .chain(&top_of(&text))
            .map(id_of)
            .collect();
        expected.sort_unstable();
        expected.dedup();
        // Each prefetch brings points the other does not rank.
        assert!(expected.len() > 5, "{expected:?}");

        let fused = shard
            .query(QueryRequest {
                prefetches: vec![dense, text],
                query: Some(ScoringQuery::Fusion(FusionInternal::Rrf {
                    k: 2,
                    weights: None,
                })),
                ..QueryRequest::new(10)
            })
            .unwrap();

        let mut ids: Vec<_> = fused.iter().map(id_of).collect();
        ids.sort_unstable();
        assert_eq!(ids, expected);
    }

    #[test]
    fn refuses_a_field_that_does_not_score() {
        let dir = tempfile::tempdir().unwrap();
        let text_without_scoring = text_shard(
            dir.path(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Text),
        );
        let err = text_without_scoring
            .query(query_request(
                text_query("alpha", Bm25Params::default()),
                10,
            ))
            .unwrap_err();
        assert!(err.to_string().contains("does not score"), "{err}");

        let dir = tempfile::tempdir().unwrap();
        let keyword = text_shard(
            dir.path(),
            PayloadFieldSchema::FieldType(PayloadSchemaType::Keyword),
        );
        let err = keyword
            .query(query_request(
                text_query("alpha", Bm25Params::default()),
                10,
            ))
            .unwrap_err();
        assert!(err.to_string().contains("indexed as"), "{err}");

        let dir = tempfile::tempdir().unwrap();
        let unindexed = EdgeShard::new(dir.path(), test_config()).unwrap();
        upsert(&unindexed, (0..POINTS).map(point_with_text).collect());
        let err = unindexed
            .query(query_request(
                text_query("alpha", Bm25Params::default()),
                10,
            ))
            .unwrap_err();
        assert!(err.to_string().contains("which has none"), "{err}");
    }

    /// Parameters out of range fail even for a query no segment holds a term
    /// of, where the scorer would never see them.
    #[test]
    fn refuses_parameters_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let shard = text_shard(dir.path(), scored_text_index());
        for params in [
            Bm25Params { k1: -1.0, b: 0.75 },
            Bm25Params { k1: 1.2, b: 1.5 },
            Bm25Params {
                k1: f32::NAN,
                b: 0.75,
            },
        ] {
            let err = shard
                .query(query_request(text_query("absent", params), 10))
                .unwrap_err();
            assert!(err.to_string().contains("BM25"), "{params:?}: {err}");
        }
    }

    /// BM25 scores what it finds in the index: it cannot rescore prefetched
    /// points yet.
    #[test]
    fn refuses_to_rescore_prefetches() {
        let dir = tempfile::tempdir().unwrap();
        let shard = text_shard(dir.path(), scored_text_index());
        let err = shard
            .query(QueryRequest {
                prefetches: vec![Prefetch::new(10)],
                ..query_request(text_query("alpha", Bm25Params::default()), 10)
            })
            .unwrap_err();
        assert!(err.to_string().contains("rescore"), "{err}");
    }
}
