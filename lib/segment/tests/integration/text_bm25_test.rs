//! BM25 over the text payload index against a reference computed from the raw
//! documents. This is the check the rest of the stack gets validated by: the
//! stored lengths, the gathered corpus statistics, the resolved term ids and
//! the term frequencies all have to agree with a definition that knows nothing
//! about postings, sidecars or segments.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use ahash::AHashSet;
use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::TextIndexParams;
use segment::data_types::named_vectors::NamedVectors;
use segment::data_types::query_context::{
    QueryContext, SegmentQueryContext, TextQueryContext, fancy_idf,
};
use segment::entry::entry_point::SegmentEntry;
use segment::entry::{NonAppendableSegmentEntry, ReadSegmentEntry};
use segment::index::field_index::FieldIndex;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::index::field_index::full_text_index::full_text_index_read::score_bm25;
use segment::index::field_index::full_text_index::tokenizers::Tokenizer;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::segment::Segment;
use segment::segment_constructor::build_segment;
use segment::types::{
    Condition, ExtendedPointId, Filter, HasIdCondition, PayloadFieldSchema, PayloadSchemaParams,
    PointIdType, ScoredPoint, SegmentConfig, SeqNumberType, WithPayload, WithVector,
};
use tempfile::Builder;

const VOCAB: usize = 60;
const QUERY_COUNT: usize = 25;
const LIMIT: usize = 10;

fn field() -> JsonPath {
    JsonPath::new("text")
}

/// Scoring needs positions, which `phrase_matching` turns on.
fn text_params() -> TextIndexParams {
    TextIndexParams {
        phrase_matching: Some(true),
        lowercase: Some(true),
        ..TextIndexParams::default()
    }
}

/// A skewed vocabulary, so that queries mix common and rare terms.
fn word(rng: &mut StdRng) -> String {
    let rank = (rng.random::<f64>().powi(3) * VOCAB as f64) as usize;
    format!("term{}", rank.min(VOCAB - 1))
}

fn document(rng: &mut StdRng) -> String {
    let len = rng.random_range(3..=50);
    (0..len).map(|_| word(rng)).collect::<Vec<_>>().join(" ")
}

/// A segment with a text index over `documents`, point offset `i` holding
/// `documents[i]`, then with `deleted` removed.
fn build_text_segment(path: &std::path::Path, documents: &[String], deleted: &[u64]) -> Segment {
    build_text_segment_deferred(path, documents, deleted, None)
}

/// [`build_text_segment`] with every offset from `deferred_internal_id` on
/// deferred.
fn build_text_segment_deferred(
    path: &std::path::Path,
    documents: &[String],
    deleted: &[u64],
    deferred_internal_id: Option<PointOffsetType>,
) -> Segment {
    let config = SegmentConfig {
        vector_data: Default::default(),
        sparse_vector_data: HashMap::new(),
        payload_storage_type: Default::default(),
        id_tracker_memory: None,
    };
    let (mut segment, _) = build_segment(path, &config, deferred_internal_id, true).unwrap();
    let hw_counter = HardwareCounterCell::new();
    let mut op_num: SeqNumberType = 0;
    segment
        .create_field_index(
            op_num,
            &field(),
            Some(&PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(
                text_params(),
            ))),
            &hw_counter,
        )
        .unwrap();
    for (point_id, document) in documents.iter().enumerate() {
        op_num += 1;
        segment
            .upsert_point(
                op_num,
                PointIdType::from(point_id as u64),
                NamedVectors::default(),
                &hw_counter,
            )
            .unwrap();
        segment
            .set_payload(
                op_num,
                PointIdType::from(point_id as u64),
                &payload_json! { "text": document.as_str() },
                &None,
                &hw_counter,
            )
            .unwrap();
    }
    for &point_id in deleted {
        op_num += 1;
        segment
            .delete_point(op_num, PointIdType::from(point_id), &hw_counter)
            .unwrap();
    }
    segment
}

/// The live documents of one segment as token lists, by point offset.
struct TokenizedSegment {
    documents: Vec<Option<Vec<String>>>,
}

impl TokenizedSegment {
    fn new(tokenizer: &Tokenizer, documents: &[String], deleted: &[u64]) -> Self {
        let documents = documents
            .iter()
            .enumerate()
            .map(|(idx, document)| {
                if deleted.contains(&(idx as u64)) {
                    return None;
                }
                let mut tokens = Vec::new();
                tokenizer.tokenize_doc(document, |token| tokens.push(token.into_owned()));
                Some(tokens)
            })
            .collect();
        Self { documents }
    }

    fn live(&self) -> impl Iterator<Item = (PointOffsetType, &[String])> {
        self.documents
            .iter()
            .enumerate()
            .filter_map(|(idx, tokens)| Some((idx as PointOffsetType, tokens.as_deref()?)))
    }
}

/// BM25 by definition over the whole corpus: `N`, `df` and `avgdl` over every
/// live document of every segment.
struct Reference<'a> {
    segments: &'a [TokenizedSegment],
    params: Bm25Params,
}

impl Reference<'_> {
    fn document_count(&self) -> usize {
        self.segments.iter().map(|s| s.live().count()).sum()
    }

    fn avg_doc_len(&self) -> ScoreType {
        let total: usize = self
            .segments
            .iter()
            .flat_map(|s| s.live().map(|(_, tokens)| tokens.len()))
            .sum();
        total as ScoreType / self.document_count() as ScoreType
    }

    fn df(&self, term: &str) -> usize {
        self.segments
            .iter()
            .flat_map(|s| s.live())
            .filter(|(_, tokens)| tokens.iter().any(|token| token == term))
            .count()
    }

    fn idf(&self, term: &str) -> ScoreType {
        fancy_idf(
            self.document_count() as ScoreType,
            self.df(term) as ScoreType,
        )
        .max(0.0)
    }

    /// Every matching document of one segment, best first.
    fn rank(&self, segment: usize, terms: &[String]) -> Vec<ScoredPointOffset> {
        let Bm25Params { k1, b } = self.params;
        let avg = self.avg_doc_len();
        let mut distinct = terms.to_vec();
        distinct.sort();
        distinct.dedup();
        let mut hits: Vec<ScoredPointOffset> = self.segments[segment]
            .live()
            .filter_map(|(idx, tokens)| {
                let len = tokens.len() as ScoreType;
                let score: ScoreType = distinct
                    .iter()
                    .map(|term| {
                        let tf = tokens.iter().filter(|token| *token == term).count() as ScoreType;
                        if tf == 0.0 {
                            return 0.0;
                        }
                        self.idf(term) * tf * (k1 + 1.0) / (tf + k1 * (1.0 - b + b * len / avg))
                    })
                    .sum();
                (score > 0.0).then_some(ScoredPointOffset { idx, score })
            })
            .collect();
        hits.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
        hits
    }
}

fn engine_rank(
    segment: &Segment,
    terms: &[String],
    context: &TextQueryContext<'_>,
    limit: usize,
) -> Vec<ScoredPointOffset> {
    let payload_index = segment.payload_index.borrow();
    let text_index = payload_index
        .field_indexes
        .get(&field())
        .expect("the field is indexed")
        .iter()
        .find_map(|index| match index {
            FieldIndex::FullTextIndex(index) => Some(index),
            _ => None,
        })
        .expect("the field has a text index");
    score_bm25(
        text_index,
        terms,
        context,
        Bm25Params::default(),
        &|_| true,
        limit,
        &AtomicBool::new(false),
        &HardwareCounterCell::new(),
    )
    .unwrap()
}

fn assert_close(actual: ScoreType, expected: ScoreType, what: &str) {
    assert!(
        (actual - expected).abs() <= 1e-4 * expected.abs().max(1.0),
        "{what}: engine {actual}, reference {expected}",
    );
}

/// `actual` is a valid top-k of `expected`: same scores in order, and every
/// returned document carries its reference score. Ids are compared through
/// their scores, since equal scores may legitimately come out in either order.
fn assert_top_k(actual: &[ScoredPointOffset], expected: &[ScoredPointOffset], limit: usize) {
    assert_eq!(actual.len(), expected.len().min(limit));
    let by_id: HashMap<PointOffsetType, ScoreType> =
        expected.iter().map(|hit| (hit.idx, hit.score)).collect();
    for (rank, (hit, reference)) in actual.iter().zip(expected).enumerate() {
        assert_close(hit.score, reference.score, &format!("rank {rank}"));
        let own = by_id
            .get(&hit.idx)
            .unwrap_or_else(|| panic!("document {} is not in the reference", hit.idx));
        assert_close(hit.score, *own, &format!("document {}", hit.idx));
    }
}

/// Two appendable segments, deletions in both, and the whole chain from raw
/// text to a ranking checked against the definition: the gathered `N`, `df`
/// and `avgdl` first, then every segment's ranking for every query.
#[test]
fn bm25_matches_the_reference_end_to_end() {
    // The const is `false`; this is what lets a segment record lengths.
    let _scoring = TextIndexParams::override_scoring(true);

    let mut rng = StdRng::seed_from_u64(2026);
    let corpora: [Vec<String>; 2] = [
        (0..180).map(|_| document(&mut rng)).collect(),
        (0..320).map(|_| document(&mut rng)).collect(),
    ];
    let deleted: [Vec<u64>; 2] = [vec![3, 4, 50, 51, 52], vec![0, 100, 200, 300, 319]];

    let dirs = [
        Builder::new().prefix("bm25_a").tempdir().unwrap(),
        Builder::new().prefix("bm25_b").tempdir().unwrap(),
    ];
    let segments: Vec<Segment> = (0..2)
        .map(|i| build_text_segment(&dirs[i].path().join("segment"), &corpora[i], &deleted[i]))
        .collect();

    let tokenizer = Tokenizer::new_from_text_index_params(&text_params());
    let tokenized: Vec<TokenizedSegment> = (0..2)
        .map(|i| TokenizedSegment::new(&tokenizer, &corpora[i], &deleted[i]))
        .collect();
    let reference = Reference {
        segments: &tokenized,
        params: Bm25Params::default(),
    };

    let mut queries: Vec<String> = (0..QUERY_COUNT)
        .map(|_| {
            let len = rng.random_range(1..=4);
            (0..len)
                .map(|_| word(&mut rng))
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    queries.push("term0 term0 term1".to_owned());
    queries.push("term5 nowhere".to_owned());
    queries.push("nowhere".to_owned());

    for query in &queries {
        let mut terms = Vec::new();
        tokenizer.tokenize_query(query, |token| terms.push(token.into_owned()));

        let mut query_context = QueryContext::default();
        query_context.init_text_stats(&field(), terms.iter().cloned());
        for segment in &segments {
            segment.fill_query_context(&mut query_context).unwrap();
        }
        let segment_context = query_context.get_segment_query_context();
        let context = segment_context.get_text_context(&field()).unwrap();

        // The statistics, before any ranking depends on them.
        assert_eq!(context.document_count(), reference.document_count());
        assert_close(
            context.avg_doc_len().expect("lengths are recorded"),
            reference.avg_doc_len(),
            "avgdl",
        );
        for term in &terms {
            assert_eq!(
                context.document_frequency(term),
                reference.df(term),
                "df of {term}"
            );
            assert_close(
                context.idf(term),
                reference.idf(term),
                &format!("idf of {term}"),
            );
        }

        for (i, segment) in segments.iter().enumerate() {
            let expected = reference.rank(i, &terms);
            eprintln!("query {query:?}, segment {i}: {} matches", expected.len());
            assert_top_k(
                &engine_rank(segment, &terms, &context, LIMIT),
                &expected,
                LIMIT,
            );
            assert_top_k(
                &engine_rank(segment, &terms, &context, usize::MAX),
                &expected,
                usize::MAX,
            );
        }
    }
}

/// Point offsets of `points`, which equal their ids in a segment built by
/// [`build_text_segment`]: ids are upserted in order from zero.
fn as_offsets(points: &[ScoredPoint]) -> Vec<ScoredPointOffset> {
    points
        .iter()
        .map(|point| match point.id {
            ExtendedPointId::NumId(id) => ScoredPointOffset {
                idx: id as PointOffsetType,
                score: point.score,
            },
            ExtendedPointId::Uuid(_) => panic!("the fixture uses numeric ids"),
        })
        .collect()
}

/// The segment entry point, from tokenized terms to `ScoredPoint`s: the
/// reference ranking with ids, versions and payloads attached, and each
/// exclusion a query applies on top of the index's own deletions. A filter
/// narrows the candidates without touching the statistics, and a deleted
/// mask in the query context takes the id tracker's place, which is how a
/// proxy segment hides the points deleted since it was created.
#[test]
fn segment_entry_point_scores_what_a_query_sees() {
    let _scoring = TextIndexParams::override_scoring(true);

    let mut rng = StdRng::seed_from_u64(7);
    let corpus: Vec<String> = (0..240).map(|_| document(&mut rng)).collect();
    let deleted = [5, 6, 70];
    let dir = Builder::new().prefix("bm25_entry").tempdir().unwrap();
    let segment = build_text_segment(&dir.path().join("segment"), &corpus, &deleted);

    let tokenizer = Tokenizer::new_from_text_index_params(&text_params());
    let tokenized = [TokenizedSegment::new(&tokenizer, &corpus, &deleted)];
    let reference = Reference {
        segments: &tokenized,
        params: Bm25Params::default(),
    };

    let even: AHashSet<PointIdType> = (0..corpus.len() as u64)
        .step_by(2)
        .map(PointIdType::from)
        .collect();
    let even_only = Filter::new_must(Condition::HasId(HasIdCondition::from(even)));
    let mut mask = BitVec::repeat(false, corpus.len());
    for idx in (0..corpus.len()).step_by(10) {
        mask.set(idx, true);
    }

    let with_payload = WithPayload::from(true);
    let without_vector = WithVector::Bool(false);
    for _ in 0..QUERY_COUNT {
        let query = (0..rng.random_range(1..=4))
            .map(|_| word(&mut rng))
            .collect::<Vec<_>>()
            .join(" ");
        let mut terms = Vec::new();
        tokenizer.tokenize_query(&query, |token| terms.push(token.into_owned()));

        let mut query_context = QueryContext::default();
        query_context.init_text_stats(&field(), terms.iter().cloned());
        segment.fill_query_context(&mut query_context).unwrap();
        let segment_context = query_context.get_segment_query_context();
        let expected = reference.rank(0, &terms);

        let score = |filter: Option<&Filter>, context: &SegmentQueryContext| {
            segment
                .score_bm25(
                    &field(),
                    &terms,
                    Bm25Params::default(),
                    &with_payload,
                    &without_vector,
                    filter,
                    LIMIT,
                    context,
                )
                .unwrap()
        };

        let points = score(None, &segment_context);
        assert_top_k(&as_offsets(&points), &expected, LIMIT);
        for point in &points {
            let ExtendedPointId::NumId(id) = point.id else {
                unreachable!()
            };
            assert_eq!(
                point.payload.as_ref().and_then(|p| p.0.get("text")),
                Some(&serde_json::Value::from(corpus[id as usize].as_str())),
                "payload of point {id}",
            );
            assert!(point.version > 0, "point {id} carries its version");
        }

        let filtered: Vec<_> = expected
            .iter()
            .filter(|hit| hit.idx % 2 == 0)
            .copied()
            .collect();
        assert_top_k(
            &as_offsets(&score(Some(&even_only), &segment_context)),
            &filtered,
            LIMIT,
        );

        let masked_context = segment_context.fork().with_deleted_points(&mask);
        let unmasked: Vec<_> = expected
            .iter()
            .filter(|hit| !mask[hit.idx as usize])
            .copied()
            .collect();
        assert_top_k(&as_offsets(&score(None, &masked_context)), &unmasked, LIMIT);
    }
}

/// Scoring without statistics would silently rank by an IDF of zero
/// documents, so a context nothing seeded for the field is an error.
#[test]
fn segment_entry_point_requires_seeded_statistics() {
    let _scoring = TextIndexParams::override_scoring(true);
    let dir = Builder::new().prefix("bm25_unseeded").tempdir().unwrap();
    let segment = build_text_segment(
        &dir.path().join("segment"),
        &["term1 term2".to_owned()],
        &[],
    );
    let query_context = QueryContext::default();
    let result = segment.score_bm25(
        &field(),
        &["term1".to_owned()],
        Bm25Params::default(),
        &WithPayload::from(false),
        &WithVector::Bool(false),
        None,
        LIMIT,
        &query_context.get_segment_query_context(),
    );
    assert!(result.is_err(), "unseeded statistics must be refused");
}

/// Deferred points are invisible to a query until they are applied, so the
/// entry point never scores them, while the index itself still holds them.
#[test]
fn segment_entry_point_skips_deferred_points() {
    let _scoring = TextIndexParams::override_scoring(true);

    const CUTOFF: PointOffsetType = 100;
    let mut rng = StdRng::seed_from_u64(11);
    let corpus: Vec<String> = (0..200).map(|_| document(&mut rng)).collect();
    let dir = Builder::new().prefix("bm25_deferred").tempdir().unwrap();
    let segment =
        build_text_segment_deferred(&dir.path().join("segment"), &corpus, &[], Some(CUTOFF));
    let tokenizer = Tokenizer::new_from_text_index_params(&text_params());

    let mut deferred_hits = 0;
    for _ in 0..QUERY_COUNT {
        let query = (0..rng.random_range(1..=4))
            .map(|_| word(&mut rng))
            .collect::<Vec<_>>()
            .join(" ");
        let mut terms = Vec::new();
        tokenizer.tokenize_query(&query, |token| terms.push(token.into_owned()));

        let mut query_context = QueryContext::default();
        query_context.init_text_stats(&field(), terms.iter().cloned());
        segment.fill_query_context(&mut query_context).unwrap();
        let segment_context = query_context.get_segment_query_context();
        let context = segment_context.get_text_context(&field()).unwrap();

        // The index ranks every point it holds; a query sees the visible ones.
        let everything = engine_rank(&segment, &terms, &context, usize::MAX);
        deferred_hits += everything.iter().filter(|hit| hit.idx >= CUTOFF).count();
        let visible: Vec<_> = everything
            .iter()
            .filter(|hit| hit.idx < CUTOFF)
            .copied()
            .collect();

        let points = segment
            .score_bm25(
                &field(),
                &terms,
                Bm25Params::default(),
                &WithPayload::from(false),
                &WithVector::Bool(false),
                None,
                LIMIT,
                &segment_context,
            )
            .unwrap();
        assert_top_k(&as_offsets(&points), &visible, LIMIT);
    }
    assert!(deferred_hits > 0, "the queries must reach deferred points");
}
