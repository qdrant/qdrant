//! BM25 over sparse vectors against BM25 over the text payload index, on the
//! same corpus, the same queries, at the same level: one index, no shard.
//!
//! Measures per-query latency for every index shape of both routes, and
//! recall at `LIMIT` against BM25 by definition, computed here from the raw
//! documents with the corpus's true average length. The sparse route embeds
//! documents with a fixed average length, 256 by default, so it is also
//! measured with that constant set to the corpus's real average, to separate
//! the cost of the constant from the rest.
//!
//! Ignored: it is a measurement, not a check. Run it in release:
//!
//! ```text
//! BM25_COMPARE_DOCS=200000 BM25_COMPARE_OUT=/tmp/bm25_compare.json \
//!   cargo test --release -p segment --features testing --test integration \
//!   bm25_compare -- --ignored --nocapture
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use bm25::{Bm25, Bm25Params as SparseBm25Params};
use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType, ScoredPointOffset};
use common::universal_io::{MmapFile, MmapFs};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::{TextIndexParams, TokenizerType};
use segment::data_types::query_context::{QueryContext, VectorQueryContext, fancy_idf};
use segment::data_types::vectors::QueryVector;
use segment::fixtures::sparse_fixtures::fixture_sparse_index_from_iter;
use segment::index::VectorIndexRead;
use segment::index::field_index::FieldIndexBuilderTrait;
use segment::index::field_index::full_text_index::full_text_index_read::{
    fill_text_statistics, score_bm25,
};
use segment::index::field_index::full_text_index::{Bm25Params, FullTextIndex};
use segment::index::sparse_index::sparse_index_config::{SparseIndexConfig, SparseIndexType};
use segment::index::sparse_index::sparse_vector_index::{
    SparseVectorIndex, SparseVectorIndexOpenArgs,
};
use segment::json_path::JsonPath;
use sparse::common::sparse_vector::SparseVector;
use sparse::index::inverted_index::inverted_index_compressed_immutable_ram::InvertedIndexCompressedImmutableRam;
use sparse::index::inverted_index::inverted_index_compressed_mmap::InvertedIndexCompressedMmap;
use sparse::index::inverted_index::inverted_index_ram::InvertedIndexRam;
use tempfile::Builder;

const VOCAB_SIZE: usize = 20_000;
const DOC_LEN: std::ops::RangeInclusive<usize> = 20..=200;
const QUERY_TERMS: std::ops::RangeInclusive<usize> = 2..=5;
const QUERY_COUNT: usize = 50;
const LIMIT: usize = 10;
/// Passes over the query set; the first is a warm-up and is not counted.
const PASSES: usize = 6;
const K1: f64 = 1.2;
const B: f64 = 0.75;

struct Vocabulary {
    cumulative: Vec<f64>,
}

impl Vocabulary {
    fn new() -> Self {
        let mut cumulative = Vec::with_capacity(VOCAB_SIZE);
        let mut total = 0.0;
        for rank in 0..VOCAB_SIZE {
            total += 1.0 / ((rank + 1) as f64).powf(0.9);
            cumulative.push(total);
        }
        Self { cumulative }
    }

    fn term(&self, rng: &mut SmallRng) -> String {
        let target = rng.random_range(0.0..*self.cumulative.last().unwrap());
        let rank = self.cumulative.partition_point(|sum| *sum < target);
        format!("w{rank}")
    }

    fn document(&self, rng: &mut SmallRng) -> Vec<String> {
        let len = rng.random_range(DOC_LEN);
        (0..len).map(|_| self.term(rng)).collect()
    }
}

/// BM25 by definition over the whole corpus, from the raw token lists.
struct Reference {
    /// term -> (document, tf), by document.
    postings: HashMap<String, Vec<(PointOffsetType, u32)>>,
    lengths: Vec<u32>,
    avg_doc_len: f64,
}

impl Reference {
    fn new(documents: &[Vec<String>]) -> Self {
        let mut postings: HashMap<String, Vec<(PointOffsetType, u32)>> = HashMap::new();
        let mut lengths = Vec::with_capacity(documents.len());
        let mut total = 0usize;
        for (idx, tokens) in documents.iter().enumerate() {
            lengths.push(tokens.len() as u32);
            total += tokens.len();
            let mut counts: HashMap<&str, u32> = HashMap::new();
            for token in tokens {
                *counts.entry(token).or_default() += 1;
            }
            for (token, tf) in counts {
                postings
                    .entry(token.to_owned())
                    .or_default()
                    .push((idx as PointOffsetType, tf));
            }
        }
        Self {
            postings,
            lengths,
            avg_doc_len: total as f64 / documents.len() as f64,
        }
    }

    fn top(&self, terms: &[String], limit: usize) -> Vec<ScoredPointOffset> {
        let n = self.lengths.len() as f64;
        let mut distinct: Vec<&String> = terms.iter().collect();
        distinct.sort();
        distinct.dedup();
        let mut scores: HashMap<PointOffsetType, f64> = HashMap::new();
        for term in distinct {
            let Some(posting) = self.postings.get(term) else {
                continue;
            };
            let idf = fancy_idf(n as ScoreType, posting.len() as ScoreType).max(0.0) as f64;
            for &(doc, tf) in posting {
                let tf = f64::from(tf);
                let len = f64::from(self.lengths[doc as usize]);
                let norm = K1 * (1.0 - B + B * len / self.avg_doc_len);
                *scores.entry(doc).or_default() += idf * tf * (K1 + 1.0) / (tf + norm);
            }
        }
        let mut hits: Vec<ScoredPointOffset> = scores
            .into_iter()
            .map(|(idx, score)| ScoredPointOffset {
                idx,
                score: score as ScoreType,
            })
            .collect();
        hits.sort_unstable_by(|a, b| b.score.total_cmp(&a.score).then(a.idx.cmp(&b.idx)));
        hits.truncate(limit);
        hits
    }
}

fn recall(actual: &[ScoredPointOffset], truth: &[ScoredPointOffset]) -> f64 {
    if truth.is_empty() {
        return 1.0;
    }
    let truth_ids: HashSet<PointOffsetType> = truth.iter().map(|hit| hit.idx).collect();
    let hits = actual
        .iter()
        .filter(|hit| truth_ids.contains(&hit.idx))
        .count();
    hits as f64 / truth.len() as f64
}

fn overlap(a: &[ScoredPointOffset], b: &[ScoredPointOffset]) -> f64 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let ids: HashSet<PointOffsetType> = a.iter().map(|hit| hit.idx).collect();
    let common = b.iter().filter(|hit| ids.contains(&hit.idx)).count();
    common as f64 / a.len().max(b.len()) as f64
}

struct Measured {
    route: &'static str,
    shape: &'static str,
    variant: &'static str,
    build: Duration,
    /// Per-query latencies over the counted passes.
    latencies: Vec<Duration>,
    /// One ranking per query, from the last pass.
    rankings: Vec<Vec<ScoredPointOffset>>,
}

impl Measured {
    fn percentile(&self, p: f64) -> Duration {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        sorted[((sorted.len() - 1) as f64 * p) as usize]
    }

    fn mean(&self) -> Duration {
        self.latencies.iter().sum::<Duration>() / self.latencies.len() as u32
    }
}

/// Time `search` over every query for `PASSES` passes, keeping the rankings of
/// the last one.
fn measure(
    route: &'static str,
    shape: &'static str,
    variant: &'static str,
    build: Duration,
    queries: &[Vec<String>],
    mut search: impl FnMut(&[String]) -> Vec<ScoredPointOffset>,
) -> Measured {
    let mut latencies = Vec::with_capacity(queries.len() * (PASSES - 1));
    let mut rankings = Vec::with_capacity(queries.len());
    for pass in 0..PASSES {
        rankings.clear();
        for query in queries {
            let start = Instant::now();
            let ranking = search(query);
            let elapsed = start.elapsed();
            if pass > 0 {
                latencies.push(elapsed);
            }
            rankings.push(ranking);
        }
    }
    Measured {
        route,
        shape,
        variant,
        build,
        latencies,
        rankings,
    }
}

// ---- text route ------------------------------------------------------------

fn text_config() -> TextIndexParams {
    TextIndexParams {
        tokenizer: TokenizerType::Whitespace,
        lowercase: Some(false),
        phrase_matching: Some(true),
        ..TextIndexParams::default()
    }
}

#[derive(Clone, Copy)]
enum TextShape {
    Mutable,
    Immutable,
    OnDisk,
}

fn build_text(shape: TextShape, documents: &[Vec<String>]) -> (FullTextIndex, tempfile::TempDir) {
    let dir = Builder::new()
        .prefix("bm25_compare_text")
        .tempdir()
        .unwrap();
    let hw_counter = HardwareCounterCell::new();
    let empty_deleted = BitVec::new();

    fn fill<B: FieldIndexBuilderTrait<FieldIndexType = FullTextIndex>>(
        mut builder: B,
        documents: &[Vec<String>],
        hw_counter: &HardwareCounterCell,
    ) -> FullTextIndex {
        builder.init().unwrap();
        for (id, document) in documents.iter().enumerate() {
            let value = serde_json::Value::String(document.join(" "));
            builder.add_point(id as u32, &[&value], hw_counter).unwrap();
        }
        builder.finalize().unwrap()
    }

    let path = dir.path().to_path_buf();
    let index = match shape {
        TextShape::Mutable => fill(
            FullTextIndex::builder_gridstore(path, text_config(), true),
            documents,
            &hw_counter,
        ),
        TextShape::Immutable => fill(
            FullTextIndex::builder_mmap(path, text_config(), false, &empty_deleted, true),
            documents,
            &hw_counter,
        ),
        TextShape::OnDisk => fill(
            FullTextIndex::builder_mmap(path, text_config(), true, &empty_deleted, true),
            documents,
            &hw_counter,
        ),
    };
    (index, dir)
}

fn text_search(
    index: &FullTextIndex,
    field: &JsonPath,
    terms: &[String],
) -> Vec<ScoredPointOffset> {
    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);
    let mut query_context = QueryContext::default();
    query_context.init_text_stats(field, terms.iter().cloned());
    fill_text_statistics(
        index,
        query_context.mut_text_stats().get_mut(field).unwrap(),
        &is_stopped,
        &hw_counter,
    )
    .unwrap();
    let segment_context = query_context.get_segment_query_context();
    let context = segment_context.get_text_context(field).unwrap();
    score_bm25(
        index,
        terms,
        &context,
        Bm25Params::default(),
        &|_| true,
        LIMIT,
        &is_stopped,
        &hw_counter,
    )
    .unwrap()
}

// ---- sparse route ----------------------------------------------------------

fn embed(bm25: &Bm25, tokens: &[String]) -> SparseVector {
    let tokens: Vec<_> = tokens.iter().map(|t| t.as_str().into()).collect();
    let embedding = bm25.embed_document(&tokens);
    SparseVector::new(embedding.indices, embedding.values).unwrap()
}

/// The production query path: `fill_idf_statistics` for the query's
/// dimensions, then `remap_idf_weights`, then the search. The remap is
/// inlined, since it lives on a context this harness has no shard to build.
fn sparse_search<I: sparse::index::inverted_index::InvertedIndex>(
    index: &SparseVectorIndex<I>,
    bm25: &Bm25,
    terms: &[String],
) -> Vec<ScoredPointOffset> {
    let hw_counter = HardwareCounterCell::new();
    let is_stopped = AtomicBool::new(false);
    let tokens: Vec<_> = terms.iter().map(|t| t.as_str().into()).collect();
    let mut query = bm25.embed_query(&tokens);

    let mut df: HashMap<u32, usize> = query.indices.iter().map(|dim| (*dim, 0)).collect();
    let n = index
        .fill_idf_statistics(&mut df, None, &is_stopped, &hw_counter)
        .unwrap() as ScoreType;
    for (weight, dim) in query.values.iter_mut().zip(&query.indices) {
        *weight *= fancy_idf(n, df[dim] as ScoreType);
    }

    let vector = QueryVector::from(SparseVector::new(query.indices, query.values).unwrap());
    index
        .search(
            &[&vector],
            None,
            LIMIT,
            None,
            &VectorQueryContext::default(),
        )
        .unwrap()
        .remove(0)
}

fn sparse_params(avg_doc_len: f64) -> SparseBm25Params {
    SparseBm25Params {
        k1: K1,
        b: B,
        avg_doc_len,
    }
}

/// Every sparse shape over one embedding, sharing storage like the optimizer
/// would: the mutable index is built first, the immutable ones are built from
/// its storage.
fn measure_sparse(
    variant: &'static str,
    bm25: &Bm25,
    documents: &[Vec<String>],
    queries: &[Vec<String>],
    shapes: &[SparseIndexType],
) -> Vec<Measured> {
    let data_dir = Builder::new()
        .prefix("bm25_compare_sparse")
        .tempdir()
        .unwrap();
    let stopped = AtomicBool::new(false);

    let start = Instant::now();
    let vectors: Vec<SparseVector> = documents.iter().map(|doc| embed(bm25, doc)).collect();
    let mutable = fixture_sparse_index_from_iter::<InvertedIndexRam>(
        data_dir.path(),
        vectors.into_iter(),
        1,
        SparseIndexType::MutableRam,
    )
    .unwrap();
    let mutable_build = start.elapsed();

    let mut out = Vec::new();
    for shape in shapes {
        match shape {
            SparseIndexType::MutableRam => out.push(measure(
                "sparse",
                "mutable",
                variant,
                mutable_build,
                queries,
                |terms| sparse_search(&mutable, bm25, terms),
            )),
            SparseIndexType::ImmutableRam => {
                let dir = Builder::new()
                    .prefix("bm25_compare_sparse_imm")
                    .tempdir()
                    .unwrap();
                let start = Instant::now();
                let index: SparseVectorIndex<InvertedIndexCompressedImmutableRam<f32>> =
                    SparseVectorIndex::open(SparseVectorIndexOpenArgs {
                        fs: &MmapFs,
                        config: SparseIndexConfig::new(
                            Some(1),
                            SparseIndexType::ImmutableRam,
                            None,
                            None,
                        ),
                        id_tracker: mutable.id_tracker().clone(),
                        vector_storage: mutable.vector_storage().clone(),
                        payload_index: mutable.payload_index().clone(),
                        path: dir.path(),
                        stopped: &stopped,
                        tick_progress: || (),
                    })
                    .unwrap();
                let build = start.elapsed();
                out.push(measure(
                    "sparse",
                    "immutable",
                    variant,
                    build,
                    queries,
                    |terms| sparse_search(&index, bm25, terms),
                ));
            }
            SparseIndexType::Mmap => {
                let dir = Builder::new()
                    .prefix("bm25_compare_sparse_mmap")
                    .tempdir()
                    .unwrap();
                let start = Instant::now();
                let index: SparseVectorIndex<InvertedIndexCompressedMmap<f32, MmapFile>> =
                    SparseVectorIndex::open(SparseVectorIndexOpenArgs {
                        fs: &MmapFs,
                        config: SparseIndexConfig::new(Some(1), SparseIndexType::Mmap, None, None),
                        id_tracker: mutable.id_tracker().clone(),
                        vector_storage: mutable.vector_storage().clone(),
                        payload_index: mutable.payload_index().clone(),
                        path: dir.path(),
                        stopped: &stopped,
                        tick_progress: || (),
                    })
                    .unwrap();
                let build = start.elapsed();
                out.push(measure(
                    "sparse",
                    "on-disk",
                    variant,
                    build,
                    queries,
                    |terms| sparse_search(&index, bm25, terms),
                ));
            }
        }
    }
    out
}

#[test]
#[ignore = "a measurement, not a check; run in release with --ignored"]
fn bm25_sparse_vs_text_compare() {
    let docs: usize = std::env::var("BM25_COMPARE_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let out_path = std::env::var("BM25_COMPARE_OUT").ok();

    let mut rng = SmallRng::seed_from_u64(42);
    let vocabulary = Vocabulary::new();
    let documents: Vec<Vec<String>> = (0..docs).map(|_| vocabulary.document(&mut rng)).collect();
    let queries: Vec<Vec<String>> = (0..QUERY_COUNT)
        .map(|_| {
            let len = rng.random_range(QUERY_TERMS);
            (0..len).map(|_| vocabulary.term(&mut rng)).collect()
        })
        .collect();

    let reference = Reference::new(&documents);
    let truth: Vec<Vec<ScoredPointOffset>> =
        queries.iter().map(|q| reference.top(q, LIMIT)).collect();
    eprintln!(
        "{docs} documents, {} queries, true avgdl {:.1}",
        queries.len(),
        reference.avg_doc_len
    );

    let field = JsonPath::new("text");
    let mut measured = Vec::new();

    for (shape, name) in [
        (TextShape::Mutable, "mutable"),
        (TextShape::Immutable, "immutable"),
        (TextShape::OnDisk, "on-disk"),
    ] {
        let start = Instant::now();
        let (index, _dir) = build_text(shape, &documents);
        let build = start.elapsed();
        measured.push(measure(
            "text",
            name,
            "avgdl from corpus",
            build,
            &queries,
            |terms| text_search(&index, &field, terms),
        ));
    }

    let default = Bm25::new(sparse_params(SparseBm25Params::DEFAULT_AVG_DOC_LEN)).unwrap();
    measured.extend(measure_sparse(
        "avg_len 256 (default)",
        &default,
        &documents,
        &queries,
        &[
            SparseIndexType::MutableRam,
            SparseIndexType::ImmutableRam,
            SparseIndexType::Mmap,
        ],
    ));
    let tuned = Bm25::new(sparse_params(reference.avg_doc_len)).unwrap();
    measured.extend(measure_sparse(
        "avg_len = corpus average",
        &tuned,
        &documents,
        &queries,
        &[SparseIndexType::ImmutableRam],
    ));

    let text_rankings = &measured[1].rankings;
    let mut rows = Vec::new();
    eprintln!(
        "{:<7} {:<10} {:<26} {:>9} {:>9} {:>9} {:>8} {:>8}",
        "route", "shape", "variant", "p50 us", "p95 us", "mean us", "recall", "build s"
    );
    for m in &measured {
        let recall_at_limit = m
            .rankings
            .iter()
            .zip(&truth)
            .map(|(actual, truth)| recall(actual, truth))
            .sum::<f64>()
            / truth.len() as f64;
        let overlap_with_text = m
            .rankings
            .iter()
            .zip(text_rankings)
            .map(|(a, b)| overlap(a, b))
            .sum::<f64>()
            / truth.len() as f64;
        eprintln!(
            "{:<7} {:<10} {:<26} {:>9.0} {:>9.0} {:>9.0} {:>8.3} {:>8.2}",
            m.route,
            m.shape,
            m.variant,
            m.percentile(0.5).as_secs_f64() * 1e6,
            m.percentile(0.95).as_secs_f64() * 1e6,
            m.mean().as_secs_f64() * 1e6,
            recall_at_limit,
            m.build.as_secs_f64(),
        );
        rows.push(serde_json::json!({
            "route": m.route,
            "shape": m.shape,
            "variant": m.variant,
            "p50_us": m.percentile(0.5).as_secs_f64() * 1e6,
            "p95_us": m.percentile(0.95).as_secs_f64() * 1e6,
            "mean_us": m.mean().as_secs_f64() * 1e6,
            "recall_at_10": recall_at_limit,
            "overlap_with_text": overlap_with_text,
            "build_s": m.build.as_secs_f64(),
        }));
    }

    if let Some(path) = out_path {
        let report = serde_json::json!({
            "documents": docs,
            "queries": queries.len(),
            "limit": LIMIT,
            "avg_doc_len": reference.avg_doc_len,
            "rows": rows,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap()).unwrap();
        eprintln!("written {path}");
    }
}
