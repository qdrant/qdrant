//! BM25 over the text payload index, per index shape.
//!
//! The number to set against `lib/collection/benches/bm25_sparse_bench.rs`:
//! the same corpus shape, the same query shape, the same limit. Each query is
//! timed with its statistics gather included, since the shard-level sparse
//! search gathers IDF per query as well. What this leaves out is everything
//! above one segment: routing, the merge across segments, and the id tracker.

#[cfg(not(target_os = "windows"))]
mod prof;

use std::sync::atomic::AtomicBool;

use common::bitvec::BitVec;
use common::counter::hardware_counter::HardwareCounterCell;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::index::{TextIndexParams, TokenizerType};
use segment::data_types::query_context::QueryContext;
use segment::index::field_index::FieldIndexBuilderTrait;
use segment::index::field_index::full_text_index::full_text_index_read::{
    fill_text_statistics, score_bm25,
};
use segment::index::field_index::full_text_index::{Bm25Params, FullTextIndex};
use segment::json_path::JsonPath;
use tempfile::{Builder, TempDir};

const POINT_COUNT: usize = 20_000;
const VOCAB_SIZE: usize = 20_000;
const DOC_LEN: std::ops::RangeInclusive<usize> = 20..=200;
const QUERY_TERMS: std::ops::RangeInclusive<usize> = 2..=5;
const QUERY_COUNT: usize = 50;
const LIMIT: usize = 10;

/// Zipf-like vocabulary, as in the sparse baseline: a handful of terms appear
/// in most documents and the tail in almost none, so pruning has something to
/// prune.
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

    fn document(&self, rng: &mut SmallRng) -> String {
        let len = rng.random_range(DOC_LEN);
        (0..len)
            .map(|_| self.term(rng))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn config() -> TextIndexParams {
    TextIndexParams {
        tokenizer: TokenizerType::Whitespace,
        lowercase: Some(false),
        phrase_matching: Some(true),
        ..TextIndexParams::default()
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Mutable,
    Immutable,
    OnDisk,
}

fn build(shape: Shape, documents: &[String]) -> (FullTextIndex, TempDir) {
    let dir = Builder::new().prefix("bm25_text_bench").tempdir().unwrap();
    let hw_counter = HardwareCounterCell::new();
    let empty_deleted = BitVec::new();

    fn fill<B: FieldIndexBuilderTrait<FieldIndexType = FullTextIndex>>(
        mut builder: B,
        documents: &[String],
        hw_counter: &HardwareCounterCell,
    ) -> FullTextIndex {
        builder.init().unwrap();
        for (id, document) in documents.iter().enumerate() {
            let value = serde_json::Value::String(document.clone());
            builder.add_point(id as u32, &[&value], hw_counter).unwrap();
        }
        builder.finalize().unwrap()
    }

    let index = match shape {
        Shape::Mutable => fill(
            FullTextIndex::builder_gridstore(dir.path().to_path_buf(), config(), true),
            documents,
            &hw_counter,
        ),
        Shape::Immutable => fill(
            FullTextIndex::builder_mmap(
                dir.path().to_path_buf(),
                config(),
                false,
                &empty_deleted,
                true,
            ),
            documents,
            &hw_counter,
        ),
        Shape::OnDisk => fill(
            FullTextIndex::builder_mmap(
                dir.path().to_path_buf(),
                config(),
                true,
                &empty_deleted,
                true,
            ),
            documents,
            &hw_counter,
        ),
    };
    (index, dir)
}

/// One query end to end on one segment: gather the statistics, then score.
fn search(index: &FullTextIndex, field: &JsonPath, terms: &[String]) -> usize {
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
    .len()
}

fn text_bm25_search(c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(42);
    let vocabulary = Vocabulary::new();
    let documents: Vec<String> = (0..POINT_COUNT)
        .map(|_| vocabulary.document(&mut rng))
        .collect();
    let queries: Vec<Vec<String>> = (0..QUERY_COUNT)
        .map(|_| {
            let len = rng.random_range(QUERY_TERMS);
            (0..len).map(|_| vocabulary.term(&mut rng)).collect()
        })
        .collect();
    let field = JsonPath::new("text");

    let mut group = c.benchmark_group("bm25-text");
    group.sample_size(20);
    for (name, shape) in [
        ("mutable", Shape::Mutable),
        ("immutable", Shape::Immutable),
        ("on-disk", Shape::OnDisk),
    ] {
        let (index, _dir) = build(shape, &documents);
        group.bench_function(name, |b| {
            b.iter(|| {
                queries
                    .iter()
                    .map(|terms| search(&index, &field, terms))
                    .sum::<usize>()
            })
        });
    }
    group.finish();
}

#[cfg(not(target_os = "windows"))]
criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(prof::FlamegraphProfiler::new(100));
    targets = text_bm25_search
}

#[cfg(target_os = "windows")]
criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = text_bm25_search
}

criterion_main!(benches);
