//! BM25 over the text payload index, end to end through a local shard.
//!
//! The counterpart of `bm25_sparse_bench`: the same corpus and queries from
//! `segment::fixtures::bm25_corpus`, the same shard states, no HTTP. Documents
//! are stored as a text payload whose index records lengths (`scoring`), and
//! queried with `LocalShard::score_bm25`, which tokenizes the text, gathers the
//! statistics over the shard's segments and scores each one.
//!
//! Three shard states, one per text index shape: freshly ingested, where the
//! index is the mutable one; optimized into the immutable RAM index; and
//! optimized with the index placed on disk. There is no avgdl variant: the
//! text route reads it from the data, which is what the sparse route's
//! `optimized-corpus-avgdl` state emulates. Recall against BM25 by definition
//! is printed before the timings.
//!
//! Every point also carries a small random dense vector, never queried: the
//! optimizer decides what to rebuild from vector storage alone, so a shard of
//! payloads only would never leave the appendable segment.
//!
//! `BM25_TEXT_DOCS` overrides the document count, 200k by default like the
//! sparse baseline, so the two report the same corpus.

use std::sync::Arc;
use std::time::{Duration, Instant};

use collection::common::adaptive_handle::AdaptiveSearchHandle;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use collection::operations::vector_params_builder::VectorParamsBuilder;
use collection::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::local_shard::LocalShard;
use collection::shards::shard_trait::{ShardOperation, WaitUntil};
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::{PointOffsetType, ScoredPointOffset};
use criterion::{Criterion, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use segment::data_types::index::{TextIndexParams, TextScoringParams, TokenizerType};
use segment::data_types::vectors::VectorStructInternal;
use segment::fixtures::bm25_corpus::{LIMIT, QUERY_COUNT, Reference, Vocabulary, recall};
use segment::fixtures::payload_fixtures::random_vector;
use segment::index::field_index::full_text_index::Bm25Params;
use segment::json_path::JsonPath;
use segment::payload_json;
use segment::types::{
    Distance, ExtendedPointId, Memory, PayloadFieldSchema, PayloadSchemaParams, ScoredPoint,
    SegmentType, WithPayload, WithVector,
};
use shard::payload_index_schema::PayloadIndexSchema;
use shard::query::text::TextScoringQuery;
use tempfile::Builder;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

const TEXT_FIELD: &str = "text";
const DEFAULT_POINT_COUNT: usize = 200_000;
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const VECTOR_DIM: usize = 4;

fn point_count() -> usize {
    let count = std::env::var("BM25_TEXT_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_POINT_COUNT);
    assert!(count > 0, "BM25_TEXT_DOCS must be positive");
    count
}

/// The corpus tokens are already what BM25 counts: split on whitespace, case
/// kept. Positions give the term frequencies, and `scoring` the lengths.
/// `memory` places the index once optimized: pinned in RAM, or cold on disk.
fn text_params(memory: Memory) -> TextIndexParams {
    TextIndexParams {
        tokenizer: TokenizerType::Whitespace,
        lowercase: Some(false),
        phrase_matching: Some(true),
        memory: Some(memory),
        scoring: Some(TextScoringParams::default()),
        ..TextIndexParams::default()
    }
}

/// Point `i` holds `documents[i]`, which is what lets a shard result be
/// checked against the reference by id.
fn corpus_points(documents: &[Vec<String>]) -> Vec<PointStructPersisted> {
    // Its own stream, so the corpus is drawn exactly as in the sparse baseline.
    let mut rng = SmallRng::seed_from_u64(7);
    documents
        .iter()
        .enumerate()
        .map(|(id, tokens)| PointStructPersisted {
            id: (id as u64).into(),
            vector: VectorStructInternal::from(random_vector(&mut rng, VECTOR_DIM)).into(),
            payload: Some(payload_json! { TEXT_FIELD: tokens.join(" ") }),
        })
        .collect()
}

/// Points held by appendable segments, by immutable ones, and whether an
/// optimization is in flight.
fn segment_census(shard: &LocalShard) -> (usize, usize, bool) {
    let segments = shard.segments();
    let holder = segments.read();
    let (mut appendable, mut immutable, mut has_proxy) = (0, 0, false);
    for (_, segment) in holder.iter() {
        let segment = segment.get();
        let segment = segment.read();
        if segment.segment_type() == SegmentType::Special {
            has_proxy = true;
        } else if segment.is_appendable() {
            appendable += segment.available_point_count();
        } else {
            immutable += segment.available_point_count();
        }
    }
    (appendable, immutable, has_proxy)
}

/// Block until the optimizer has moved the corpus into immutable segments,
/// failing loudly rather than timing the appendable shape twice.
fn wait_until_optimized(shard: &LocalShard, point_count: usize) {
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        let (_, immutable, has_proxy) = segment_census(shard);
        if !has_proxy && immutable >= point_count {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "optimization did not finish: {immutable} of {point_count} points are immutable",
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The counterpart: nothing may have been optimized away behind our back.
fn assert_still_appendable(shard: &LocalShard, point_count: usize) {
    let (appendable, immutable, has_proxy) = segment_census(shard);
    assert!(
        !has_proxy && immutable == 0 && appendable >= point_count,
        "the fresh shard optimized itself: {appendable} appendable, {immutable} immutable",
    );
}

/// Build a shard with a scoring text index on `TEXT_FIELD`, placed as
/// `memory` asks once optimized, holding the whole corpus.
fn shard_with(
    handle: &tokio::runtime::Handle,
    search_handle: &AdaptiveSearchHandle,
    optimizer_config: OptimizersConfig,
    memory: Memory,
    points: Vec<PointStructPersisted>,
) -> (LocalShard, tempfile::TempDir) {
    // Under `CARGO_TARGET_TMPDIR`, not the system tempdir: on a tmpfs `/tmp`
    // the on-disk index would be read from RAM and measure like the RAM one.
    let storage_dir = Builder::new()
        .prefix("bm25-text")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let schema_dir = Builder::new().prefix("bm25-schema").tempdir().unwrap();

    let collection_config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorParamsBuilder::new(VECTOR_DIM as u64, Distance::Dot)
                .build()
                .into(),
            ..CollectionParams::empty()
        },
        optimizer_config: optimizer_config.clone(),
        wal_config: WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        },
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    // The schema is what the optimizer builds the index from in the segments
    // it writes, and what the shard tokenizes the query with.
    let field: JsonPath = TEXT_FIELD.parse().unwrap();
    let field_schema =
        PayloadFieldSchema::FieldParams(PayloadSchemaParams::Text(text_params(memory)));
    let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> = Arc::new(
        SaveOnDisk::load_or_init_default(schema_dir.path().join("payload-schema.json")).unwrap(),
    );
    payload_index_schema
        .write(|schema| {
            schema.schema.insert(field.clone(), field_schema.clone());
        })
        .unwrap();

    let shard = handle
        .block_on(LocalShard::build_local(
            0,
            "bm25_text".to_string(),
            storage_dir.path(),
            Arc::new(RwLock::new(collection_config)),
            Default::default(),
            payload_index_schema,
            handle.clone(),
            search_handle.clone(),
            ResourceBudget::default(),
            optimizer_config,
        ))
        .unwrap();

    // Index first, as a user would: the fresh shard then holds the mutable
    // index built point by point, not one built over existing payloads.
    let operations = [
        CollectionUpdateOperations::FieldIndexOperation(FieldIndexOperations::CreateIndex(
            CreateIndex {
                field_name: field,
                field_schema: Some(field_schema),
            },
        )),
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
            PointInsertOperationsInternal::PointsList(points),
        )),
    ];
    for operation in operations {
        handle
            .block_on(shard.update(
                operation.into(),
                WaitUntil::Visible,
                None,
                HwMeasurementAcc::new(),
            ))
            .unwrap();
    }

    (shard, storage_dir)
}

/// One state to time.
struct State {
    name: &'static str,
    shard: LocalShard,
    _dir: tempfile::TempDir,
}

fn run_batch(
    runtime: &Runtime,
    shard: &LocalShard,
    queries: &[TextScoringQuery],
) -> Vec<Vec<ScoredPoint>> {
    runtime.block_on(async {
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            let hits = shard
                .score_bm25(
                    query,
                    None,
                    LIMIT,
                    WithPayload::from(false),
                    WithVector::from(false),
                    QUERY_TIMEOUT,
                    HwMeasurementAcc::new(),
                )
                .await
                .unwrap();
            results.push(hits);
        }
        results
    })
}

/// Mean recall at `LIMIT` of one pass over the queries against the reference.
fn recall_at_limit(results: &[Vec<ScoredPoint>], truth: &[Vec<ScoredPointOffset>]) -> f64 {
    let total: f64 = results
        .iter()
        .zip(truth)
        .map(|(hits, truth)| {
            let ids = hits.iter().map(|hit| match hit.id {
                ExtendedPointId::NumId(id) => id as PointOffsetType,
                ExtendedPointId::Uuid(_) => unreachable!("the corpus uses numeric ids"),
            });
            recall(ids, truth)
        })
        .sum();
    total / truth.len() as f64
}

fn bm25_text_bench(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let search_runtime = Runtime::new().unwrap();
    let search_handle = AdaptiveSearchHandle::new_fixed(search_runtime.handle().clone());
    let handle = runtime.handle().clone();
    let point_count = point_count();

    // The same two configs as the sparse baseline.
    let never_optimize = OptimizersConfig {
        deleted_threshold: 1.0,
        vacuum_min_vector_number: usize::MAX,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: Some(0),
        flush_interval_sec: 30,
        max_optimization_threads: Some(2),
        prevent_unoptimized: None,
    };
    let always_optimize = OptimizersConfig {
        indexing_threshold: Some(1),
        vacuum_min_vector_number: 1000,
        deleted_threshold: 0.9,
        max_segment_size: Some(1_000_000),
        ..never_optimize.clone()
    };

    // Same seed and draw order as the sparse baseline, so the same corpus.
    let mut rng = SmallRng::seed_from_u64(42);
    let vocabulary = Vocabulary::new();
    let documents: Vec<Vec<String>> = (0..point_count)
        .map(|_| vocabulary.document(&mut rng))
        .collect();
    let queries: Vec<Vec<String>> = (0..QUERY_COUNT)
        .map(|_| vocabulary.query(&mut rng))
        .collect();
    let reference = Reference::new(&documents);
    let truth: Vec<Vec<ScoredPointOffset>> =
        queries.iter().map(|q| reference.top(q, LIMIT)).collect();
    eprintln!(
        "{point_count} documents, corpus avgdl {:.1}",
        reference.avg_doc_len(),
    );

    let text_queries: Vec<TextScoringQuery> = queries
        .iter()
        .map(|tokens| TextScoringQuery {
            field: TEXT_FIELD.parse().unwrap(),
            text: tokens.join(" "),
            params: Bm25Params::default(),
        })
        .collect();
    let points = corpus_points(&documents);

    let mut states = Vec::new();

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        never_optimize,
        Memory::Pinned,
        points.clone(),
    );
    assert_still_appendable(&shard, point_count);
    states.push(State {
        name: "fresh",
        shard,
        _dir: dir,
    });

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        always_optimize.clone(),
        Memory::Pinned,
        points.clone(),
    );
    wait_until_optimized(&shard, point_count);
    states.push(State {
        name: "optimized",
        shard,
        _dir: dir,
    });

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        always_optimize,
        Memory::Cold,
        points,
    );
    wait_until_optimized(&shard, point_count);
    states.push(State {
        name: "on-disk",
        shard,
        _dir: dir,
    });

    for state in &states {
        let results = run_batch(&runtime, &state.shard, &text_queries);
        eprintln!(
            "recall@{LIMIT} {:<24} {:.3}",
            state.name,
            recall_at_limit(&results, &truth)
        );
    }

    let mut group = c.benchmark_group("bm25-text");
    for state in &states {
        group.bench_function(state.name, |b| {
            b.iter(|| run_batch(&runtime, &state.shard, &text_queries))
        });
    }
    group.finish();

    for state in &states {
        if state.name == "fresh" {
            assert_still_appendable(&state.shard, point_count);
        }
    }
}

criterion_group!(benches, bm25_text_bench);
criterion_main!(benches);
