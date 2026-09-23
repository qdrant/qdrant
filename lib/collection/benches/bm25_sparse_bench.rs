//! Baseline for BM25 over sparse vectors, end to end through a local shard.
//!
//! This is the number a text-index scorer has to match: same corpus, same
//! queries, no HTTP. The corpus and the queries come from
//! `segment::fixtures::bm25_corpus`, so a text-index measurement can use the
//! same documents.
//!
//! Three shard states, one per sparse index shape: freshly ingested, where the
//! index is the appendable RAM one; optimized into the immutable RAM index; and
//! optimized with the index placed on disk. The sparse shapes measure within a
//! few percent of each other; the states exist because the text index they are
//! compared against differs a lot more between its own shapes.
//!
//! Latency is not the whole baseline. `lib/bm25` embeds documents with a fixed
//! average length, 256 by default, while this corpus averages about 110 tokens,
//! and that constant is baked into every stored vector. Against BM25 by
//! definition the default embedding recalls about three quarters of the true
//! top 10; set to the corpus average it recalls all of it. So the recall of
//! every timed state is printed before the timings, and the optimized state is
//! also timed with the constant set to the corpus average, which is the
//! like-for-like baseline for a scorer that reads `avgdl` from the data.
//!
//! `BM25_SPARSE_DOCS` overrides the document count. The default is 200k: at
//! 20k every shape of both routes measures the same and half of a shard-level
//! query is the shard rather than the scoring. Before #10682 this size was
//! unreachable through the shard, since every sparse upsert rewrote
//! `max_next_weight` to the head of its posting lists and a term present in
//! most documents made ingestion quadratic.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::rest::{NamedVectorStruct, SearchRequestInternal};
use bm25::{Bm25, Bm25Params};
use collection::common::adaptive_handle::AdaptiveSearchHandle;
use collection::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use collection::operations::CollectionUpdateOperations;
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorPersisted,
    VectorStructPersisted,
};
use collection::operations::types::{SparseIndexParams, SparseVectorParams};
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
use segment::data_types::modifier::Modifier;
use segment::data_types::vectors::NamedSparseVector;
use segment::fixtures::bm25_corpus::{LIMIT, QUERY_COUNT, Reference, Vocabulary, recall};
use segment::types::{ExtendedPointId, Memory, ScoredPoint, SegmentType};
use shard::search::CoreSearchRequestBatch;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

const SPARSE_VECTOR_NAME: &str = "text";
const DEFAULT_POINT_COUNT: usize = 200_000;

fn point_count() -> usize {
    let count = std::env::var("BM25_SPARSE_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_POINT_COUNT);
    // An empty corpus has no average length, and `Bm25::new` would reject the
    // NaN with an unwrap panic that does not name the variable to fix.
    assert!(count > 0, "BM25_SPARSE_DOCS must be positive");
    count
}

fn to_sparse(embedding: bm25::SparseEmbedding) -> SparseVector {
    SparseVector::new(embedding.indices, embedding.values).expect("valid sparse vector")
}

fn embed_document(bm25: &Bm25, tokens: &[String]) -> SparseVector {
    let tokens: Vec<_> = tokens.iter().map(|t| t.as_str().into()).collect();
    to_sparse(bm25.embed_document(&tokens))
}

fn embed_query(bm25: &Bm25, tokens: &[String]) -> SparseVector {
    let tokens: Vec<_> = tokens.iter().map(|t| t.as_str().into()).collect();
    to_sparse(bm25.embed_query(&tokens))
}

/// Embed the corpus the way the production route does: the same crate, so the
/// baseline measures the route a user is actually on. Point `i` holds
/// `documents[i]`, which is what lets a shard result be checked against the
/// reference by id.
fn corpus_points(bm25: &Bm25, documents: &[Vec<String>]) -> Vec<PointStructPersisted> {
    documents
        .iter()
        .enumerate()
        .map(|(id, tokens)| PointStructPersisted {
            id: (id as u64).into(),
            vector: VectorStructPersisted::Named(HashMap::from([(
                SPARSE_VECTOR_NAME.to_owned(),
                VectorPersisted::Sparse(embed_document(bm25, tokens)),
            )])),
            payload: None,
        })
        .collect()
}

fn search_request(vector: SparseVector) -> SearchRequestInternal {
    SearchRequestInternal {
        vector: NamedVectorStruct::Sparse(NamedSparseVector {
            name: SPARSE_VECTOR_NAME.to_owned(),
            vector,
        }),
        filter: None,
        params: None,
        limit: LIMIT,
        offset: None,
        with_payload: None,
        with_vector: None,
        score_threshold: None,
    }
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

/// Block until the optimizer has moved the corpus into immutable segments.
///
/// Measuring the optimized state against a shard that never optimized would
/// report the appendable numbers twice, so this fails loudly rather than
/// timing out quietly.
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
///
/// Each state is measured on its own shard rather than before and after on
/// one, because a shard that will optimize starts doing it the moment the
/// upsert lands, and criterion would then time a half-converted index and
/// call it fresh.
fn assert_still_appendable(shard: &LocalShard, point_count: usize) {
    let (appendable, immutable, has_proxy) = segment_census(shard);
    assert!(
        !has_proxy && immutable == 0 && appendable >= point_count,
        "the fresh shard optimized itself: {appendable} appendable, {immutable} immutable",
    );
}

/// Build a shard holding the whole corpus under the given optimizer config,
/// with the sparse index placed as `memory` asks (`None` keeps the default,
/// the RAM index).
fn shard_with(
    handle: &tokio::runtime::Handle,
    search_handle: &AdaptiveSearchHandle,
    optimizer_config: OptimizersConfig,
    memory: Option<Memory>,
    points: Vec<PointStructPersisted>,
) -> (LocalShard, tempfile::TempDir) {
    // Under `CARGO_TARGET_TMPDIR`, not the system tempdir: on a tmpfs `/tmp`
    // the on-disk index would be read from RAM and measure like the RAM one.
    let storage_dir = Builder::new()
        .prefix("bm25-sparse")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap();
    let schema_dir = Builder::new().prefix("bm25-schema").tempdir().unwrap();

    let collection_params = CollectionParams {
        sparse_vectors: Some(BTreeMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorParams {
                index: memory.map(|memory| SparseIndexParams {
                    memory: Some(memory),
                    ..SparseIndexParams::default()
                }),
                // What makes this the BM25 route rather than a dot product: the
                // term frequencies are baked into the stored vectors and IDF is
                // applied to the query at search time.
                modifier: Some(Modifier::Idf),
            },
        )])),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfigInternal {
        params: collection_params,
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

    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(schema_dir.path().join("payload-schema.json")).unwrap(),
    );

    let shard = handle
        .block_on(LocalShard::build_local(
            0,
            "bm25_sparse".to_string(),
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

    handle
        .block_on(
            shard.update(
                CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                    PointInsertOperationsInternal::PointsList(points),
                ))
                .into(),
                WaitUntil::Visible,
                None,
                HwMeasurementAcc::new(),
            ),
        )
        .unwrap();

    (shard, storage_dir)
}

/// One state to time: the shard and its query vectors.
struct State {
    name: &'static str,
    shard: LocalShard,
    queries: Vec<SparseVector>,
    _dir: tempfile::TempDir,
}

fn run_batch(
    runtime: &Runtime,
    search_handle: &AdaptiveSearchHandle,
    shard: &LocalShard,
    queries: &[SparseVector],
) -> Vec<Vec<ScoredPoint>> {
    runtime.block_on(async {
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            let request = search_request(query.clone());
            let mut batch = shard
                .core_search(
                    Arc::new(CoreSearchRequestBatch {
                        searches: vec![request.into()],
                    }),
                    search_handle,
                    None,
                    HwMeasurementAcc::new(),
                )
                .await
                .unwrap();
            results.push(batch.remove(0));
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

fn bm25_sparse_bench(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let search_runtime = Runtime::new().unwrap();
    let search_handle = AdaptiveSearchHandle::new_fixed(search_runtime.handle().clone());
    let handle = runtime.handle().clone();
    let point_count = point_count();

    // Nothing for the optimizer to convert: one segment that already holds
    // everything, indexing disabled (`None` would be the 10 MB default, which
    // the corpus crosses), no vacuum.
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

    // The route as shipped: `lib/bm25` defaults, average length 256.
    let default = Bm25::new(Bm25Params::default()).unwrap();
    // The same route told the truth about the corpus.
    let tuned = Bm25::new(Bm25Params {
        avg_doc_len: reference.avg_doc_len(),
        ..Bm25Params::default()
    })
    .unwrap();
    eprintln!(
        "{point_count} documents, corpus avgdl {:.1}, embedded avgdl {:.0} (default) and {:.1} (tuned)",
        reference.avg_doc_len(),
        Bm25Params::DEFAULT_AVG_DOC_LEN,
        reference.avg_doc_len(),
    );

    let default_points = corpus_points(&default, &documents);
    let default_queries: Vec<SparseVector> =
        queries.iter().map(|q| embed_query(&default, q)).collect();
    let tuned_queries: Vec<SparseVector> = queries.iter().map(|q| embed_query(&tuned, q)).collect();

    let mut states = Vec::new();

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        never_optimize.clone(),
        None,
        default_points.clone(),
    );
    assert_still_appendable(&shard, point_count);
    states.push(State {
        name: "fresh",
        shard,
        queries: default_queries.clone(),
        _dir: dir,
    });

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        always_optimize.clone(),
        None,
        default_points.clone(),
    );
    wait_until_optimized(&shard, point_count);
    states.push(State {
        name: "optimized",
        shard,
        queries: default_queries.clone(),
        _dir: dir,
    });

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        always_optimize.clone(),
        Some(Memory::Cold),
        default_points,
    );
    wait_until_optimized(&shard, point_count);
    states.push(State {
        name: "on-disk",
        shard,
        queries: default_queries,
        _dir: dir,
    });

    let (shard, dir) = shard_with(
        &handle,
        &search_handle,
        always_optimize,
        None,
        corpus_points(&tuned, &documents),
    );
    wait_until_optimized(&shard, point_count);
    states.push(State {
        name: "optimized-corpus-avgdl",
        shard,
        queries: tuned_queries,
        _dir: dir,
    });

    // What each timed state actually returns, against BM25 by definition.
    for state in &states {
        let results = run_batch(&runtime, &search_handle, &state.shard, &state.queries);
        eprintln!(
            "recall@{LIMIT} {:<24} {:.3}",
            state.name,
            recall_at_limit(&results, &truth)
        );
    }

    let mut group = c.benchmark_group("bm25-sparse");
    for state in &states {
        group.bench_function(state.name, |b| {
            b.iter(|| run_batch(&runtime, &search_handle, &state.shard, &state.queries))
        });
    }
    group.finish();

    // The fresh numbers only mean something if the shard stayed fresh while
    // it was timed.
    for state in &states {
        if state.name == "fresh" {
            assert_still_appendable(&state.shard, point_count);
        }
    }
}

criterion_group!(benches, bm25_sparse_bench);
criterion_main!(benches);
