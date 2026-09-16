//! Baseline for BM25 over sparse vectors, end to end through a local shard.
//!
//! This is the number a text-index scorer has to match: same corpus, same
//! queries, no HTTP. Two states, because the two sparse backends have different
//! asymptotics: freshly ingested, where the index is appendable, and optimized,
//! where it is immutable.

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
use collection::operations::types::SparseVectorParams;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::local_shard::LocalShard;
use collection::shards::shard_trait::{ShardOperation, WaitUntil};
use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use criterion::{Criterion, criterion_group, criterion_main};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use segment::data_types::modifier::Modifier;
use segment::data_types::vectors::NamedSparseVector;
use segment::types::SegmentType;
use shard::search::CoreSearchRequestBatch;
use sparse::common::sparse_vector::SparseVector;
use tempfile::Builder;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

const SPARSE_VECTOR_NAME: &str = "text";

const POINT_COUNT: usize = 20_000;
const VOCAB_SIZE: usize = 20_000;
const DOC_LEN: std::ops::RangeInclusive<usize> = 20..=200;
const QUERY_TERMS: std::ops::RangeInclusive<usize> = 2..=5;
const QUERY_COUNT: usize = 50;
const LIMIT: usize = 10;

/// A vocabulary with a Zipf-like frequency distribution: term `i` is drawn with
/// weight `1/(i+1)^0.9`, so a handful of terms appear in most documents and the
/// tail appears in almost none.
///
/// The shape matters more than the words. On a uniform vocabulary every term is
/// equally selective, IDF is flat and pruning has nothing to prune, so a
/// baseline measured there would flatter any scorer.
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

fn to_sparse(embedding: bm25::SparseEmbedding) -> SparseVector {
    SparseVector::new(embedding.indices, embedding.values).expect("valid sparse vector")
}

/// Embed the corpus the way the production route does: the same crate, the same
/// defaults, so the baseline measures the route a user is actually on.
fn corpus_points(
    bm25: &Bm25,
    vocabulary: &Vocabulary,
    rng: &mut SmallRng,
) -> Vec<PointStructPersisted> {
    (0..POINT_COUNT)
        .map(|id| {
            let tokens = vocabulary.document(rng);
            let tokens: Vec<_> = tokens.iter().map(|t| t.as_str().into()).collect();
            PointStructPersisted {
                id: (id as u64).into(),
                vector: VectorStructPersisted::Named(HashMap::from([(
                    SPARSE_VECTOR_NAME.to_owned(),
                    VectorPersisted::Sparse(to_sparse(bm25.embed_document(&tokens))),
                )])),
                payload: None,
            }
        })
        .collect()
}

fn queries(bm25: &Bm25, vocabulary: &Vocabulary, rng: &mut SmallRng) -> Vec<SparseVector> {
    (0..QUERY_COUNT)
        .map(|_| {
            let len = rng.random_range(QUERY_TERMS);
            let terms: Vec<String> = (0..len).map(|_| vocabulary.term(rng)).collect();
            let terms: Vec<_> = terms.iter().map(|t| t.as_str().into()).collect();
            to_sparse(bm25.embed_query(&terms))
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
fn wait_until_optimized(shard: &LocalShard) {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let (_, immutable, has_proxy) = segment_census(shard);
        if !has_proxy && immutable >= POINT_COUNT {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "optimization did not finish: {immutable} of {POINT_COUNT} points are immutable",
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The counterpart: nothing may have been optimized away behind our back.
///
/// The two states are measured on two shards rather than one before and one
/// after, because a shard that will optimize starts doing it the moment the
/// upsert lands, and criterion would then time a half-converted index and
/// call it fresh.
fn assert_still_appendable(shard: &LocalShard) {
    let (appendable, immutable, has_proxy) = segment_census(shard);
    assert!(
        !has_proxy && immutable == 0 && appendable >= POINT_COUNT,
        "the fresh shard optimized itself: {appendable} appendable, {immutable} immutable",
    );
}

/// Build a shard holding the whole corpus under the given optimizer config.
fn shard_with(
    handle: &tokio::runtime::Handle,
    search_handle: &AdaptiveSearchHandle,
    optimizer_config: OptimizersConfig,
    points: Vec<PointStructPersisted>,
) -> (LocalShard, tempfile::TempDir) {
    let storage_dir = Builder::new().prefix("bm25-sparse").tempdir().unwrap();
    let schema_dir = Builder::new().prefix("bm25-schema").tempdir().unwrap();

    let collection_params = CollectionParams {
        sparse_vectors: Some(BTreeMap::from([(
            SPARSE_VECTOR_NAME.to_owned(),
            SparseVectorParams {
                index: None,
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

fn bm25_sparse_bench(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let search_runtime = Runtime::new().unwrap();
    let search_handle = AdaptiveSearchHandle::new_fixed(search_runtime.handle().clone());
    let handle = runtime.handle().clone();

    // Nothing for the optimizer to convert: one segment that already holds
    // everything, no indexing threshold to cross, no vacuum.
    let never_optimize = OptimizersConfig {
        deleted_threshold: 1.0,
        vacuum_min_vector_number: usize::MAX,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        indexing_threshold: None,
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
    let bm25 = Bm25::new(Bm25Params::default()).unwrap();
    let points = corpus_points(&bm25, &vocabulary, &mut rng);
    let queries = queries(&bm25, &vocabulary, &mut rng);

    let (fresh, _fresh_dir) = shard_with(&handle, &search_handle, never_optimize, points.clone());
    assert_still_appendable(&fresh);

    let (optimized, _optimized_dir) = shard_with(&handle, &search_handle, always_optimize, points);
    wait_until_optimized(&optimized);

    let run_batch = |shard: &LocalShard, queries: &[SparseVector]| {
        runtime.block_on(async {
            for query in queries {
                let request = search_request(query.clone());
                shard
                    .core_search(
                        Arc::new(CoreSearchRequestBatch {
                            searches: vec![request.into()],
                        }),
                        &search_handle,
                        None,
                        HwMeasurementAcc::new(),
                    )
                    .await
                    .unwrap();
            }
        })
    };

    let mut group = c.benchmark_group("bm25-sparse");
    group.bench_function("fresh", |b| b.iter(|| run_batch(&fresh, &queries)));
    group.bench_function("optimized", |b| b.iter(|| run_batch(&optimized, &queries)));
    group.finish();
}

criterion_group!(benches, bm25_sparse_bench);
criterion_main!(benches);
