use collection::operations::point_ops::{PointInsertOperations, PointOperations, PointStruct};
use collection::operations::types::{BatchSearchRequest, Query, SearchRequest};
use collection::operations::CollectionUpdateOperations;
use collection::optimizers_builder::OptimizersConfig;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::thread_rng;
use segment::fixtures::payload_fixtures::random_vector;
use segment::types::{Distance, PointIdType};
use std::sync::Arc;
use storage::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollection, CreateCollectionOperation,
};
use storage::content_manager::toc::TableOfContent;
use storage::types::{PerformanceConfig, StorageConfig};
use tempdir::TempDir;
use tokio::runtime::Runtime;

fn batch_search_benchmark(c: &mut Criterion) {
    let storage_dir = TempDir::new("storage").unwrap();

    let config = StorageConfig {
        storage_path: storage_dir.path().to_str().unwrap().to_string(),
        on_disk_payload: false,
        optimizers: OptimizersConfig {
            deleted_threshold: 0.5,
            vacuum_min_vector_number: 100,
            default_segment_number: 2,
            max_segment_size: 100_000,
            memmap_threshold: 100,
            indexing_threshold: 100,
            flush_interval_sec: 2,
            max_optimization_threads: 2,
        },
        wal: Default::default(),
        performance: PerformanceConfig {
            max_search_threads: 1,
        },
        hnsw_index: Default::default(),
    };

    let runtime = Runtime::new().unwrap();
    let handle = runtime.handle().clone();

    let toc = Arc::new(TableOfContent::new(&config, runtime, Default::default(), 0));

    handle
        .block_on(
            toc.perform_collection_meta_op(CollectionMetaOperations::CreateCollection(
                CreateCollectionOperation {
                    collection_name: "test".to_string(),
                    create_collection: CreateCollection {
                        vector_size: 4,
                        distance: Distance::Cosine,
                        hnsw_config: None,
                        wal_config: None,
                        optimizers_config: None,
                        shard_number: Some(1),
                        on_disk_payload: None,
                    },
                },
            )),
        )
        .unwrap();

    let rnd_batch = create_rnd_batch();

    handle
        .block_on(toc.update(
            "test",
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperations::PointsList(rnd_batch),
            )),
            None,
            true,
        ))
        .unwrap();

    let mut group = c.benchmark_group("batch-search-bench");

    group.bench_function("search", |b| {
        b.iter(|| {
            handle
                .block_on(toc.search(
                    "test",
                    SearchRequest {
                        vector: vec![0.0, 0.0, 1.0, 1.0],
                        filter: None,
                        params: None,
                        limit: 0,
                        offset: 0,
                        with_payload: None,
                        with_vector: false,
                        score_threshold: None,
                    },
                    None,
                ))
                .unwrap()
        })
    });

    let batch_query = create_rnd_batch_query();

    group.bench_function("batch-search", |b| {
        b.iter(|| {
            handle
                .block_on(toc.search_batch(
                    "test",
                    BatchSearchRequest {
                        batch: batch_query.clone(),
                        params: None,
                        with_payload: None,
                        with_vector: false,
                        score_threshold: None,
                        offset: 0,
                        limit: 0,
                    },
                    None,
                ))
                .unwrap()
        })
    });

    group.finish();
}

fn create_rnd_batch() -> Vec<PointStruct> {
    let mut rnd = thread_rng();
    (0..1000)
        .into_iter()
        .map(|n| PointStruct {
            id: PointIdType::NumId(n),
            vector: random_vector(&mut rnd, 4),
            payload: None,
        })
        .collect_vec()
}

fn create_rnd_batch_query() -> Vec<Query> {
    let mut rnd = thread_rng();
    (0..100)
        .into_iter()
        .map(|_| Query {
            vector: random_vector(&mut rnd, 4),
            filter: None,
        })
        .collect_vec()
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = batch_search_benchmark,
}

criterion_main!(benches);
