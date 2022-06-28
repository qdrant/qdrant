use collection::operations::types::{BatchSearchRequest, Query, SearchRequest};
use collection::optimizers_builder::OptimizersConfig;
use criterion::{criterion_group, criterion_main, Criterion};
use segment::types::Distance;
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
                        top: 0,
                        with_payload: None,
                        with_vector: false,
                        score_threshold: None,
                    },
                    None,
                ))
                .unwrap()
        })
    });

    group.bench_function("batch-search", |b| {
        b.iter(|| {
            handle
                .block_on(toc.search_batch(
                    "test",
                    BatchSearchRequest {
                        batch: vec![Query {
                            vector: vec![0.0, 0.0, 1.0, 1.0],
                            filter: None,
                        }],
                        params: None,
                        top: 0,
                        with_payload: None,
                        with_vector: false,
                        score_threshold: None,
                    },
                    None,
                ))
                .unwrap()
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = batch_search_benchmark,
}

criterion_main!(benches);
