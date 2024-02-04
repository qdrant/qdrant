#[cfg(not(target_os = "windows"))]
mod prof;

use std::num::NonZeroU64;
use std::sync::Arc;

use collection::config::{CollectionConfig, CollectionParams, WalConfig};
use collection::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStruct,
};
use collection::operations::types::{CoreSearchRequestBatch, SearchRequestInternal, VectorParams};
use collection::operations::CollectionUpdateOperations;
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::local_shard::LocalShard;
use collection::shards::shard_trait::ShardOperation;
use common::cpu::CpuBudget;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::thread_rng;
use segment::data_types::vectors::only_default_vector;
use segment::fixtures::payload_fixtures::random_vector;
use segment::types::{Condition, Distance, FieldCondition, Filter, Payload, Range};
use serde_json::Map;
use tempfile::Builder;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

fn create_rnd_batch() -> CollectionUpdateOperations {
    let mut rng = thread_rng();
    let num_points = 2000;
    let dim = 100;
    let mut points = Vec::with_capacity(num_points);
    for i in 0..num_points {
        let mut payload_map = Map::new();
        payload_map.insert("a".to_string(), (i % 5).into());
        let vector = random_vector(&mut rng, dim);
        let vectors = only_default_vector(&vector);
        let point = PointStruct {
            id: (i as u64).into(),
            vector: vectors.into(),
            payload: Some(Payload(payload_map)),
        };
        points.push(point);
    }
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ))
}

fn batch_search_bench(c: &mut Criterion) {
    let storage_dir = Builder::new().prefix("storage").tempdir().unwrap();

    let runtime = Runtime::new().unwrap();
    let search_runtime = Runtime::new().unwrap();
    let search_runtime_handle = search_runtime.handle();
    let handle = runtime.handle().clone();

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
    };

    let collection_params = CollectionParams {
        vectors: VectorParams {
            size: NonZeroU64::new(100).unwrap(),
            distance: Distance::Dot,
            hnsw_config: None,
            quantization_config: None,
            on_disk: None,
        }
        .into(),
        ..CollectionParams::empty()
    };

    let collection_config = CollectionConfig {
        params: collection_params,
        optimizer_config: OptimizersConfig {
            deleted_threshold: 0.9,
            vacuum_min_vector_number: 1000,
            default_segment_number: 2,
            max_segment_size: Some(100_000),
            memmap_threshold: Some(100_000),
            indexing_threshold: Some(50_000),
            flush_interval_sec: 30,
            max_optimization_threads: 2,
        },
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
    };

    let shared_config = Arc::new(RwLock::new(collection_config));

    let shard = handle
        .block_on(LocalShard::build_local(
            0,
            "test_collection".to_string(),
            storage_dir.path(),
            shared_config,
            Default::default(),
            handle.clone(),
            CpuBudget::default(),
        ))
        .unwrap();

    let rnd_batch = create_rnd_batch();

    handle
        .block_on(shard.update(rnd_batch.into(), true))
        .unwrap();

    let mut group = c.benchmark_group("batch-search-bench");

    let filters = vec![
        None,
        Some(Filter::new_must(Condition::Field(
            FieldCondition::new_match("a".to_string(), 3.into()),
        ))),
        Some(Filter::new_must(Condition::Field(
            FieldCondition::new_range(
                "a".to_string(),
                Range {
                    lt: None,
                    gt: Some(-1.),
                    gte: None,
                    lte: Some(100.0),
                },
            ),
        ))),
    ];

    let batch_size = 100;

    for (fid, filter) in filters.into_iter().enumerate() {
        group.bench_function(format!("search-{fid}"), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let mut rng = thread_rng();
                    for _i in 0..batch_size {
                        let query = random_vector(&mut rng, 100);
                        let search_query = SearchRequestInternal {
                            vector: query.into(),
                            filter: filter.clone(),
                            params: None,
                            limit: 10,
                            offset: None,
                            with_payload: None,
                            with_vector: None,
                            score_threshold: None,
                        };
                        let result = shard
                            .core_search(
                                Arc::new(CoreSearchRequestBatch {
                                    searches: vec![search_query.into()],
                                }),
                                search_runtime_handle,
                                None,
                            )
                            .await
                            .unwrap();
                        assert!(!result.is_empty());
                    }
                });
            })
        });

        group.bench_function(format!("search-batch-{fid}"), |b| {
            b.iter(|| {
                runtime.block_on(async {
                    let mut rng = thread_rng();
                    let mut searches = Vec::with_capacity(batch_size);
                    for _i in 0..batch_size {
                        let query = random_vector(&mut rng, 100);
                        let search_query = SearchRequestInternal {
                            vector: query.into(),
                            filter: filter.clone(),
                            params: None,
                            limit: 10,
                            offset: None,
                            with_payload: None,
                            with_vector: None,
                            score_threshold: None,
                        };
                        searches.push(search_query.into());
                    }

                    let search_query = CoreSearchRequestBatch { searches };
                    let result = shard
                        .core_search(Arc::new(search_query), search_runtime_handle, None)
                        .await
                        .unwrap();
                    assert!(!result.is_empty());
                });
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = batch_search_bench,
}

criterion_main!(benches);
