mod fix_payload_indices;
pub mod fixtures;
mod hw_metrics;
mod payload;
mod points_dedup;
mod sha_256_test;
mod shard_query;
mod snapshot_test;
mod sparse_vectors_validation_tests;
mod wal_recovery_test;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::counter::hardware_counter::HardwareCounterCell;
use common::cpu::CpuBudget;
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use segment::data_types::vectors::only_default_vector;
use segment::index::hnsw_index::num_rayon_threads;
use segment::types::{Distance, PointIdType};
use tempfile::Builder;
use tokio::time::{sleep, Instant};

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection::Collection;
use crate::collection_manager::fixtures::{
    get_indexing_optimizer, get_merge_optimizer, random_segment, PointIdGenerator,
};
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder, SegmentId};
use crate::collection_manager::optimizers::segment_optimizer::OptimizerThresholds;
use crate::collection_manager::optimizers::TrackerStatus;
use crate::config::CollectionParams;
use crate::operations::types::VectorsConfig;
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::update_handler::{Optimizer, UpdateHandler};

#[tokio::test]
async fn test_optimization_process() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let dim = 256;
    let mut holder = SegmentHolder::default();

    let segments_to_merge = vec![
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
    ];

    let segment_to_index = holder.add_new(random_segment(dir.path(), 100, 110, dim));

    let _other_segment_ids: Vec<SegmentId> = vec![
        holder.add_new(random_segment(dir.path(), 100, 20, dim)),
        holder.add_new(random_segment(dir.path(), 100, 20, dim)),
    ];

    let merge_optimizer: Arc<Optimizer> =
        Arc::new(get_merge_optimizer(dir.path(), temp_dir.path(), dim, None));
    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![merge_optimizer, indexing_optimizer]);

    let optimizers_log = Arc::new(Mutex::new(Default::default()));
    let total_optimized_points = Arc::new(AtomicUsize::new(0));
    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &CpuBudget::default(),
        segments.clone(),
        |_| {},
        None,
    );

    // We expect a total of 2 optimizations for the above segments
    let mut total_optimizations = 2;

    // The optimizers try to saturate the CPU, as number of optimizations tasks we should therefore
    // expect the amount that would fit within our CPU budget
    // We skip optimizations that use less than half of the preferred CPU budget
    let expected_optimization_count = {
        let cpus = common::cpu::get_cpu_budget(0);
        let hnsw_threads = num_rayon_threads(0);
        (cpus / hnsw_threads + usize::from((cpus % hnsw_threads) >= hnsw_threads.div_ceil(2)))
            .clamp(1, total_optimizations)
    };

    assert_eq!(handles.len(), expected_optimization_count);
    total_optimizations -= expected_optimization_count;

    let join_res = join_all(handles.into_iter().map(|x| x.join_handle).collect_vec()).await;

    // Assert optimizer statuses are tracked properly
    {
        let log = optimizers_log.lock().to_telemetry();
        assert_eq!(log.len(), expected_optimization_count);
        log.iter().for_each(|entry| {
            assert!(["indexing", "merge"].contains(&entry.name.as_str()));
            assert_eq!(entry.status, TrackerStatus::Done);
        });
    }

    for res in join_res {
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some(true));
    }

    let handles = UpdateHandler::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &CpuBudget::default(),
        segments.clone(),
        |_| {},
        None,
    );

    // Because we may not have completed all optimizations due to limited CPU budget, we may expect
    // another round of optimizations here
    assert_eq!(
        handles.len(),
        expected_optimization_count.min(total_optimizations),
    );

    let join_res = join_all(handles.into_iter().map(|x| x.join_handle).collect_vec()).await;

    for res in join_res {
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some(true));
    }

    assert_eq!(segments.read().len(), 4);

    assert!(segments.read().get(segment_to_index).is_none());

    for sid in segments_to_merge {
        assert!(segments.read().get(sid).is_none());
    }

    assert_eq!(total_optimized_points.load(Ordering::Relaxed), 119);
}

#[tokio::test]
async fn test_cancel_optimization() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let mut holder = SegmentHolder::default();
    let dim = 256;

    for _ in 0..5 {
        holder.add_new(random_segment(dir.path(), 100, 1000, dim));
    }

    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![indexing_optimizer]);

    let now = Instant::now();

    let optimizers_log = Arc::new(Mutex::new(Default::default()));
    let total_optimized_points = Arc::new(AtomicUsize::new(0));
    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &CpuBudget::default(),
        segments.clone(),
        |_| {},
        None,
    );

    sleep(Duration::from_millis(100)).await;

    let join_handles = handles.into_iter().filter_map(|h| h.stop()).collect_vec();

    let optimization_res = join_all(join_handles).await;

    let actual_optimization_duration = now.elapsed().as_millis();
    eprintln!("actual_optimization_duration = {actual_optimization_duration:#?} ms");

    for res in optimization_res {
        let was_finished = res.expect("Should be no errors during optimization");
        assert_ne!(was_finished, Some(true));
    }

    // Assert optimizer statuses are tracked properly
    // The optimizers try to saturate the CPU, as number of optimizations tasks we should therefore
    // expect the amount that would fit within our CPU budget
    {
        // We skip optimizations that use less than half of the preferred CPU budget
        let expected_optimization_count = {
            let cpus = common::cpu::get_cpu_budget(0);
            let hnsw_threads = num_rayon_threads(0);
            (cpus / hnsw_threads + usize::from((cpus % hnsw_threads) >= hnsw_threads.div_ceil(2)))
                .clamp(1, 3)
        };

        let log = optimizers_log.lock().to_telemetry();
        assert_eq!(log.len(), expected_optimization_count);
        for status in log {
            assert_eq!(status.name, "indexing");
            assert!(matches!(status.status, TrackerStatus::Cancelled(_)));
        }
    }

    for (_idx, segment) in segments.read().iter() {
        match segment {
            LockedSegment::Original(_) => {}
            LockedSegment::Proxy(_) => panic!("segment is not restored"),
        }
    }

    assert_eq!(total_optimized_points.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn test_new_segment_when_all_over_capacity() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();

    let dim = 256;
    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(dim as u64, Distance::Dot).build()),
        ..CollectionParams::empty()
    };
    let optimizer_thresholds = OptimizerThresholds {
        max_segment_size_kb: 1,
        memmap_threshold_kb: 1_000_000,
        indexing_threshold_kb: 1_000_000,
    };
    let payload_index_schema = PayloadIndexSchema::default();
    let mut holder = SegmentHolder::default();

    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));

    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));

    // Expect our 5 created segments now
    assert_eq!(segments.read().len(), 5);

    // On optimization we expect one new segment to be created, all are over capacity
    UpdateHandler::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &collection_params,
        &optimizer_thresholds,
        &payload_index_schema,
    )
    .unwrap();
    assert_eq!(segments.read().len(), 6);

    // On reoptimization we don't expect another segment, we have one segment with capacity
    UpdateHandler::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &collection_params,
        &optimizer_thresholds,
        &payload_index_schema,
    )
    .unwrap();

    assert_eq!(segments.read().len(), 6);

    let hw_counter = HardwareCounterCell::new();

    // Insert some points in the smallest segment to fill capacity
    {
        let segments_read = segments.read();
        let (_, segment) = segments_read
            .iter()
            .min_by_key(|(_, segment)| {
                segment
                    .get()
                    .read()
                    .max_available_vectors_size_in_bytes()
                    .unwrap()
            })
            .unwrap();

        let mut rnd = rand::thread_rng();
        for _ in 0..10 {
            let point_id: PointIdType = PointIdGenerator::default().unique();
            let random_vector: Vec<_> = (0..dim).map(|_| rnd.gen()).collect();
            segment
                .get()
                .write()
                .upsert_point(
                    101,
                    point_id,
                    only_default_vector(&random_vector),
                    &hw_counter,
                )
                .unwrap();
        }
    }

    // On reoptimization we expect one more segment to be created, all are over capacity
    UpdateHandler::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &collection_params,
        &optimizer_thresholds,
        &payload_index_schema,
    )
    .unwrap();
    assert_eq!(segments.read().len(), 7);
}

#[test]
fn check_version_upgrade() {
    assert!(!Collection::can_upgrade_storage(
        &"0.3.1".parse().unwrap(),
        &"0.4.0".parse().unwrap()
    ));
    assert!(!Collection::can_upgrade_storage(
        &"0.4.0".parse().unwrap(),
        &"0.5.0".parse().unwrap()
    ));
    assert!(!Collection::can_upgrade_storage(
        &"0.4.0".parse().unwrap(),
        &"0.4.2".parse().unwrap()
    ));
    assert!(Collection::can_upgrade_storage(
        &"0.4.0".parse().unwrap(),
        &"0.4.1".parse().unwrap()
    ));
    assert!(Collection::can_upgrade_storage(
        &"0.4.1".parse().unwrap(),
        &"0.4.2".parse().unwrap()
    ));
}
