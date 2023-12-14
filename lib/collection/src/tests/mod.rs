mod sha_256_test;
mod snapshot_test;
mod sparse_vectors_validation_tests;
mod wal_recovery_test;

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use segment::index::hnsw_index::max_rayon_threads;
use tempfile::Builder;
use tokio::time::{sleep, Instant};

use crate::collection::Collection;
use crate::collection_manager::fixtures::{
    get_indexing_optimizer, get_merge_optimizer, random_segment,
};
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder, SegmentId};
use crate::collection_manager::optimizers::TrackerStatus;
use crate::update_handler::{Optimizer, UpdateHandler};

#[tokio::test]
async fn test_optimization_process() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let dim = 256;
    let mut holder = SegmentHolder::default();

    let segments_to_merge = vec![
        holder.add(random_segment(dir.path(), 100, 3, dim)),
        holder.add(random_segment(dir.path(), 100, 3, dim)),
        holder.add(random_segment(dir.path(), 100, 3, dim)),
    ];

    let segment_to_index = holder.add(random_segment(dir.path(), 100, 110, dim));

    let _other_segment_ids: Vec<SegmentId> = vec![
        holder.add(random_segment(dir.path(), 100, 20, dim)),
        holder.add(random_segment(dir.path(), 100, 20, dim)),
    ];

    let merge_optimizer: Arc<Optimizer> =
        Arc::new(get_merge_optimizer(dir.path(), temp_dir.path(), dim));
    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![merge_optimizer, indexing_optimizer]);

    let optimizers_log = Arc::new(Mutex::new(Default::default()));
    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        segments.clone(),
        |_| {},
        None,
    );

    // We expect a total of 2 optimizations for the above segments
    let mut total_optimizations = 2;

    // The optimizers try to saturate the CPU, as number of optimizat tasks we should therefore
    // expect the amount that would fit within our CPU budget
    let expected_optimization_count = common::cpu::get_cpu_budget()
        .div_ceil(max_rayon_threads(0))
        .clamp(1, 2);

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
}

#[tokio::test]
async fn test_cancel_optimization() {
    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let mut holder = SegmentHolder::default();
    let dim = 256;

    for _ in 0..5 {
        holder.add(random_segment(dir.path(), 100, 1000, dim));
    }

    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));

    let optimizers = Arc::new(vec![indexing_optimizer]);

    let now = Instant::now();

    let optimizers_log = Arc::new(Mutex::new(Default::default()));
    let segments: Arc<RwLock<_>> = Arc::new(RwLock::new(holder));
    let handles = UpdateHandler::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
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
    // The optimizers try to saturate the CPU, as number of optimizat tasks we should therefore
    // expect the amount that would fit within our CPU budget
    {
        let expected_optimization_count = common::cpu::get_cpu_budget()
            .div_ceil(max_rayon_threads(0))
            .clamp(1, 3);

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
