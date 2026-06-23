mod deferred_points_dedup;
mod deferred_points_tests;
mod delete_vector_name_replay;
mod facet;
mod fix_payload_indices;
pub mod fixtures;
mod hw_metrics;
mod payload;
mod points_dedup;
mod query_prefetch_offset_limit;
mod segment_manifest;
mod sha_256_test;
mod shard_query;
mod shard_telemetry;
mod snapshot_test;
mod sparse_vectors_validation_tests;
mod wal_recovery_test;

use std::assert_matches;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_counter::HardwareCounterCell;
use common::save_on_disk::SaveOnDisk;
use futures::future::join_all;
use itertools::Itertools;
use parking_lot::Mutex;
use rand::RngExt;
use segment::data_types::vectors::only_default_vector;
use segment::index::hnsw_index::get_num_indexing_threads;
use segment::types::{Distance, PointIdType};
use shard::operations::optimization::OptimizerThresholds;
use shard::segment_holder::locked::LockedSegmentHolder;
use tempfile::Builder;
use tokio::time::{Instant, sleep};

use crate::collection::Collection;
use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::collection_manager::fixtures::{
    PointIdGenerator, get_indexing_optimizer, get_merge_optimizer, random_segment,
};
use crate::collection_manager::holders::segment_holder::{LockedSegment, SegmentHolder, SegmentId};
use crate::collection_manager::optimizers::TrackerStatus;
use crate::config::CollectionParams;
use crate::operations::types::VectorsConfig;
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::optimizers_builder::build_segment_optimizer_config;
use crate::update_handler::Optimizer;
use crate::update_workers::UpdateWorkers;

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
    let segments = LockedSegmentHolder::new(holder);
    let handles = UpdateWorkers::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &ResourceBudget::default(),
        segments.clone(),
        || {},
        None,
    );

    // We expect a total of 2 optimizations for the above segments
    let mut total_optimizations = 2;

    // The optimizers try to saturate the CPU, as number of optimizations tasks we should therefore
    // expect the amount that would fit within our CPU budget
    // We skip optimizations that use less than half of the preferred CPU budget
    let expected_optimization_count = {
        let cpus = common::cpu::get_cpu_budget(0);
        let hnsw_threads = get_num_indexing_threads(0);
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
            assert!(["indexing", "merge"].contains(&entry.name));
            assert_eq!(entry.status, TrackerStatus::Done);
        });
    }

    for res in join_res {
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), Some(true));
    }

    let handles = UpdateWorkers::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &ResourceBudget::default(),
        segments.clone(),
        || {},
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

/// Regression test for the manifest safety invariant: the on-disk segment manifest must never
/// reference a segment that has been deleted from disk while its replacement is unregistered (that
/// would lose data when loading a read replica from that moment).
///
/// During a merge optimization the input segments are deleted from disk and replaced by a new
/// optimized segment. The holder drops the superseded segments from the manifest as part of the
/// swap, and `finish_optimization` confirms the manifest is in sync *before* deleting them from
/// disk, so afterwards the persisted manifest must list exactly the live segment set — never a
/// merged-away segment. This test runs the optimization directly (no worker loop, so the lazy
/// backstop never runs); the holder's in-place manifest maintenance is the only thing keeping it
/// correct here.
#[tokio::test]
async fn optimization_keeps_manifest_consistent_with_live_segments() {
    use shard::segment_manifest::SegmentsManifest;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

    let dim = 256;
    let mut holder = SegmentHolder::default();

    let segments_to_merge = [
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
        holder.add_new(random_segment(dir.path(), 100, 3, dim)),
    ];
    let segment_to_index = holder.add_new(random_segment(dir.path(), 100, 110, dim));
    let _other_segment_ids = [
        holder.add_new(random_segment(dir.path(), 100, 20, dim)),
        holder.add_new(random_segment(dir.path(), 100, 20, dim)),
    ];

    // UUIDs of the segments that will be optimized away and deleted from disk.
    let optimized_away_uuids = segments_to_merge
        .iter()
        .chain(std::iter::once(&segment_to_index))
        .map(|&sid| holder.get(sid).unwrap().get().read().segment_uuid())
        .collect_vec();

    // Initialize the manifest from the starting segment set and attach it to the holder, like a
    // freshly built shard would. From here on the holder keeps it in sync automatically.
    let manifest_path = dir.path().join("manifest.json");
    let manifest = Arc::new(
        SaveOnDisk::new(
            manifest_path.clone(),
            SegmentsManifest::from_segment_holder(&holder),
        )
        .unwrap(),
    );
    holder.set_segment_manifest(Some(manifest));

    let merge_optimizer: Arc<Optimizer> =
        Arc::new(get_merge_optimizer(dir.path(), temp_dir.path(), dim, None));
    let indexing_optimizer: Arc<Optimizer> =
        Arc::new(get_indexing_optimizer(dir.path(), temp_dir.path(), dim));
    let optimizers = Arc::new(vec![merge_optimizer, indexing_optimizer]);

    let optimizers_log = Arc::new(Mutex::new(Default::default()));
    let total_optimized_points = Arc::new(AtomicUsize::new(0));
    let segments = LockedSegmentHolder::new(holder);

    // Drain all scheduled optimizations (may take several rounds under a tight CPU budget).
    loop {
        let handles = UpdateWorkers::launch_optimization(
            optimizers.clone(),
            optimizers_log.clone(),
            total_optimized_points.clone(),
            &ResourceBudget::default(),
            segments.clone(),
            || {},
            None,
        );
        if handles.is_empty() {
            break;
        }
        let join_res = join_all(handles.into_iter().map(|x| x.join_handle).collect_vec()).await;
        for res in join_res {
            assert!(res.is_ok());
        }
    }

    // Sanity: an optimization actually happened, otherwise the test is vacuous.
    assert!(total_optimized_points.load(Ordering::Relaxed) > 0);

    // The persisted manifest must reflect exactly the live segment set: never missing a live
    // segment, never referencing a deleted one.
    let persisted: SegmentsManifest = common::fs::read_json(&manifest_path).unwrap();
    let live = SegmentsManifest::from_segment_holder(&segments.read());
    assert_eq!(
        persisted, live,
        "persisted manifest must match the live segment set after optimization",
    );

    // Explicitly assert the optimized-away segments (deleted from disk) are not referenced.
    for uuid in &optimized_away_uuids {
        assert!(
            persisted.get(uuid).is_none(),
            "manifest must not reference an optimized-away (deleted) segment {uuid}",
        );
    }
    assert!(!persisted.is_empty());
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
    let segments = LockedSegmentHolder::new(holder);
    let handles = UpdateWorkers::launch_optimization(
        optimizers.clone(),
        optimizers_log.clone(),
        total_optimized_points.clone(),
        &ResourceBudget::default(),
        segments.clone(),
        || {},
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
            let hnsw_threads = get_num_indexing_threads(0);
            (cpus / hnsw_threads + usize::from((cpus % hnsw_threads) >= hnsw_threads.div_ceil(2)))
                .clamp(1, 3)
        };

        let log = optimizers_log.lock().to_telemetry();
        assert!(log.len() <= expected_optimization_count);
        for status in log {
            assert_eq!(status.name, "indexing");
            assert_matches!(status.status, TrackerStatus::Cancelled(_));
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
        deferred_internal_id: None,
    };
    let hnsw_config = Default::default();
    let segment_config =
        build_segment_optimizer_config(&collection_params, &hnsw_config, &Default::default());

    let payload_schema_file = dir.path().join("payload.schema");
    let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    let mut holder = SegmentHolder::default();

    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));
    holder.add_new(random_segment(dir.path(), 100, 3, dim));

    let segments = LockedSegmentHolder::new(holder);

    // Expect our 5 created segments now
    assert_eq!(segments.read().len(), 5);

    // On optimization we expect one new segment to be created, all are over capacity
    UpdateWorkers::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &segment_config,
        &optimizer_thresholds,
        payload_index_schema.clone(),
    )
    .unwrap();
    assert_eq!(segments.read().len(), 6);

    // On reoptimization we don't expect another segment, we have one segment with capacity
    UpdateWorkers::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &segment_config,
        &optimizer_thresholds,
        payload_index_schema.clone(),
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

        let mut rnd = rand::rng();
        for _ in 0..10 {
            let point_id: PointIdType = PointIdGenerator::default().unique();
            let random_vector: Vec<_> = (0..dim).map(|_| rnd.random()).collect();
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
    UpdateWorkers::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &segment_config,
        &optimizer_thresholds,
        payload_index_schema,
    )
    .unwrap();
    assert_eq!(segments.read().len(), 7);
}

/// Regression test: when a new appendable segment is created at runtime because all existing
/// appendable segments are over capacity, it must be registered in the manifest *before* it
/// becomes a live write target. The holder does this automatically when the segment is added; this
/// test calls `ensure_appendable_segment_with_capacity` in isolation (no worker loop, so the lazy
/// backstop never runs), so the holder's in-place registration is the only thing keeping it correct.
#[tokio::test]
async fn ensure_appendable_segment_registers_in_manifest() {
    use std::collections::HashSet;

    use shard::segment_manifest::SegmentsManifest;

    let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
    let dim = 256;
    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(dim as u64, Distance::Dot).build()),
        ..CollectionParams::empty()
    };
    // Tiny max segment size so all existing segments are considered over capacity.
    let optimizer_thresholds = OptimizerThresholds {
        max_segment_size_kb: 1,
        memmap_threshold_kb: 1_000_000,
        indexing_threshold_kb: 1_000_000,
        deferred_internal_id: None,
    };
    let hnsw_config = Default::default();
    let segment_config =
        build_segment_optimizer_config(&collection_params, &hnsw_config, &Default::default());

    let payload_schema_file = dir.path().join("payload.schema");
    let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_schema_file).unwrap());

    let mut holder = SegmentHolder::default();
    for _ in 0..3 {
        holder.add_new(random_segment(dir.path(), 100, 3, dim));
    }

    let initial_uuids: HashSet<_> = holder
        .iter()
        .map(|(_, seg)| seg.get().read().segment_uuid())
        .collect();

    // Initialize the manifest from the starting segment set and attach it to the holder, like a
    // freshly built shard would.
    let manifest_path = dir.path().join("manifest.json");
    let manifest = Arc::new(
        SaveOnDisk::new(
            manifest_path.clone(),
            SegmentsManifest::from_segment_holder(&holder),
        )
        .unwrap(),
    );
    holder.set_segment_manifest(Some(manifest));

    let segments = LockedSegmentHolder::new(holder);

    UpdateWorkers::ensure_appendable_segment_with_capacity(
        &segments,
        dir.path(),
        &segment_config,
        &optimizer_thresholds,
        payload_index_schema,
    )
    .unwrap();

    // A new appendable segment must have been created.
    assert_eq!(segments.read().len(), initial_uuids.len() + 1);

    // It must already be in the persisted manifest, which must match the live segment set exactly:
    // never missing a live (writable) segment.
    let persisted: SegmentsManifest = common::fs::read_json(&manifest_path).unwrap();
    let live = SegmentsManifest::from_segment_holder(&segments.read());
    assert_eq!(
        persisted, live,
        "manifest must include the newly created appendable segment",
    );

    // Sanity: the manifest grew by exactly the new segment and still lists the originals.
    assert_eq!(persisted.len(), initial_uuids.len() + 1);
    for uuid in &initial_uuids {
        assert!(persisted.get(uuid).is_some());
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
