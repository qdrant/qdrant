use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use rand::rng;
use segment::data_types::vectors::VectorStructInternal;
use segment::fixtures::payload_fixtures::random_vector;
use segment::types::{Distance, Filter, PointIdType};
use tempfile::{Builder, TempDir};
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::collection::payload_index_schema::PayloadIndexSchema;
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::types::{ShardStatus, VectorsConfig};
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};

const DIM: usize = 64;
const NUM_POINTS: u64 = 200;

fn random_points() -> Vec<PointStructPersisted> {
    let mut rng = rng();
    (0..NUM_POINTS)
        .map(|i| PointStructPersisted {
            id: i.into(),
            vector: VectorStructInternal::from(random_vector(&mut rng, DIM)).into(),
            payload: None,
        })
        .collect()
}

fn upsert_op(points: Vec<PointStructPersisted>) -> OperationWithClockTag {
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ))
    .into()
}

/// Build a local shard with `prevent_unoptimized = true` and a very small
/// indexing threshold so that points become deferred almost immediately.
async fn build_shard() -> (LocalShard, TempDir) {
    let collection_dir = Builder::new()
        .prefix("test_deferred_dedup")
        .tempdir()
        .unwrap();

    let wal_config = WalConfig {
        wal_capacity_mb: 1,
        wal_segments_ahead: 0,
        wal_retain_closed: 1,
    };

    let collection_params = CollectionParams {
        vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM as u64, Distance::Dot).build()),
        ..CollectionParams::empty()
    };

    let optimizer_config = OptimizersConfig {
        deleted_threshold: 0.1,
        vacuum_min_vector_number: 10,
        default_segment_number: 1,
        max_segment_size: None,
        #[expect(deprecated)]
        memmap_threshold: None,
        // Very small threshold so optimization triggers quickly and points become deferred.
        indexing_threshold: Some(1),
        flush_interval_sec: 0,
        max_optimization_threads: Some(2),
        prevent_unoptimized: Some(true),
    };

    let config = CollectionConfigInternal {
        params: collection_params,
        optimizer_config: optimizer_config.clone(),
        wal_config,
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
    };

    let payload_index_schema_file = collection_dir.path().join("payload.json");
    let payload_index_schema: Arc<SaveOnDisk<PayloadIndexSchema>> =
        Arc::new(SaveOnDisk::load_or_init_default(payload_index_schema_file).unwrap());

    let current_runtime = Handle::current();

    let shard = LocalShard::build(
        0,
        "test_deferred_dedup".to_string(),
        collection_dir.path(),
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema,
        current_runtime.clone(),
        current_runtime.clone(),
        ResourceBudget::default(),
        optimizer_config,
    )
    .await
    .unwrap();

    (shard, collection_dir)
}

/// Wait until the shard is Green (no pending optimizations) or timeout.
async fn wait_optimization(shard: &LocalShard, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        let (status, _) = shard.local_shard_status().await;
        if status == ShardStatus::Green {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "Timeout waiting for optimizations to finish (status: {status:?})",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Check that across all segments each point has at most one non-deferred copy
/// and at most one deferred copy.
fn assert_no_duplicate_point_ids(shard: &LocalShard) {
    let segments = shard.segments();
    let holder = segments.read();

    // For each point, collect (segment_id, is_deferred) pairs
    let mut point_occurrences: std::collections::HashMap<
        segment::types::PointIdType,
        Vec<(usize, bool)>,
    > = std::collections::HashMap::new();

    for (seg_id, segment) in holder.iter() {
        let seg = segment.get();
        let seg_read = seg.read();
        for pid in seg_read.iter_points() {
            let is_deferred = seg_read.point_is_deferred(pid);
            point_occurrences
                .entry(pid)
                .or_default()
                .push((seg_id, is_deferred));
        }
    }

    let mut non_deferred_dups = Vec::new();
    let mut deferred_dups = Vec::new();

    for (pid, occurrences) in &point_occurrences {
        let non_deferred_count = occurrences.iter().filter(|(_, d)| !*d).count();
        let deferred_count = occurrences.iter().filter(|(_, d)| *d).count();

        if non_deferred_count > 1 {
            let seg_ids: Vec<_> = occurrences
                .iter()
                .filter(|(_, d)| !*d)
                .map(|(s, _)| *s)
                .collect();
            non_deferred_dups.push((*pid, seg_ids));
        }
        if deferred_count > 1 {
            let seg_ids: Vec<_> = occurrences
                .iter()
                .filter(|(_, d)| *d)
                .map(|(s, _)| *s)
                .collect();
            deferred_dups.push((*pid, seg_ids));
        }
    }

    assert!(
        non_deferred_dups.is_empty(),
        "Found {count} points with multiple non-deferred copies: {non_deferred_dups:?}",
        count = non_deferred_dups.len(),
    );

    assert!(
        deferred_dups.is_empty(),
        "Found {count} points with multiple deferred copies: {deferred_dups:?}",
        count = deferred_dups.len(),
    );

    // Also verify we didn't lose any points
    assert_eq!(
        point_occurrences.len(),
        NUM_POINTS as usize,
        "Expected {NUM_POINTS} unique point IDs, got {}",
        point_occurrences.len(),
    );
}

/// Test that deferred points are properly deduplicated after optimization.
///
/// The scenario:
/// 1. Create shard with `prevent_unoptimized = true` and tiny indexing threshold
/// 2. Insert points — some will become deferred
/// 3. Overwrite existing point values to trigger CoW (the overwritten points
///    may exist in both the optimized segment and the appendable segment with deferred status)
/// 4. Wait for optimization to complete
/// 5. Verify no duplicate non-deferred point IDs across segments
#[tokio::test(flavor = "multi_thread")]
async fn test_deferred_points_dedup_after_optimization() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (shard, _tmp_dir) = build_shard().await;
    let hw_acc = HwMeasurementAcc::new();
    let timeout = Duration::from_secs(30);

    // Step 1: Insert initial batch of points (wait=true to ensure they are persisted)
    shard
        .update(
            upsert_op(random_points()),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // Wait for any triggered optimization to settle
    wait_optimization(&shard, timeout).await;

    // Step 2: Overwrite all point values with new random vectors — this triggers CoW.
    // Use wait=false so that the update doesn't block waiting for deferred points to be
    // resolved (with prevent_unoptimized=true, wait=true would wait for optimization).
    // Then use plunge_async to ensure the update is actually applied before checking.
    shard
        .update(
            upsert_op(random_points()),
            WaitUntil::Wal,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // Wait for empty update worker
    shard.plunge_async().await.unwrap().await.unwrap();

    // Check that there are deferred points in the shard, otherwise the test scenario is not valid.
    let has_deferred = shard
        .segments()
        .read()
        .iter()
        .any(|segment| !segment.1.get().read().deferred_point_ids().is_empty());
    assert!(
        has_deferred,
        "Expected to have deferred points after updates, but found none"
    );

    // Wait for optimization triggered by the overwrites
    wait_optimization(&shard, timeout).await;

    // Step 3: Overwrite again — this creates another round of CoW on top of the
    // previous deferred points, stressing the deduplication logic further.
    shard
        .update(
            upsert_op(random_points()),
            WaitUntil::Wal,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();

    // Wait for empty update worker
    shard.plunge_async().await.unwrap().await.unwrap();

    // Check that there are deferred points in the shard, otherwise the test scenario is not valid.
    let has_deferred = shard
        .segments()
        .read()
        .iter()
        .any(|segment| !segment.1.get().read().deferred_point_ids().is_empty());
    assert!(
        has_deferred,
        "Expected to have deferred points after updates, but found none"
    );

    // Wait for final optimization
    wait_optimization(&shard, timeout).await;

    // Step 4: Assert no duplicate non-deferred point IDs across segments
    assert_no_duplicate_point_ids(&shard);

    shard.stop_gracefully().await;
}

fn delete_by_ids_op(ids: Vec<PointIdType>) -> OperationWithClockTag {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePoints { ids }).into()
}

fn delete_by_filter_op(filter: Filter) -> OperationWithClockTag {
    CollectionUpdateOperations::PointOperation(PointOperations::DeletePointsByFilter(filter)).into()
}

/// Count total available (non-deleted) points across all segments.
fn total_point_count(shard: &LocalShard) -> usize {
    let segments = shard.segments();
    let holder = segments.read();
    holder
        .iter()
        .map(|(_, segment)| segment.get().read().available_point_count())
        .sum()
}

/// Set up a shard where points have a newer deferred copy and an older non-deferred copy.
///
/// Returns the shard after:
/// 1. Insert points + wait for optimization (all points non-deferred in optimized segment)
/// 2. Overwrite all points (creates deferred copies in appendable segment)
/// 3. Plunge to apply update, verify deferred points exist
async fn setup_shard_with_deferred_points() -> (LocalShard, TempDir) {
    let (shard, tmp_dir) = build_shard().await;
    let hw_acc = HwMeasurementAcc::new();
    let timeout = Duration::from_secs(30);

    // Insert initial points and wait for optimization so they are non-deferred in optimized segment
    shard
        .update(
            upsert_op(random_points()),
            WaitUntil::Visible,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();
    wait_optimization(&shard, timeout).await;

    // Overwrite all points — creates newer deferred copies in appendable segment
    // while old non-deferred copies remain in the optimized segment
    shard
        .update(
            upsert_op(random_points()),
            WaitUntil::Wal,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();
    shard.plunge_async().await.unwrap().await.unwrap();

    // Verify we actually have deferred points, otherwise the test scenario is invalid
    let has_deferred = shard
        .segments()
        .read()
        .iter()
        .any(|segment| !segment.1.get().read().deferred_point_ids().is_empty());
    assert!(
        has_deferred,
        "Expected deferred points after overwrites, but found none"
    );

    (shard, tmp_dir)
}

/// Test that delete-by-ID removes all copies of a point, including old non-deferred
/// copies when the latest version is deferred.
///
/// Bug scenario:
/// 1. Point X is in optimized segment (v5, non-deferred, visible)
/// 2. Upsert creates a new copy in appendable segment (v10, deferred, hidden)
/// 3. Delete by ID should remove from ALL segments
/// 4. After optimization, point should be completely gone
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_by_id_with_deferred_points() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (shard, _tmp_dir) = setup_shard_with_deferred_points().await;
    let hw_acc = HwMeasurementAcc::new();
    let timeout = Duration::from_secs(30);

    // Delete all points by ID
    let all_ids: Vec<PointIdType> = (0..NUM_POINTS).map(|i| i.into()).collect();
    shard
        .update(
            delete_by_ids_op(all_ids),
            WaitUntil::Wal,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();
    shard.plunge_async().await.unwrap().await.unwrap();

    // Wait for optimization to settle (deferred points trigger optimization)
    wait_optimization(&shard, timeout).await;

    // All points should be gone
    let remaining = total_point_count(&shard);
    assert_eq!(
        remaining, 0,
        "Expected 0 points after delete-by-ID, but {remaining} survived \
         (old non-deferred copies likely not deleted when latest was deferred)"
    );

    shard.stop_gracefully().await;
}

/// Test that delete-by-filter removes all copies of a point, including deferred ones.
/// This uses an empty filter (matches all points) to mirror the delete-by-ID test above.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_by_filter_with_deferred_points() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (shard, _tmp_dir) = setup_shard_with_deferred_points().await;
    let hw_acc = HwMeasurementAcc::new();
    let timeout = Duration::from_secs(30);

    // Delete all points by filter (empty filter = match all)
    shard
        .update(
            delete_by_filter_op(Filter::default()),
            WaitUntil::Wal,
            None,
            hw_acc.clone(),
        )
        .await
        .unwrap();
    shard.plunge_async().await.unwrap().await.unwrap();

    // Wait for optimization to settle
    wait_optimization(&shard, timeout).await;

    // All points should be gone
    let remaining = total_point_count(&shard);
    assert_eq!(
        remaining, 0,
        "Expected 0 points after delete-by-filter, but {remaining} survived"
    );

    shard.stop_gracefully().await;
}
