use std::sync::Arc;
use std::time::Duration;

use common::budget::ResourceBudget;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::save_on_disk::SaveOnDisk;
use common::types::DeferredBehavior;
use segment::data_types::vectors::VectorStructInternal;
use segment::types::{WithPayload, WithVector};
use shard::operations::CollectionUpdateOperations;
use shard::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use tempfile::Builder;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::operations::types::{PointRequestInternal, UpdateStatus};
use crate::shards::local_shard::LocalShard;
use crate::shards::shard_trait::{ShardOperation, WaitUntil};
use crate::tests::fixtures::*;

/// Create a collection config with `prevent_unoptimized` and a very small indexing threshold
/// so that deferred points are triggered quickly.
fn create_deferred_points_config() -> crate::config::CollectionConfigInternal {
    let mut config = create_collection_config();
    config.optimizer_config.prevent_unoptimized = Some(true);
    // Very small threshold (1 KB) so deferred points trigger after just a few vectors
    config.optimizer_config.indexing_threshold = Some(1);
    config
}

fn make_upsert_op(point_id: u64) -> CollectionUpdateOperations {
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(vec![PointStructPersisted {
            id: point_id.into(),
            vector: VectorStructInternal::from(vec![
                point_id as f32,
                point_id as f32 + 1.0,
                point_id as f32 + 2.0,
                point_id as f32 + 3.0,
            ])
            .into(),
            payload: None,
        }]),
    ))
}

async fn build_shard(
    config: &crate::config::CollectionConfigInternal,
    collection_dir: &std::path::Path,
    payload_index_schema: Arc<
        SaveOnDisk<crate::collection::payload_index_schema::PayloadIndexSchema>,
    >,
) -> LocalShard {
    let current_runtime = Handle::current();
    LocalShard::build(
        0,
        "test_deferred".to_string(),
        collection_dir,
        Arc::new(RwLock::new(config.clone())),
        Arc::new(Default::default()),
        payload_index_schema,
        current_runtime.clone(),
        current_runtime,
        ResourceBudget::default(),
        config.optimizer_config.clone(),
    )
    .await
    .unwrap()
}

async fn retrieve_point(shard: &LocalShard, point_id: u64) -> bool {
    let request = Arc::new(PointRequestInternal {
        ids: vec![point_id.into()],
        with_payload: None,
        with_vector: WithVector::Bool(false),
    });
    let current_runtime = Handle::current();
    let retrieved = shard
        .retrieve(
            request,
            &WithPayload::from(false),
            &WithVector::Bool(false),
            &current_runtime,
            None,
            HwMeasurementAcc::new(),
            DeferredBehavior::Exclude,
        )
        .await
        .unwrap();
    !retrieved.is_empty()
}

const NUM_POINTS: u64 = 100;

/// Test that with `prevent_unoptimized=true` and `wait=true`, every upserted point
/// is visible immediately after the upsert returns.
#[tokio::test(flavor = "multi_thread")]
async fn test_deferred_points_wait_true() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new()
        .prefix("test_deferred_wait_true")
        .tempdir()
        .unwrap();
    let payload_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(payload_schema_dir.path().join("payload-schema.json"))
            .unwrap(),
    );

    let config = create_deferred_points_config();
    let shard = build_shard(&config, collection_dir.path(), payload_index_schema).await;

    let hw_acc = HwMeasurementAcc::new();

    for i in 1..=NUM_POINTS {
        let op = make_upsert_op(i);
        let result = shard
            .update(op.into(), WaitUntil::Visible, None, hw_acc.clone())
            .await;
        assert!(
            result.is_ok(),
            "Upsert with wait=true should succeed for point {i}"
        );

        // With wait=true, the point must be visible after the upsert returns
        assert!(
            retrieve_point(&shard, i).await,
            "Point {i} should be visible after upsert with wait=true"
        );
    }

    shard.stop_gracefully().await;
}

/// Regression test: a `wait=true` upsert that enters the deferred-points wait
/// must not block the update worker from processing subsequent operations.
///
/// Optimization is disabled (`max_optimization_threads=0`) so that A's deferred
/// wait is effectively indefinite — exposing any blocking of the worker loop.
///
/// - A: `WaitUntil::Visible` — enters the deferred-points wait (detached task
///   with the fix; inline in the worker loop without it).
/// - B: `WaitUntil::Segment` — only needs the worker to apply it to segments,
///   so it must complete quickly as long as the worker is free to pull B.
///
/// Without the fix, B is never picked up while A is waiting, and its 5 s
/// timeout trips.
#[tokio::test(flavor = "multi_thread")]
async fn test_wait_deferred_does_not_block_update_worker() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new()
        .prefix("test_deferred_does_not_block")
        .tempdir()
        .unwrap();
    let payload_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(payload_schema_dir.path().join("payload-schema.json"))
            .unwrap(),
    );

    let mut config = create_deferred_points_config();
    // Disable the optimizer so A's deferred wait cannot resolve on its own.
    config.optimizer_config.max_optimization_threads = Some(0);

    let shard = Arc::new(build_shard(&config, collection_dir.path(), payload_index_schema).await);
    let hw_acc = HwMeasurementAcc::new();

    // Build up deferred state so that when A is processed, `has_deferred_points`
    // is already true and the worker enters the deferred-wait branch.
    for i in 1..=NUM_POINTS {
        shard
            .update(
                make_upsert_op(i).into(),
                WaitUntil::Wal,
                None,
                hw_acc.clone(),
            )
            .await
            .unwrap();
    }

    // Spawn A on a separate task so its future (and thus its feedback receiver)
    // stays alive while B races. If dropped, the worker would detect the closed
    // receiver and exit the deferred wait early — masking the regression.
    let shard_a = Arc::clone(&shard);
    let hw_acc_a = hw_acc.clone();
    let a_handle = tokio::spawn(async move {
        shard_a
            .update(
                make_upsert_op(NUM_POINTS + 1).into(),
                WaitUntil::Visible,
                None,
                hw_acc_a,
            )
            .await
    });

    // Give the worker a beat to pick up A and enter its deferred wait.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let b_result = tokio::time::timeout(
        Duration::from_secs(5),
        shard.update(
            make_upsert_op(NUM_POINTS + 2).into(),
            WaitUntil::Segment,
            None,
            hw_acc.clone(),
        ),
    )
    .await
    .expect("B should complete within 5s — update worker appears blocked on A's deferred wait")
    .expect("B update call should not error");
    assert_eq!(b_result.status, UpdateStatus::Completed);

    // B only demonstrates non-blocking if A was genuinely still waiting on
    // deferred points. With the optimizer disabled, A must not have resolved.
    assert!(
        !a_handle.is_finished(),
        "A should still be waiting on deferred points — otherwise B's completion \
         does not prove the worker was free"
    );

    // A is not expected to resolve (optimizer is disabled). Abort it so the
    // runtime can clean up.
    a_handle.abort();
}

/// Test that with `prevent_unoptimized=true` and `wait=false`, points that reach
/// the indexing threshold are not guaranteed to be visible immediately.
#[tokio::test(flavor = "multi_thread")]
async fn test_deferred_points_wait_false() {
    let _ = env_logger::builder().is_test(true).try_init();

    let collection_dir = Builder::new()
        .prefix("test_deferred_wait_false")
        .tempdir()
        .unwrap();
    let payload_schema_dir = Builder::new().prefix("qdrant-test").tempdir().unwrap();
    let payload_index_schema = Arc::new(
        SaveOnDisk::load_or_init_default(payload_schema_dir.path().join("payload-schema.json"))
            .unwrap(),
    );

    let config = create_deferred_points_config();
    let shard = build_shard(&config, collection_dir.path(), payload_index_schema).await;

    let hw_acc = HwMeasurementAcc::new();

    // Push all points with wait=false — returns immediately without waiting for application
    for i in 1..=NUM_POINTS {
        let op = make_upsert_op(i);
        let result = shard
            .update(op.into(), WaitUntil::Wal, None, hw_acc.clone())
            .await;
        assert!(
            result.is_ok(),
            "Upsert with wait=false should succeed for point {i}"
        );
    }

    // Immediately after sending all upserts with wait=false,
    // count how many points are visible.
    let mut visible_count = 0;
    for i in 1..=NUM_POINTS {
        if retrieve_point(&shard, i).await {
            visible_count += 1;
        }
    }

    // With wait=false the update worker may not have processed all operations yet,
    // especially once the indexing threshold is reached and optimization is triggered.
    // We assert that NOT all points are visible, showing that wait=false
    // does not guarantee point visibility unlike wait=true.
    eprintln!("Visible points with wait=false: {visible_count}/{NUM_POINTS}");
    assert!(
        visible_count < NUM_POINTS as usize,
        "With wait=false, not all {NUM_POINTS} points should be immediately visible, \
         but found {visible_count} visible. This means the update worker processed \
         all operations before we could check — the indexing threshold may be too high."
    );

    shard.stop_gracefully().await;
}
