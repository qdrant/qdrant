use std::sync::Arc;

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

use crate::operations::types::PointRequestInternal;
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
