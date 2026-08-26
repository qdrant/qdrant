//! Regression test: aborting a scale-down resharding must not wait for
//! deferred points to become visible.
//!
//! `abort_resharding` holds the shard-holder write lock while
//! `scale_down_cleanup_points` deletes the points that were migrated into the
//! remaining shards. If that delete waits for *visibility*
//! (`WaitUntil::Visible`, no timeout) and `prevent_unoptimized` is enabled,
//! it only resolves once an optimization has cleared every deferred point of
//! the shard. When the optimizer cannot make progress, the write lock is held
//! indefinitely: every shard-holder reader (REST/gRPC handlers, searches,
//! telemetry) hangs behind it, and in a cluster the consensus apply thread —
//! which drives this abort on `SetShardReplicaState(Dead)` for a
//! `ReshardingScaleDown` replica, e.g. after killing that peer — wedges the
//! whole node.

use std::time::Duration;

use collection::operations::CollectionUpdateOperations;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::optimizers_builder::OptimizersConfig;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard_trait::WaitUntil;
use common::counter::hardware_accumulator::HwMeasurementAcc;
use itertools::Itertools;
use tempfile::Builder;
use uuid::Uuid;

use crate::common::{NoopReshardingConsensus, TEST_OPTIMIZERS_CONFIG, collection_fixture};

/// Abort of a scale-down resharding must complete even if deferred points can
/// never become visible. The optimizer is disabled here
/// (`max_optimization_threads: 0`), so once a point is deferred it stays
/// deferred: an abort whose cleanup waits for visibility hangs forever and
/// trips the timeout below.
#[tokio::test(flavor = "multi_thread")]
async fn test_abort_resharding_down_does_not_wait_for_deferred_points() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();

    let optimizer_config = OptimizersConfig {
        // 1 KB indexing threshold: with 4-dim f32 vectors points become
        // deferred from segment offset 64 on, so a moderate insert is
        // guaranteed to leave deferred points behind.
        indexing_threshold: Some(1),
        max_optimization_threads: Some(0),
        prevent_unoptimized: Some(true),
        ..TEST_OPTIMIZERS_CONFIG
    };
    let collection = collection_fixture(dir.path(), 2, optimizer_config).await;

    // Scale-down resharding: drain shard 1 into shard 0.
    let key = ReshardKey {
        uuid: Uuid::new_v4(),
        direction: ReshardingDirection::Down,
        peer_id: 0,
        shard_id: 1,
        shard_key: None,
    };
    collection
        .start_resharding(
            key.clone(),
            Box::new(NoopReshardingConsensus),
            std::future::ready(()),
            std::future::ready(()),
        )
        .await
        .expect("start_resharding down must succeed");
    collection
        .set_shard_replica_state(
            1,
            0,
            ReplicaState::ReshardingScaleDown,
            Some(ReplicaState::Active),
        )
        .await
        .expect("marking the target replica ReshardingScaleDown must succeed");

    // Upsert while the resharding hash ring is active: points whose home is
    // shard 1 are dual-written into shard 0 (their post-resharding home), so
    // shard 0 now holds points outside its own hash ring — exactly what the
    // abort's cleanup deletes. `WaitUntil::Segment` (not `Visible`): the
    // setup itself must not wait on the disabled optimizer.
    let batch = BatchPersisted {
        ids: (0..1000_u64).map(u64::into).collect_vec(),
        vectors: BatchVectorStructPersisted::Single(
            (0..1000).map(|i| vec![i as f32, 0.0, 1.0, 1.0]).collect(),
        ),
        payloads: None,
    };
    let insert = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::from(batch),
    ));
    collection
        .update_from_client(
            insert,
            WaitUntil::Segment,
            None,
            WriteOrdering::default(),
            None,
            HwMeasurementAcc::new(),
        )
        .await
        .expect("insert must succeed");

    // The abort itself: scroll plus one delete operation on ~500 points. Its
    // correctness must not depend on optimizer progress, so it has to finish
    // promptly; the generous timeout only guards against the hang.
    let abort = collection.abort_resharding(key, false, Default::default());
    tokio::time::timeout(Duration::from_secs(30), abort)
        .await
        .expect("abort_resharding must not hang waiting for deferred points to become visible")
        .expect("abort_resharding must succeed");

    assert!(
        collection.resharding_state().await.is_none(),
        "resharding state must be cleared after abort",
    );
}
