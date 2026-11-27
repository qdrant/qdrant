use std::sync::Arc;
use std::time::Duration;

use collection::collection::Collection;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::types::CountRequestInternal;
use collection::operations::{CollectionUpdateOperations, OperationWithClockTag};
use collection::shards::channel_service::ChannelService;
use collection::shards::remote_shard::RemoteShard;
use collection::shards::replica_set::ShardReplicaSet;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::ShardId;
use collection::shards::shard_trait::WaitUntil;
use collection::shards::transfer::transfer_tasks_pool::TransferTaskProgress;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::types::DeferredBehavior;
use parking_lot::Mutex;
use rstest::rstest;
use tempfile::{Builder, TempDir};
use uuid::Uuid;

use crate::common::{NoopReshardingConsensus, simple_collection_fixture};

const POINTS: usize = 100;
const RECEIVER: ShardId = 0;
const REMOVED: ShardId = 1;

/// Aborting a resharding down must delete the dual-written copies on a transfer source, whether
/// it is a queue proxy or already a forward proxy, without forwarding anything to the remote.
#[rstest]
#[case::queue_proxy(false)]
#[case::forward_proxy(true)]
#[tokio::test(flavor = "multi_thread")]
async fn test_abort_resharding_down_cleans_transfer_source(#[case] forward: bool) {
    let (_dir, collection, key) = resharding_down_with_copies().await;
    let receiver = replica_set(&collection, RECEIVER).await;

    let queued_from = receiver.wal_version().await.unwrap();
    receiver
        .queue_proxify_local(
            RemoteShard::new(RECEIVER, "test".to_string(), 1, ChannelService::default()),
            None,
            Arc::new(Mutex::new(TransferTaskProgress::new())),
        )
        .await
        .unwrap();
    if forward {
        receiver.queue_proxy_into_forward_proxy().await.unwrap();
    }

    collection
        .abort_resharding(key, false, Default::default())
        .await
        .unwrap();

    if forward {
        assert!(receiver.is_proxy().await);
        receiver.un_proxify_local().await.unwrap();
    } else {
        assert!(receiver.is_queue_proxy().await);
        receiver.revert_queue_proxy_local().await;
        assert!(
            receiver.wal_version().await.unwrap() > queued_from,
            "the delete must be in the WAL, so the queue proxy replays it to the target",
        );
    }
    wait_for_total(&collection, POINTS).await;
}

/// Copies may reach a transfer target after the abort cleaned it, so it must be cleaned again when
/// the transfer finishes or is aborted.
#[rstest]
#[case::finish(true)]
#[case::abort(false)]
#[tokio::test(flavor = "multi_thread")]
async fn test_abort_resharding_down_cleans_transfer_target_when_transfer_ends(
    #[case] finish: bool,
) {
    let (_dir, collection, key) = resharding_down_with_copies().await;
    let transfer = ShardTransfer {
        shard_id: RECEIVER,
        to_shard_id: None,
        from: 1,
        to: 0,
        sync: true,
        method: Some(ShardTransferMethod::WalDelta),
        filter: None,
    };
    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer.clone())
        .unwrap();

    collection
        .abort_resharding(key, false, Default::default())
        .await
        .unwrap();
    wait_for_total(&collection, POINTS).await;

    // The transfer replays the copies after the abort
    let receiver = replica_set(&collection, RECEIVER).await;
    receiver
        .update_local(
            OperationWithClockTag::from(upsert_all()),
            WaitUntil::Segment,
            None,
            true,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count(&collection, RECEIVER).await, POINTS);

    if finish {
        collection
            .finish_shard_transfer(transfer, None)
            .await
            .unwrap();
    } else {
        let shard_holder = collection.shards_holder();
        let shard_holder = shard_holder.read().await;
        collection
            .abort_shard_transfer(transfer, &shard_holder)
            .await
            .unwrap();
    }
    wait_for_total(&collection, POINTS).await;
}

/// A collection with shards 0 and 1, resharding down shard 1, with all points dual-written into
/// shard 0.
async fn resharding_down_with_copies() -> (TempDir, Collection, ReshardKey) {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 2).await;

    let key = ReshardKey {
        uuid: Uuid::new_v4(),
        direction: ReshardingDirection::Down,
        peer_id: 0,
        shard_id: REMOVED,
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
        .unwrap();
    collection
        .set_shard_replica_state(
            REMOVED,
            0,
            ReplicaState::ReshardingScaleDown,
            Some(ReplicaState::Active),
        )
        .await
        .unwrap();

    collection
        .update_from_client(
            upsert_all(),
            WaitUntil::Segment,
            None,
            WriteOrdering::default(),
            None,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();
    assert_eq!(count(&collection, RECEIVER).await, POINTS);

    (dir, collection, key)
}

fn upsert_all() -> CollectionUpdateOperations {
    let batch = BatchPersisted {
        ids: (0..POINTS as u64).map(u64::into).collect(),
        vectors: BatchVectorStructPersisted::Single(vec![vec![1.0, 0.0, 0.0, 0.0]; POINTS]),
        payloads: None,
    };
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::from(batch),
    ))
}

/// The cleanup delete only waits for the WAL, so poll until it is applied.
async fn wait_for_total(collection: &Collection, expected: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let total = count(collection, RECEIVER).await + count(collection, REMOVED).await;
        if total == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shard {RECEIVER} must drop the copies of shard {REMOVED}'s points, {total} points in total",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn replica_set(collection: &Collection, shard_id: ShardId) -> Arc<ShardReplicaSet> {
    collection
        .shards_holder()
        .read()
        .await
        .get_shard(shard_id)
        .unwrap()
        .clone()
}

async fn count(collection: &Collection, shard_id: ShardId) -> usize {
    replica_set(collection, shard_id)
        .await
        .count_local(
            Arc::new(CountRequestInternal {
                filter: None,
                exact: true,
            }),
            None,
            DeferredBehavior::WithDeferred,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap()
        .unwrap()
        .count
}
