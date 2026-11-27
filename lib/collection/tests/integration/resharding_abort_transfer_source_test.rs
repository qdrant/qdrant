use std::sync::Arc;
use std::time::Duration;

use collection::collection::Collection;
use collection::operations::CollectionUpdateOperations;
use collection::operations::cluster_ops::ReshardingDirection;
use collection::operations::point_ops::{
    BatchPersisted, BatchVectorStructPersisted, PointInsertOperationsInternal, PointOperations,
    WriteOrdering,
};
use collection::operations::types::CountRequestInternal;
use collection::shards::channel_service::ChannelService;
use collection::shards::remote_shard::RemoteShard;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::resharding::ReshardKey;
use collection::shards::shard::ShardId;
use collection::shards::shard_trait::WaitUntil;
use collection::shards::transfer::transfer_tasks_pool::TransferTaskProgress;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::types::DeferredBehavior;
use parking_lot::Mutex;
use tempfile::Builder;
use uuid::Uuid;

use crate::common::{NoopReshardingConsensus, simple_collection_fixture};

const POINTS: usize = 100;

/// Aborting a resharding down must delete the dual-written copies on a replica that is the
/// source of a WAL delta transfer (a queue proxy), and queue that delete for the transfer target.
#[tokio::test(flavor = "multi_thread")]
async fn test_abort_resharding_down_cleans_queue_proxy() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 2).await;

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
        .unwrap();
    collection
        .set_shard_replica_state(
            1,
            0,
            ReplicaState::ReshardingScaleDown,
            Some(ReplicaState::Active),
        )
        .await
        .unwrap();

    let batch = BatchPersisted {
        ids: (0..POINTS as u64).map(u64::into).collect(),
        vectors: BatchVectorStructPersisted::Single(vec![vec![1.0, 0.0, 0.0, 0.0]; POINTS]),
        payloads: None,
    };
    collection
        .update_from_client(
            CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
                PointInsertOperationsInternal::from(batch),
            )),
            WaitUntil::Segment,
            None,
            WriteOrdering::default(),
            None,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();
    assert_eq!(
        count(&collection, 0).await,
        POINTS,
        "shard 0 must hold every dual-written point"
    );

    let shard_holder = collection.shards_holder();
    let receiver = shard_holder.read().await.get_shard(0).unwrap().clone();
    let queued_from = receiver.wal_version().await.unwrap();
    receiver
        .queue_proxify_local(
            RemoteShard::new(0, "test".to_string(), 1, ChannelService::default()),
            None,
            Arc::new(Mutex::new(TransferTaskProgress::new())),
        )
        .await
        .unwrap();

    collection
        .abort_resharding(key, false, Default::default())
        .await
        .unwrap();

    assert!(receiver.is_queue_proxy().await);
    receiver.revert_queue_proxy_local().await;
    assert!(
        receiver.wal_version().await.unwrap() > queued_from,
        "the cleanup delete must go through the queue proxy, so the transfer target gets it",
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let total = count(&collection, 0).await + count(&collection, 1).await;
        if total == POINTS {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "shard 0 must drop the copies of shard 1's points, {total} points in total",
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn count(collection: &Collection, shard_id: ShardId) -> usize {
    collection
        .shards_holder()
        .read()
        .await
        .get_shard(shard_id)
        .unwrap()
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
