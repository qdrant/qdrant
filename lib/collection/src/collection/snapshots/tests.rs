use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ahash::AHashMap;
use common::ambient;
use common::budget::ResourceBudget;
use segment::types::PointIdType;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::*;
use crate::hash_ring::HashRingRouter;
use crate::operations::CollectionUpdateOperations;
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted,
};
use crate::operations::shard_selector_internal::ShardSelectorInternal;
use crate::operations::types::UpdateStatus;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::{ReshardState, ReshardingStage};
use crate::shards::shard_trait::WaitUntil;
use crate::tests::fixtures::create_collection_config;

type SnapshotPause = (PathBuf, oneshot::Sender<()>, oneshot::Receiver<()>);
static SNAPSHOT_PAUSE: Mutex<Option<SnapshotPause>> = Mutex::new(None);

pub(super) async fn pause_after_listing_shards(temp_dir: &Path) {
    let pause = {
        let mut hook = SNAPSHOT_PAUSE.lock().unwrap();
        if hook.as_ref().is_some_and(|(path, ..)| path == temp_dir) {
            hook.take()
        } else {
            None
        }
    };
    if let Some((_, listed, resume)) = pause {
        listed.send(()).unwrap();
        resume.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_rejects_concurrent_resharding_cleanup() {
    let _scope = ambient::async_test_guard();
    let collection_dir = tempfile::tempdir().unwrap();
    let snapshots_dir = tempfile::tempdir().unwrap();
    let temp_dir = tempfile::tempdir().unwrap();
    let config = create_collection_config();
    let collection = Collection::new(
        "test".into(),
        0,
        collection_dir.path(),
        snapshots_dir.path(),
        &config,
        Default::default(),
        CollectionShardDistribution {
            shards: AHashMap::from([(0, HashSet::from([0]))]),
        },
        None,
        ChannelService::default(),
        Arc::new(|_, _, _| {}),
        Arc::new(|_| {}),
        Arc::new(|_, _| {}),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();
    collection
        .apply_replica_state(0, 0, ReplicaState::Active)
        .await
        .unwrap();

    let mut ring = HashRingRouter::single();
    ring.add(0);
    ring.start_resharding(1, ReshardingDirection::Up);
    let id = (0..100_u64)
        .map(PointIdType::from)
        .find(|id| ring.is_in_shard(id, 1))
        .unwrap();
    let point = CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::from(vec![PointStructPersisted {
            id,
            vector: vec![1.0, 2.0, 3.0, 4.0].into(),
            payload: None,
        }]),
    ));

    async fn write_point(
        collection: &Collection,
        shard_id: ShardId,
        point: CollectionUpdateOperations,
    ) {
        collection
            .update_from_peer(
                point.into(),
                shard_id,
                WaitUntil::Visible,
                None,
                Default::default(),
            )
            .in_current_ambient()
            .await
            .unwrap();
    }

    async fn point_ids(collection: &Collection, shard_id: ShardId) -> Vec<PointIdType> {
        collection
            .scroll_by(
                Default::default(),
                None,
                None,
                &ShardSelectorInternal::ShardId(shard_id),
                None,
            )
            .in_current_ambient()
            .await
            .unwrap()
            .points
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    write_point(&collection, 0, point.clone()).await;
    assert_eq!(point_ids(&collection, 0).await, vec![id]);

    let (listed, listed_rx) = oneshot::channel();
    let (resume, resume_rx) = oneshot::channel();
    *SNAPSHOT_PAUSE.lock().unwrap() = Some((temp_dir.path().to_owned(), listed, resume_rx));
    let migrate_and_clean = async {
        listed_rx.await.unwrap();
        collection
            .create_and_register_shards(None, &[(1, vec![0], ReplicaState::Resharding)])
            .await
            .unwrap();
        let holder = collection.shards_holder();
        holder
            .write()
            .await
            .set_resharding_state(Some(ReshardState::new(
                Uuid::new_v4(),
                ReshardingDirection::Up,
                0,
                1,
                None,
            )))
            .unwrap();
        collection
            .set_shard_number(2.try_into().unwrap())
            .await
            .unwrap();
        // Copy the moving point through the same peer update path as shard transfer.
        write_point(&collection, 1, point).await;
        {
            let mut holder = holder.write().await;
            holder
                .set_resharding_stage(ReshardingStage::ReadHashRingCommitted)
                .unwrap();
            holder
                .set_resharding_stage(ReshardingStage::WriteHashRingCommitted)
                .unwrap();
        }
        let result = collection.cleanup_local_shard(0, true, None).await.unwrap();
        assert_eq!(result.status, UpdateStatus::Completed);
        assert!(point_ids(&collection, 0).await.is_empty());
        collection
            .apply_replica_state(1, 0, ReplicaState::Active)
            .await
            .unwrap();
        holder.write().await.set_resharding_state(None).unwrap();
        assert_eq!(point_ids(&collection, 1).await, vec![id]);
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            collection.create_snapshot(temp_dir.path(), 0),
            migrate_and_clean
        )
    })
    .await
    .expect("snapshot and resharding must make progress independently");

    async fn load_snapshot(
        data: SnapshotData,
        snapshots_dir: &Path,
    ) -> (tempfile::TempDir, Collection) {
        let recovered_dir = tempfile::tempdir().unwrap();
        Collection::restore_snapshot(data, recovered_dir.path(), 0, false).unwrap();
        let recovered = Collection::load(
            "recovered".into(),
            0,
            recovered_dir.path(),
            snapshots_dir,
            Default::default(),
            ChannelService::default(),
            Arc::new(|_, _, _| {}),
            Arc::new(|_| {}),
            Arc::new(|_, _| {}),
            None,
            None,
            ResourceBudget::default(),
            None,
        )
        .await;
        (recovered_dir, recovered)
    }

    let err = match result {
        Err(err) => err,
        Ok(snapshot) => {
            let unpacked = tempfile::tempdir().unwrap();
            tar_unpack_file(&snapshots_dir.path().join(snapshot.name), unpacked.path()).unwrap();
            assert!(!shard_path(unpacked.path(), 1).exists());
            // Restore just the topology that was archived so a late shard count cannot
            // hide whether the archive actually contains the point.
            config.save(unpacked.path()).unwrap();
            let (_dir, recovered) =
                load_snapshot(SnapshotData::Unpacked(unpacked), snapshots_dir.path()).await;
            assert_eq!(
                point_ids(&recovered, 0).await,
                vec![id],
                "published snapshot lost the moved point"
            );
            panic!("snapshot overlapping write hash ring commit was published");
        }
    };
    assert!(err.to_string().contains("write hash ring changed"), "{err}");
    assert!(collection.list_snapshots().await.unwrap().is_empty());

    let snapshot = collection
        .create_snapshot(temp_dir.path(), 0)
        .await
        .unwrap();
    let snapshot_data =
        SnapshotData::new_packed_persistent(snapshots_dir.path().join(snapshot.name));
    let (_dir, recovered) = load_snapshot(snapshot_data, snapshots_dir.path()).await;
    assert!(point_ids(&recovered, 0).await.is_empty());
    assert_eq!(point_ids(&recovered, 1).await, vec![id]);
}
