//! Shard cleanup must not delete points with a stale hash ring.
//!
//! The clean task clones the hash ring and filters a batch without holding the shards holder lock.
//! If routing changes in the meantime, the delete must not be submitted, otherwise it could
//! delete points that now belong in the shard, and that a user may have just written into it.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use common::ambient::{AmbientContext, AmbientFutureExt};
use common::budget::ResourceBudget;
use common::reason::reason;
use common::types::DeferredBehavior;
use segment::types::{Distance, ExtendedPointId};
use tempfile::Builder;
use uuid::Uuid;

use crate::collection::Collection;
use crate::collection::clean::{SubmitDelete, get_local_shard, submit_fenced_delete};
use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
use crate::operations::cluster_ops::ReshardingDirection;
use crate::operations::point_ops::{
    PointInsertOperationsInternal, PointOperations, PointStructPersisted, VectorStructPersisted,
    WriteOrdering,
};
use crate::operations::shared_storage_config::SharedStorageConfig;
use crate::operations::types::VectorsConfig;
use crate::operations::vector_params_builder::VectorParamsBuilder;
use crate::operations::{CollectionUpdateOperations, OperationWithClockTag};
use crate::optimizers_builder::OptimizersConfig;
use crate::shards::channel_service::ChannelService;
use crate::shards::collection_shard_distribution::CollectionShardDistribution;
use crate::shards::replica_set::ShardReplicaSet;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::resharding::ReshardKey;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_trait::WaitUntil;
use crate::tests::points_dedup::{
    dummy_abort_shard_transfer, dummy_on_replica_failure, dummy_request_shard_transfer,
};

const DIM: u64 = 4;
const PEER_ID: PeerId = 1;
const CLEAN_SHARD: ShardId = 0;
const OTHER_SHARD: ShardId = 1;
const POINT_COUNT: u64 = 200;

async fn fixture(path: &Path) -> Collection {
    let config = CollectionConfigInternal {
        params: CollectionParams {
            vectors: VectorsConfig::Single(VectorParamsBuilder::new(DIM, Distance::Dot).build()),
            shard_number: NonZeroU32::new(2).unwrap(),
            replication_factor: NonZeroU32::new(1).unwrap(),
            write_consistency_factor: NonZeroU32::new(1).unwrap(),
            ..CollectionParams::empty()
        },
        optimizer_config: OptimizersConfig::fixture(),
        wal_config: WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
            wal_retain_closed: 1,
        },
        hnsw_config: Default::default(),
        quantization_config: Default::default(),
        strict_mode_config: Default::default(),
        uuid: None,
        metadata: None,
        created_at: None,
    };

    let collection_path = path.join("collection");
    let snapshots_path = path.join("snapshots");
    fs_err::create_dir_all(&collection_path).unwrap();
    fs_err::create_dir_all(&snapshots_path).unwrap();

    let shards: AHashMap<ShardId, HashSet<PeerId>> = AHashMap::from_iter([
        (CLEAN_SHARD, HashSet::from([PEER_ID])),
        (OTHER_SHARD, HashSet::from([PEER_ID])),
    ]);

    let collection = Collection::new(
        "test".to_string(),
        PEER_ID,
        &collection_path,
        &snapshots_path,
        &config,
        Arc::new(SharedStorageConfig::default()),
        CollectionShardDistribution { shards },
        None,
        ChannelService::default(),
        dummy_on_replica_failure(),
        dummy_request_shard_transfer(),
        dummy_abort_shard_transfer(),
        None,
        None,
        ResourceBudget::default(),
        None,
    )
    .await
    .unwrap();

    for shard_id in [CLEAN_SHARD, OTHER_SHARD] {
        collection
            .set_shard_replica_state(shard_id, PEER_ID, ReplicaState::Active, None)
            .await
            .unwrap();
    }

    collection
}

fn upsert(ids: impl IntoIterator<Item = ExtendedPointId>) -> CollectionUpdateOperations {
    let points = ids
        .into_iter()
        .map(|id| PointStructPersisted {
            id,
            vector: VectorStructPersisted::Single(vec![1.0; DIM as usize]),
            payload: None,
        })
        .collect();
    CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(
        PointInsertOperationsInternal::PointsList(points),
    ))
}

/// Insert points with a client update, then write the points that belong to the other shard into
/// the clean shard as well, leaving stray points for the cleanup to remove.
///
/// Returns the IDs of the stray points.
async fn insert_with_stray_points(collection: &Collection) -> Vec<ExtendedPointId> {
    let ids: Vec<ExtendedPointId> = (0..POINT_COUNT).map(ExtendedPointId::from).collect();

    collection
        .update_from_client(
            upsert(ids.iter().copied()),
            WaitUntil::Visible,
            None,
            WriteOrdering::Weak,
            None,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();

    let (shard, hashring) = get_local_shard(&collection.shards_holder.downgrade(), CLEAN_SHARD)
        .await
        .unwrap();
    let stray: Vec<ExtendedPointId> = ids
        .into_iter()
        .filter(|id| !hashring.is_in_shard(id, CLEAN_SHARD))
        .collect();
    assert!(
        !stray.is_empty(),
        "some points must belong to the other shard"
    );

    shard
        .update_local(
            OperationWithClockTag::from(upsert(stray.iter().copied())),
            WaitUntil::Visible,
            None,
            false,
        )
        .measured(AmbientContext::new())
        .await
        .unwrap();

    stray
}

async fn local_point_ids(shard: &ShardReplicaSet) -> HashSet<ExtendedPointId> {
    shard
        .local_scroll_by_id(
            None,
            POINT_COUNT as usize * 2,
            &false.into(),
            &false.into(),
            None,
            None,
            None,
            DeferredBehavior::WithDeferred,
        )
        .unmeasured(reason("test"))
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.id)
        .collect()
}

/// If routing changes after the batch was filtered, the delete must not be submitted
#[tokio::test(flavor = "multi_thread")]
async fn test_clean_delete_is_fenced_against_routing_change() {
    let dir = Builder::new().prefix("clean_fenced").tempdir().unwrap();
    let collection = fixture(dir.path()).await;
    let stray = insert_with_stray_points(&collection).await;
    let shard_holder = collection.shards_holder.downgrade();

    // Clone shard and hash ring like the clean task does, then release the lock
    let (shard, stale_hashring) = get_local_shard(&shard_holder, CLEAN_SHARD).await.unwrap();
    let before = local_point_ids(&shard).await;
    assert!(stray.iter().all(|id| before.contains(id)));

    // Routing changes: a scale-down of the other shard starts, so its points now map back into
    // the clean shard
    collection
        .shards_holder
        .write()
        .await
        .start_resharding_unchecked(
            ReshardKey {
                uuid: Uuid::new_v4(),
                direction: ReshardingDirection::Down,
                peer_id: PEER_ID,
                shard_id: OTHER_SHARD,
                shard_key: None,
            },
            None,
        )
        .await
        .unwrap();

    // Delete with the stale hash ring must be refused
    let outcome = submit_fenced_delete(
        &shard_holder,
        CLEAN_SHARD,
        &shard,
        &stale_hashring,
        stray.clone(),
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, SubmitDelete::RoutingChanged),
        "delete with a stale hash ring must not be submitted",
    );
    assert_eq!(
        local_point_ids(&shard).await,
        before,
        "no points may be deleted"
    );

    // With the current hash ring, the points belong in the clean shard and are kept
    let (shard, hashring) = get_local_shard(&shard_holder, CLEAN_SHARD).await.unwrap();
    assert_ne!(hashring, stale_hashring);
    assert!(stray.iter().all(|id| hashring.is_in_shard(id, CLEAN_SHARD)));
    let outcome = submit_fenced_delete(&shard_holder, CLEAN_SHARD, &shard, &hashring, vec![])
        .await
        .unwrap();
    assert!(matches!(outcome, SubmitDelete::Submitted));
}

/// A full cleanup removes stray points and keeps the ones that belong in the shard
#[tokio::test(flavor = "multi_thread")]
async fn test_clean_removes_stray_points() {
    let dir = Builder::new().prefix("clean_stray").tempdir().unwrap();
    let collection = fixture(dir.path()).await;
    let stray = insert_with_stray_points(&collection).await;

    collection
        .cleanup_local_shard(CLEAN_SHARD, true, Some(Duration::from_secs(30)))
        .await
        .unwrap();

    let (shard, hashring) = get_local_shard(&collection.shards_holder.downgrade(), CLEAN_SHARD)
        .await
        .unwrap();
    let remaining = local_point_ids(&shard).await;
    assert!(stray.iter().all(|id| !remaining.contains(id)));
    let expected: HashSet<ExtendedPointId> = (0..POINT_COUNT)
        .map(ExtendedPointId::from)
        .filter(|id| hashring.is_in_shard(id, CLEAN_SHARD))
        .collect();
    assert_eq!(remaining, expected);
}
