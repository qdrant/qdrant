//! The receiver of a shard snapshot transfer clears its local shard before downloading the
//! snapshot. Only the source of a registered transfer into that shard may trigger this, so a
//! sender driving a transfer that consensus has since aborted cannot wipe a replica that another
//! transfer is populating.

use collection::operations::types::CollectionError;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use tempfile::Builder;

use crate::common::simple_collection_fixture;

const SHARD_ID: ShardId = 0;
const THIS_PEER_ID: PeerId = 0;
const SOURCE_PEER_ID: PeerId = 1;
const STALE_PEER_ID: PeerId = 2;

#[tokio::test]
async fn test_clear_for_snapshot_recovery_refuses_unregistered_sender() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 1).await;
    let shards_holder = collection.shards_holder();

    // This peer receives shard 0 from `SOURCE_PEER_ID`: the source replica is `Active`, the
    // receiving replica is in `Recovery`, and the transfer is registered
    shards_holder
        .read()
        .await
        .get_shard(SHARD_ID)
        .unwrap()
        .add_remote(SOURCE_PEER_ID, ReplicaState::Active)
        .await
        .unwrap();
    collection
        .set_shard_replica_state(SHARD_ID, THIS_PEER_ID, ReplicaState::Recovery, None)
        .await
        .unwrap();
    shards_holder
        .read()
        .await
        .register_start_shard_transfer(ShardTransfer {
            shard_id: SHARD_ID,
            to_shard_id: None,
            from: SOURCE_PEER_ID,
            to: THIS_PEER_ID,
            sync: true,
            method: Some(ShardTransferMethod::Snapshot),
            filter: None,
        })
        .unwrap();

    // A peer without a registered transfer into this shard must not get to wipe it
    let result = collection
        .clear_local_shard_for_snapshot_recovery(SHARD_ID, Some(STALE_PEER_ID))
        .await;
    assert!(
        matches!(result, Err(CollectionError::BadRequest { .. })),
        "clearing for an unregistered sender must be refused, got {result:?}",
    );
    assert!(
        !shards_holder
            .read()
            .await
            .get_shard(SHARD_ID)
            .unwrap()
            .is_dummy()
            .await,
        "refused clear must leave the local shard in place",
    );

    // The registered source may
    collection
        .clear_local_shard_for_snapshot_recovery(SHARD_ID, Some(SOURCE_PEER_ID))
        .await
        .unwrap();
    assert!(
        shards_holder
            .read()
            .await
            .get_shard(SHARD_ID)
            .unwrap()
            .is_dummy()
            .await,
        "clear for the registered source must replace the local shard with a dummy",
    );
}
