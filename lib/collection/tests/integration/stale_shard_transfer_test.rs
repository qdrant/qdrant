//! A replica receiving a shard transfer, and the dummy shard a snapshot recovery puts in its place
//! before downloading. Only a registered transfer may trigger that clear, and a transfer from a
//! peer left with such a dummy must still abort cleanly.

use std::path::Path;

use collection::collection::Collection;
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

/// A collection whose local replica of the shard is being received: the source holds an `Active`
/// replica, and the local one is in `Recovery`.
async fn receiving_collection(dir: &Path) -> Collection {
    let collection = simple_collection_fixture(dir, 1).await;
    collection
        .shards_holder()
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
    collection
}

fn transfer(from: PeerId, to: PeerId) -> ShardTransfer {
    ShardTransfer {
        shard_id: SHARD_ID,
        to_shard_id: None,
        from,
        to,
        sync: true,
        method: Some(ShardTransferMethod::Snapshot),
        filter: None,
    }
}

async fn is_dummy(collection: &Collection) -> bool {
    collection
        .shards_holder()
        .read()
        .await
        .get_shard(SHARD_ID)
        .unwrap()
        .is_dummy()
        .await
}

#[tokio::test]
async fn test_clear_for_snapshot_recovery_refuses_unregistered_sender() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer(SOURCE_PEER_ID, THIS_PEER_ID))
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
        !is_dummy(&collection).await,
        "refused clear must leave the local shard in place",
    );

    // The registered source may
    collection
        .clear_local_shard_for_snapshot_recovery(SHARD_ID, Some(SOURCE_PEER_ID))
        .await
        .unwrap();
    assert!(
        is_dummy(&collection).await,
        "clear for the registered source must replace the local shard with a dummy",
    );
}

#[tokio::test]
async fn test_clear_for_snapshot_recovery_refuses_without_registered_transfer() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;

    // A sender that does not identify itself - an older peer - still needs a transfer
    let result = collection
        .clear_local_shard_for_snapshot_recovery(SHARD_ID, None)
        .await;
    assert!(
        matches!(result, Err(CollectionError::BadRequest { .. })),
        "clearing without any registered transfer must be refused, got {result:?}",
    );
    assert!(
        !is_dummy(&collection).await,
        "refused clear must leave the local shard in place",
    );
}

/// Transfer restarts and aborts un-proxify the sender's local shard when applied. A dummy has
/// nothing to revert, and an error here would be fatal to consensus: a peer replaying such an
/// entry on startup would fail on every start.
#[tokio::test]
async fn test_abort_shard_transfer_with_dummy_local_shard_succeeds() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer(SOURCE_PEER_ID, THIS_PEER_ID))
        .unwrap();

    // Clear the local shard the way a snapshot recovery does before downloading
    collection
        .clear_local_shard_for_snapshot_recovery(SHARD_ID, None)
        .await
        .unwrap();
    assert!(is_dummy(&collection).await);

    // This peer is the sender of a registered transfer
    let transfer = transfer(THIS_PEER_ID, SOURCE_PEER_ID);
    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer.clone())
        .unwrap();

    collection
        .abort_shard_transfer_and_resharding(transfer.key())
        .await
        .expect("aborting a transfer from a dummy local shard must not fail");

    assert!(
        collection
            .shards_holder()
            .read()
            .await
            .get_transfer(&transfer.key())
            .is_none(),
        "abort must unregister the transfer",
    );
    assert!(
        is_dummy(&collection).await,
        "abort must leave the dummy in place for recovery to replace",
    );
}
