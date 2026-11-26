use std::path::Path;
use std::time::Duration;

use collection::collection::Collection;
use collection::operations::types::CollectionError;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::transfer::{ShardTransfer, ShardTransferMethod};
use segment::types::StrictModeConfig;
use tempfile::Builder;

use crate::common::simple_collection_fixture;

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

async fn receiving_collection(path: &Path) -> Collection {
    let collection = simple_collection_fixture(path, 1).await;
    collection
        .shards_holder()
        .read()
        .await
        .get_shard(0)
        .unwrap()
        .add_remote(1, ReplicaState::Active)
        .await
        .unwrap();
    collection
        .set_shard_replica_state(0, 0, ReplicaState::Recovery, None)
        .await
        .unwrap();
    collection
}

fn transfer() -> ShardTransfer {
    ShardTransfer {
        shard_id: 0,
        to_shard_id: None,
        from: 1,
        to: 0,
        sync: true,
        method: Some(ShardTransferMethod::Snapshot),
        filter: None,
    }
}

#[tokio::test]
async fn initiation_allows_config_updates_and_readers_before_transfer_registration() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    let mut initiate = Box::pin(collection.initiate_shard_transfer(0, Some(1)));
    assert!(futures::poll!(initiate.as_mut()).is_pending());

    let strict_mode = StrictModeConfig {
        enabled: Some(true),
        ..Default::default()
    };

    // Apply the same writer as a consensus configuration entry while driving initiation.
    // It must finish before the transfer entry, otherwise consensus cannot register it.
    tokio::time::timeout(WAIT_TIMEOUT, async {
        tokio::select! {
            biased;
            result = &mut initiate => panic!("initiation finished before registration: {result:?}"),
            result = collection.update_strict_mode_config(strict_mode) => result.unwrap(),
        }
        collection.state().await;
    })
    .await
    .expect("configuration updates and readers must progress while initiation waits");

    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer())
        .unwrap();

    tokio::time::timeout(WAIT_TIMEOUT, initiate)
        .await
        .expect("initiation must observe transfer registration")
        .unwrap();
}

#[tokio::test]
async fn initiation_allows_writers_before_local_shard_initialization() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    let mut state = collection.state().await;
    state.transfers.insert(transfer());

    collection
        .shards_holder()
        .read()
        .await
        .get_shard(0)
        .unwrap()
        .remove_local()
        .await
        .unwrap();

    let mut initiate = Box::pin(collection.initiate_shard_transfer(0, Some(1)));
    assert!(futures::poll!(initiate.as_mut()).is_pending());

    // Applying shard information needs the registry write lock and recreates the local shard.
    tokio::time::timeout(WAIT_TIMEOUT, async {
        tokio::select! {
            biased;
            result = &mut initiate => panic!("initiation finished without a local shard: {result:?}"),
            result = collection.apply_state(state, 0, |_| {}) => result.unwrap(),
        }
    })
    .await
    .expect("state application must initialize the local shard while initiation waits");

    tokio::time::timeout(WAIT_TIMEOUT, initiate)
        .await
        .expect("initiation must observe local shard initialization")
        .unwrap();
}

#[tokio::test]
async fn initiation_allows_config_updates_before_replica_state_transition() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = simple_collection_fixture(dir.path(), 1).await;
    collection
        .shards_holder()
        .read()
        .await
        .get_shard(0)
        .unwrap()
        .add_remote(1, ReplicaState::Active)
        .await
        .unwrap();
    collection
        .shards_holder()
        .read()
        .await
        .register_start_shard_transfer(transfer())
        .unwrap();

    let mut initiate = Box::pin(collection.initiate_shard_transfer(0, Some(1)));
    assert!(futures::poll!(initiate.as_mut()).is_pending());

    let strict_mode = StrictModeConfig {
        enabled: Some(true),
        ..Default::default()
    };

    tokio::time::timeout(WAIT_TIMEOUT, async {
        tokio::select! {
            biased;
            result = &mut initiate => panic!("initiation accepted an active replica: {result:?}"),
            result = collection.update_strict_mode_config(strict_mode) => result.unwrap(),
        }
        collection.state().await;
    })
    .await
    .expect("configuration updates must progress before the replica state transition");

    collection
        .set_shard_replica_state(0, 0, ReplicaState::Recovery, None)
        .await
        .unwrap();

    tokio::time::timeout(WAIT_TIMEOUT, initiate)
        .await
        .expect("initiation must observe the replica state transition")
        .unwrap();
}

#[tokio::test]
async fn initiation_rejects_a_shard_removed_while_waiting() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    let mut initiate = Box::pin(collection.initiate_shard_transfer(0, Some(1)));
    assert!(futures::poll!(initiate.as_mut()).is_pending());

    // Keep initiation suspended until removal finishes. The retained replica state still
    // satisfies the waits, but it must not authorize preparation of an unregistered shard.
    let holder = collection.shards_holder();
    let mut guard = tokio::time::timeout(WAIT_TIMEOUT, holder.write())
        .await
        .expect("removing a shard must not wait for initiation");
    guard.register_start_shard_transfer(transfer()).unwrap();
    guard.drop_and_remove_shard(0).await.unwrap();
    drop(guard);

    let result = tokio::time::timeout(WAIT_TIMEOUT, initiate)
        .await
        .expect("initiation must finish after removal");

    assert!(matches!(result, Err(CollectionError::ServiceError { .. })));
    assert!(!collection.contains_shard(0).await);
}

#[tokio::test]
async fn initiation_rejects_a_shard_replaced_while_waiting() {
    let dir = Builder::new().prefix("collection").tempdir().unwrap();
    let collection = receiving_collection(dir.path()).await;
    let mut state = collection.state().await;
    state.transfers.insert(transfer());

    let mut initiate = Box::pin(collection.initiate_shard_transfer(0, Some(1)));
    assert!(futures::poll!(initiate.as_mut()).is_pending());

    let holder = collection.shards_holder();
    let mut guard = tokio::time::timeout(WAIT_TIMEOUT, holder.write())
        .await
        .expect("replacing a shard must not wait for initiation");
    guard.register_start_shard_transfer(transfer()).unwrap();
    guard.drop_and_remove_shard(0).await.unwrap();
    drop(guard);

    collection.apply_state(state, 0, |_| {}).await.unwrap();

    let result = tokio::time::timeout(WAIT_TIMEOUT, initiate)
        .await
        .expect("initiation must finish after replacement");

    let Err(CollectionError::ServiceError { error, .. }) = result else {
        panic!("initiation must reject the replaced replica, got {result:?}");
    };

    assert!(error.contains("was replaced during transfer initiation"));
    assert!(collection.contains_shard(0).await);
}
