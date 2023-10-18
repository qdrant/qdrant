use std::fmt;
use std::path::Path;
use std::sync::Arc;

use collection::collection::Collection;
use collection::operations::snapshot_ops::{
    ShardSnapshotLocation, SnapshotDescription, SnapshotPriority,
};
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use storage::content_manager::errors::StorageError;
use storage::content_manager::snapshots;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::RwLockReadGuard;

pub async fn create_shard_snapshot(
    toc: Arc<TableOfContent>,
    collection_name: String,
    shard_id: ShardId,
) -> Result<SnapshotDescription, StorageError> {
    let collection = toc.get_collection(&collection_name).await?;

    let snapshot = collection
        .create_shard_snapshot(shard_id, &toc.optional_temp_or_snapshot_temp_path()?)
        .await?;

    Ok(snapshot)
}

pub async fn list_shard_snapshots(
    toc: Arc<TableOfContent>,
    collection_name: String,
    shard_id: ShardId,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let collection = toc.get_collection(&collection_name).await?;
    let snapshots = collection.list_shard_snapshots(shard_id).await?;
    Ok(snapshots)
}

pub async fn delete_shard_snapshot(
    toc: Arc<TableOfContent>,
    collection_name: String,
    shard_id: ShardId,
    snapshot_name: String,
) -> Result<(), StorageError> {
    let collection = toc.get_collection(&collection_name).await?;
    let snapshot_path = collection
        .get_shard_snapshot_path(shard_id, &snapshot_name)
        .await?;

    check_shard_snapshot_file_exists(&snapshot_path)?;
    std::fs::remove_file(&snapshot_path)?;

    Ok(())
}

pub async fn recover_shard_snapshot(
    toc: Arc<TableOfContent>,
    collection_name: String,
    shard_id: ShardId,
    snapshot_location: ShardSnapshotLocation,
    snapshot_priority: SnapshotPriority,
) -> Result<(), StorageError> {
    let collection = toc.get_collection(&collection_name).await?;
    collection.assert_shard_exists(shard_id).await?;

    let download_dir = toc.snapshots_download_tempdir()?;

    let snapshot_path = match snapshot_location {
        ShardSnapshotLocation::Url(url) => {
            if !matches!(url.scheme(), "http" | "https") {
                let description = format!(
                    "Invalid snapshot URL {url}: URLs with {} scheme are not supported",
                    url.scheme(),
                );

                return Err(StorageError::bad_input(description));
            }
            snapshots::download::download_snapshot(url, download_dir.path()).await?
        }

        ShardSnapshotLocation::Path(path) => {
            let snapshot_path = collection.get_shard_snapshot_path(shard_id, path).await?;
            check_shard_snapshot_file_exists(&snapshot_path)?;
            snapshot_path
        }
    };

    recover_shard_snapshot_impl(
        &toc,
        &collection,
        shard_id,
        &snapshot_path,
        snapshot_priority,
    )
    .await?;

    Ok(())
}

pub async fn recover_shard_snapshot_impl(
    toc: &TableOfContent,
    collection: &RwLockReadGuard<'_, Collection>,
    shard: ShardId,
    snapshot_path: &std::path::Path,
    priority: SnapshotPriority,
) -> Result<(), StorageError> {
    // TODO: Check snapshot compatibility?
    // TODO: Switch replica into `Partial` state?

    collection
        .restore_shard_snapshot(
            shard,
            snapshot_path,
            toc.this_peer_id,
            toc.is_distributed(),
            &toc.optional_temp_or_snapshot_temp_path()?,
        )
        .await?;

    let state = collection.state().await;
    let shard_info = state.shards.get(&shard).unwrap(); // TODO: Handle `unwrap`?..

    // TODO: Unify (and de-duplicate) "recovered shard state notification" logic in `_do_recover_from_snapshot` with this one!

    let other_active_replicas: Vec<_> = shard_info
        .replicas
        .iter()
        .map(|(&peer, &state)| (peer, state))
        .filter(|&(peer, state)| peer != toc.this_peer_id && state == ReplicaState::Active)
        .collect();

    if other_active_replicas.is_empty() {
        snapshots::recover::activate_shard(toc, collection, toc.this_peer_id, &shard).await?;
    } else {
        match priority {
            SnapshotPriority::NoSync => {
                snapshots::recover::activate_shard(toc, collection, toc.this_peer_id, &shard)
                    .await?;
            }

            SnapshotPriority::Snapshot => {
                snapshots::recover::activate_shard(toc, collection, toc.this_peer_id, &shard)
                    .await?;

                for &(peer, _) in other_active_replicas.iter() {
                    toc.send_set_replica_state_proposal(
                        collection.name(),
                        peer,
                        shard,
                        ReplicaState::Dead,
                        None,
                    )?;
                }
            }

            SnapshotPriority::Replica => {
                toc.send_set_replica_state_proposal(
                    collection.name(),
                    toc.this_peer_id,
                    shard,
                    ReplicaState::Dead,
                    None,
                )?;
            }

            // `ShardTransfer` is only used during snapshot *shard transfer*.
            // State transitions are performed as part of shard transfer *later*, so this simply does *nothing*.
            SnapshotPriority::ShardTransfer => (),
        }
    }

    Ok(())
}

fn check_shard_snapshot_file_exists(snapshot_path: &Path) -> Result<(), StorageError> {
    let snapshot_path_display = snapshot_path.display();
    let snapshot_file_name = snapshot_path.file_name().and_then(|str| str.to_str());

    let snapshot: &dyn fmt::Display = snapshot_file_name
        .as_ref()
        .map_or(&snapshot_path_display, |str| str);

    if !snapshot_path.exists() {
        let description = format!("Snapshot {snapshot} not found");
        Err(StorageError::NotFound { description })
    } else if !snapshot_path.is_file() {
        let description = format!("{snapshot} is not a file");
        Err(StorageError::service_error(description))
    } else {
        Ok(())
    }
}
