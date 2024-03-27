use std::fmt;
use std::path::Path;
use std::sync::Arc;

use collection::collection::Collection;
use collection::common::sha_256::hash_file;
use collection::operations::snapshot_ops::{
    ShardSnapshotLocation, SnapshotDescription, SnapshotPriority,
};
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::ShardId;
use storage::content_manager::errors::StorageError;
use storage::content_manager::snapshots;
use storage::content_manager::toc::TableOfContent;
use storage::rbac::access::Access;

use super::http_client::HttpClient;

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn create_shard_snapshot(
    toc: Arc<TableOfContent>,
    access: Access,
    collection_name: String,
    shard_id: ShardId,
) -> Result<SnapshotDescription, StorageError> {
    let collection_pass = access.check_whole_collection_rights(&collection_name)?;
    let collection = toc.get_collection_by_pass(&collection_pass).await?;

    let snapshot = collection
        .create_shard_snapshot(shard_id, &toc.optional_temp_or_snapshot_temp_path()?)
        .await?;

    Ok(snapshot)
}

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn list_shard_snapshots(
    toc: Arc<TableOfContent>,
    access: Access,
    collection_name: String,
    shard_id: ShardId,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let collection_pass = access.check_whole_collection_rights(&collection_name)?;
    let collection = toc.get_collection_by_pass(&collection_pass).await?;
    let snapshots = collection.list_shard_snapshots(shard_id).await?;
    Ok(snapshots)
}

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn delete_shard_snapshot(
    toc: Arc<TableOfContent>,
    access: Access,
    collection_name: String,
    shard_id: ShardId,
    snapshot_name: String,
) -> Result<(), StorageError> {
    let collection_pass = access.check_whole_collection_rights(&collection_name)?;
    let collection = toc.get_collection_by_pass(&collection_pass).await?;
    let snapshot_path = collection
        .get_shard_snapshot_path(shard_id, &snapshot_name)
        .await?;
    let snapshot_manager = collection.get_snapshots_storage_manager();
    check_shard_snapshot_file_exists(&snapshot_path)?;

    let _task = tokio::spawn(async move { snapshot_manager.delete_snapshot(&snapshot_path).await });

    Ok(())
}

/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn recover_shard_snapshot(
    toc: Arc<TableOfContent>,
    access: Access,
    collection_name: String,
    shard_id: ShardId,
    snapshot_location: ShardSnapshotLocation,
    snapshot_priority: SnapshotPriority,
    checksum: Option<String>,
    client: HttpClient,
) -> Result<(), StorageError> {
    let collection_pass = access
        .check_whole_collection_rights(&collection_name)?
        .into_static();

    // - `download_dir` handled by `tempfile` and would be deleted, if request is cancelled
    //   - remote snapshot is downloaded into `download_dir` and would be deleted with it
    // - `recover_shard_snapshot_impl` is *not* cancel safe
    //   - but the task is *spawned* on the runtime and won't be cancelled, if request is cancelled

    cancel::future::spawn_cancel_on_drop(move |cancel| async move {
        let future = async {
            let collection = toc.get_collection_by_pass(&collection_pass).await?;
            collection.assert_shard_exists(shard_id).await?;

            let download_dir = toc.snapshots_download_tempdir()?;

            let (snapshot_path, snapshot_temp_path) = match snapshot_location {
                ShardSnapshotLocation::Url(url) => {
                    if !matches!(url.scheme(), "http" | "https") {
                        let description = format!(
                            "Invalid snapshot URL {url}: URLs with {} scheme are not supported",
                            url.scheme(),
                        );

                        return Err(StorageError::bad_input(description));
                    }

                    let client = client.client()?;

                    let (snapshot_path, snapshot_temp_path) =
                        snapshots::download::download_snapshot(&client, url, download_dir.path())
                            .await?;

                    (snapshot_path, snapshot_temp_path)
                }

                ShardSnapshotLocation::Path(path) => {
                    let snapshot_path = collection.get_shard_snapshot_path(shard_id, path).await?;
                    check_shard_snapshot_file_exists(&snapshot_path)?;
                    (snapshot_path, None)
                }
            };

            if let Some(checksum) = checksum {
                let snapshot_checksum = hash_file(&snapshot_path).await?;
                if snapshot_checksum != checksum {
                    return Err(StorageError::bad_input(format!(
                        "Snapshot checksum mismatch: expected {checksum}, got {snapshot_checksum}"
                    )));
                }
            }

            Result::<_, StorageError>::Ok((
                collection,
                download_dir,
                snapshot_path,
                snapshot_temp_path,
            ))
        };

        let (collection, _download_dir, snapshot_path, snapshot_temp_path) =
            cancel::future::cancel_on_token(cancel.clone(), future).await??;

        // `recover_shard_snapshot_impl` is *not* cancel safe
        let result = recover_shard_snapshot_impl(
            &toc,
            &collection,
            shard_id,
            &snapshot_path,
            snapshot_priority,
            cancel,
        )
        .await;

        // Remove snapshot after recovery if downloaded
        if let Some(path) = snapshot_temp_path {
            if let Err(err) = path.close() {
                log::error!("Failed to remove downloaded shards snapshot after recovery: {err}");
            }
        }

        result
    })
    .await??;

    Ok(())
}

/// # Cancel safety
///
/// This function is *not* cancel safe.
pub async fn recover_shard_snapshot_impl(
    toc: &TableOfContent,
    collection: &Collection,
    shard: ShardId,
    snapshot_path: &std::path::Path,
    priority: SnapshotPriority,
    cancel: cancel::CancellationToken,
) -> Result<(), StorageError> {
    // `Collection::restore_shard_snapshot` and `activate_shard` calls *have to* be executed as a
    // single transaction
    //
    // It is *possible* to make this function to be cancel safe, but it is *extremely tedious* to do so

    // `Collection::restore_shard_snapshot` is *not* cancel safe
    // (see `ShardReplicaSet::restore_local_replica_from`)
    collection
        .restore_shard_snapshot(
            shard,
            snapshot_path,
            toc.this_peer_id,
            toc.is_distributed(),
            &toc.optional_temp_or_snapshot_temp_path()?,
            cancel,
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
