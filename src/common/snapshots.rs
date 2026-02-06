use std::sync::Arc;

use collection::collection::Collection;
use collection::common::sha_256;
use collection::common::snapshot_stream::SnapshotStream;
use collection::operations::snapshot_ops::{
    ShardSnapshotLocation, SnapshotDescription, SnapshotPriority,
};
use collection::operations::verification::VerificationPass;
use collection::shards::replica_set::replica_set_state::ReplicaState;
use collection::shards::shard::ShardId;
use collection::shards::transfer::RecoveryStage;
use shard::snapshots::snapshot_data::SnapshotData;
use shard::snapshots::snapshot_manifest::{RecoveryType, SnapshotManifest};
use storage::content_manager::errors::StorageError;
use storage::content_manager::snapshots;
use storage::content_manager::snapshots::download_result::DownloadResult;
use storage::content_manager::toc::TableOfContent;
use storage::dispatcher::Dispatcher;
use storage::rbac::AccessRequirements;
use tokio::sync::OwnedRwLockWriteGuard;

use super::auth::Auth;
use super::http_client::HttpClient;

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn create_shard_snapshot(
    toc: Arc<TableOfContent>,
    auth: &Auth,
    collection_name: String,
    shard_id: ShardId,
) -> Result<SnapshotDescription, StorageError> {
    let collection_pass = auth.check_collection_access(
        &collection_name,
        AccessRequirements::new().write().extras(),
        "create_shard_snapshot",
    )?;
    let collection = toc.get_collection(&collection_pass).await?;

    let _telemetry_scope_guard = toc
        .snapshot_telemetry_collector(&collection_name)
        .running_snapshots
        .measure_scope();

    let snapshot = collection
        .create_shard_snapshot(shard_id, &toc.optional_temp_or_snapshot_temp_path()?)
        .await?;

    Ok(snapshot)
}

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn stream_shard_snapshot(
    toc: Arc<TableOfContent>,
    auth: &Auth,
    collection_name: String,
    shard_id: ShardId,
    manifest: Option<SnapshotManifest>,
) -> Result<SnapshotStream, StorageError> {
    let collection_pass = auth.check_collection_access(
        &collection_name,
        AccessRequirements::new().write().extras(),
        "stream_shard_snapshot",
    )?;

    let collection = toc.get_collection(&collection_pass).await?;

    let _telemetry_scope_guard = toc
        .snapshot_telemetry_collector(&collection_name)
        .running_snapshots
        .measure_scope();

    if let Some(old_manifest) = &manifest {
        let current_manifest = collection.get_partial_snapshot_manifest(shard_id).await?;

        // If `old_manifest` is *exactly* the same, as `current_manifest`, return specialized error
        // instead of creating partial snapshot.
        //
        // Snapshot manifest format is flexible, so it *might* be possible that manifests are *not*
        // exactly the same, but resulting partial snapshot will still be "empty", but:
        // - it should *not* happen in practice currently
        // - we intentionally use exact equality as the most "conservative" comparison, just in case
        if old_manifest == &current_manifest {
            return Err(StorageError::EmptyPartialSnapshot { shard_id });
        }
    }

    let snapshot_stream = toc
        .get_collection(&collection_pass)
        .await?
        .stream_shard_snapshot(
            shard_id,
            manifest,
            &toc.optional_temp_or_snapshot_temp_path()?,
        )
        .await?;

    Ok(snapshot_stream)
}

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn list_shard_snapshots(
    toc: Arc<TableOfContent>,
    auth: &Auth,
    collection_name: String,
    shard_id: ShardId,
) -> Result<Vec<SnapshotDescription>, StorageError> {
    let collection_pass = auth.check_collection_access(
        &collection_name,
        AccessRequirements::new().extras(),
        "list_shard_snapshots",
    )?;
    let collection = toc.get_collection(&collection_pass).await?;
    let snapshots = collection.list_shard_snapshots(shard_id).await?;
    Ok(snapshots)
}

/// # Cancel safety
///
/// This function is cancel safe.
pub async fn delete_shard_snapshot(
    toc: Arc<TableOfContent>,
    auth: &Auth,
    collection_name: String,
    shard_id: ShardId,
    snapshot_name: String,
) -> Result<(), StorageError> {
    let collection_pass = auth.check_collection_access(
        &collection_name,
        AccessRequirements::new().write().extras(),
        "delete_shard_snapshot",
    )?;
    let collection = toc.get_collection(&collection_pass).await?;
    let snapshot_manager = collection.get_snapshots_storage_manager()?;

    let snapshot_path = collection
        .shards_holder()
        .read()
        .await
        .get_shard_snapshot_path(collection.snapshots_path(), shard_id, &snapshot_name)
        .await?;

    tokio::spawn(async move { snapshot_manager.delete_snapshot(&snapshot_path).await }).await??;

    Ok(())
}

/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn recover_shard_snapshot(
    toc: Arc<TableOfContent>,
    auth: &Auth,
    collection_name: String,
    shard_id: ShardId,
    snapshot_location: ShardSnapshotLocation,
    snapshot_priority: SnapshotPriority,
    checksum: Option<String>,
    client: HttpClient,
    api_key: Option<String>,
) -> Result<(), StorageError> {
    let collection_pass = auth
        .check_global_access(AccessRequirements::new().manage(), "recover_shard_snapshot")?
        .issue_pass(&collection_name)
        .into_static();

    // - `recover_shard_snapshot_impl` is *not* cancel safe
    //   - but the task is *spawned* on the runtime and won't be cancelled, if request is cancelled

    cancel::future::spawn_cancel_on_drop(async move |cancel| {
        let pre_recovery_task = async {
            let collection = toc.get_collection(&collection_pass).await?;
            collection.assert_shard_exists(shard_id).await?;

            // Default temporary path to storage dir, to allow faster recovery within the same volume
            let download_dir = toc.optional_temp_or_storage_temp_path()?;
            Result::<_, StorageError>::Ok((collection, download_dir))
        };

        let (collection, download_dir) =
            cancel::future::cancel_on_token(cancel.clone(), pre_recovery_task).await??;

        // Once recovery tracking starts, `finish_shard_recovery` must run on all paths
        let recovery_progress = collection
            .shards_holder()
            .read()
            .await
            .start_shard_recovery(shard_id);

        let download_task = async {
            let DownloadResult {
                snapshot,
                hash
            } = match snapshot_location {
                ShardSnapshotLocation::Url(url) => {
                    if !matches!(url.scheme(), "http" | "https") {
                        let description = format!(
                            "Invalid snapshot URL {url}: URLs with {} scheme are not supported",
                            url.scheme(),
                        );

                        return Err(StorageError::bad_input(description));
                    }

                    recovery_progress
                        .lock()
                        .set_stage(RecoveryStage::Downloading);

                    let client = client.client(api_key.as_deref())?;
                    snapshots::download::download_snapshot(
                        &client,
                        url,
                        &download_dir,
                        checksum.is_some(),
                    )
                    .await?
                }

                ShardSnapshotLocation::Path(snapshot_file_name) => {
                    let snapshot_path = collection
                        .shards_holder()
                        .read()
                        .await
                        .get_shard_snapshot_path(
                            collection.snapshots_path(),
                            shard_id,
                            &snapshot_file_name,
                        )
                        .await?;

                    let snapshot_file = collection
                        .get_snapshots_storage_manager()?
                        .get_snapshot_file(&snapshot_path, &download_dir)
                        .await?;

                    let hash = if checksum.is_some() {
                        Some(sha_256::hash_file(&snapshot_path).await?)
                    } else {
                        None
                    };

                    DownloadResult {
                        snapshot: SnapshotData::Packed(snapshot_file),
                        hash,
                    }
                }
            };

            if let Some(checksum) = checksum {
                if let Some(snapshot_checksum) = hash {
                    if !sha_256::hashes_equal(&snapshot_checksum, &checksum) {
                        return Err(StorageError::bad_input(format!(
                            "Snapshot checksum mismatch: expected {checksum}, got {snapshot_checksum}"
                        )));
                    }
                } else {
                    return Err(StorageError::service_error(
                        "Snapshot checksum could not be verified".to_string(),
                    ));
                }
            }

            Ok(snapshot)
        };

        let snapshot_data =
            cancel::future::cancel_on_token(cancel.clone(), download_task).await??;

        // `recover_shard_snapshot_impl` is *not* cancel safe
        let result = recover_shard_snapshot_impl(
            &toc,
            &collection,
            shard_id,
            snapshot_data,
            snapshot_priority,
            RecoveryType::Full,
            cancel,
        )
        .await;

        // Finish tracking recovery progress
        collection
            .shards_holder()
            .read()
            .await
            .finish_shard_recovery(shard_id);

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
    snapshot_data: SnapshotData,
    priority: SnapshotPriority,
    recovery_type: RecoveryType,
    cancel: cancel::CancellationToken,
) -> Result<(), StorageError> {
    let _recover_tracker_guard = toc
        .snapshot_telemetry_collector(collection.name())
        .running_snapshot_recovery
        .measure_scope();

    // `Collection::restore_shard_snapshot` and `activate_shard` calls *have to* be executed as a
    // single transaction
    //
    // It is *possible* to make this function to be cancel safe, but it is *extremely tedious* to do so

    // TODO: `Collection::restore_shard_snapshot` *is* cancel-safe, but `recover_shard_snapshot_impl` is *not* cancel-safe (yet)
    collection
        .restore_shard_snapshot(
            shard,
            snapshot_data,
            recovery_type,
            toc.this_peer_id,
            toc.is_distributed(),
            // Default temporary path to storage dir, to allow faster recovery within the same volume
            &toc.optional_temp_or_storage_temp_path()?,
            cancel,
        )
        .await?
        .await?;

    let state = collection.state().await;
    let shard_info = state.shards.get(&shard).unwrap(); // TODO: Handle `unwrap`?..

    // TODO: Unify (and de-duplicate) "recovered shard state notification" logic in `_do_recover_from_snapshot` with this one!

    let other_active_replicas: Vec<_> = shard_info
        .replicas
        .iter()
        .map(|(&peer, &state)| (peer, state))
        .filter(|&(peer, state)| {
            // Check if there are *other* active replicas, after recovering shard snapshot.
            // This should include `ReshardingScaleDown` replicas.

            let is_active = matches!(
                state,
                ReplicaState::Active | ReplicaState::ReshardingScaleDown
            );

            peer != toc.this_peer_id && is_active
        })
        .collect();

    if other_active_replicas.is_empty() || recovery_type.is_partial() {
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
                        collection.name().to_string(),
                        peer,
                        shard,
                        ReplicaState::Dead,
                        None,
                    )?;
                }
            }

            SnapshotPriority::Replica => {
                toc.send_set_replica_state_proposal(
                    collection.name().to_string(),
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

pub async fn try_take_partial_snapshot_recovery_lock(
    dispatcher: &Dispatcher,
    collection_name: &str,
    shard_id: ShardId,
    auth: &Auth,
    pass: &VerificationPass,
) -> Result<Option<OwnedRwLockWriteGuard<()>>, StorageError> {
    let collection_pass = auth
        .check_global_access(
            AccessRequirements::new().manage(),
            "partial_snapshot_recovery_lock",
        )?
        .issue_pass(collection_name);

    let recovery_lock = dispatcher
        .toc(auth, pass)
        .get_collection(&collection_pass)
        .await?
        .try_take_partial_snapshot_recovery_lock(shard_id, RecoveryType::Partial)
        .await?;

    Ok(recovery_lock)
}
