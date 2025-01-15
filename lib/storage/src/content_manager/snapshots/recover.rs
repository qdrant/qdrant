use collection::collection::Collection;
use collection::common::sha_256::{hash_file, hashes_equal};
use collection::config::CollectionConfigInternal;
use collection::operations::snapshot_ops::{SnapshotPriority, SnapshotRecover};
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::shard_config::ShardType;
use collection::shards::shard_versioning::latest_shard_paths;

use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollectionOperation,
};
use crate::content_manager::snapshots::download::download_snapshot;
use crate::dispatcher::Dispatcher;
use crate::rbac::{Access, AccessRequirements, CollectionPass};
use crate::{StorageError, TableOfContent};

pub async fn activate_shard(
    toc: &TableOfContent,
    collection: &Collection,
    peer_id: PeerId,
    shard_id: &ShardId,
) -> Result<(), StorageError> {
    if toc.is_distributed() {
        log::debug!(
            "Activating shard {} of collection {} with consensus",
            shard_id,
            &collection.name()
        );
        toc.send_set_replica_state_proposal(
            collection.name(),
            peer_id,
            *shard_id,
            ReplicaState::Active,
            None,
        )?;
    } else {
        log::debug!(
            "Activating shard {} of collection {} locally",
            shard_id,
            &collection.name()
        );
        collection
            .set_shard_replica_state(*shard_id, peer_id, ReplicaState::Active, None)
            .await?;
    }
    Ok(())
}

pub async fn do_recover_from_snapshot(
    dispatcher: &Dispatcher,
    collection_name: &str,
    source: SnapshotRecover,
    access: Access,
    client: reqwest::Client,
) -> Result<bool, StorageError> {
    let multipass = access.check_global_access(AccessRequirements::new().manage())?;

    let dispatcher = dispatcher.clone();
    let collection_pass = multipass.issue_pass(collection_name).into_static();

    let res = tokio::spawn(async move {
        _do_recover_from_snapshot(dispatcher, access, collection_pass, source, &client).await
    })
    .await??;

    Ok(res)
}

async fn _do_recover_from_snapshot(
    dispatcher: Dispatcher,
    access: Access,
    collection_pass: CollectionPass<'static>,
    source: SnapshotRecover,
    client: &reqwest::Client,
) -> Result<bool, StorageError> {
    let SnapshotRecover {
        location,
        priority,
        checksum,
        api_key: _,
    } = source;

    // All checks should've been done at this point.
    let pass = new_unchecked_verification_pass();

    let toc = dispatcher.toc(&access, &pass);

    let this_peer_id = toc.this_peer_id;

    let is_distributed = toc.is_distributed();

    let snapshot_path = download_snapshot(
        client,
        location,
        &toc.optional_temp_or_snapshot_temp_path()?,
    )
    .await?;

    if let Some(checksum) = checksum {
        let snapshot_checksum = hash_file(&snapshot_path).await?;
        if !hashes_equal(&snapshot_checksum, &checksum) {
            return Err(StorageError::bad_input(format!(
                "Snapshot checksum mismatch: expected {checksum}, got {snapshot_checksum}"
            )));
        }
    }

    log::debug!("Snapshot downloaded to {}", snapshot_path.display());

    let temp_storage_path = toc.optional_temp_or_storage_temp_path()?;

    let tmp_collection_dir = tempfile::Builder::new()
        .prefix(&format!("col-{collection_pass}-recovery-"))
        .tempdir_in(temp_storage_path)?;

    log::debug!(
        "Recovering collection {collection_pass} from snapshot {}",
        snapshot_path.display(),
    );

    log::debug!(
        "Unpacking snapshot to {}",
        tmp_collection_dir.path().display(),
    );

    let tmp_collection_dir_clone = tmp_collection_dir.path().to_path_buf();
    let snapshot_path_clone = snapshot_path.to_path_buf();
    let restoring = tokio::task::spawn_blocking(move || {
        Collection::restore_snapshot(
            &snapshot_path_clone,
            &tmp_collection_dir_clone,
            this_peer_id,
            is_distributed,
        )
    });
    restoring.await??;

    let snapshot_config = CollectionConfigInternal::load(tmp_collection_dir.path())?;
    snapshot_config.validate_and_warn();

    let collection = match toc.get_collection(&collection_pass).await.ok() {
        Some(collection) => collection,
        None => {
            log::debug!("Collection {collection_pass} does not exist, creating it");
            let operation =
                CollectionMetaOperations::CreateCollection(CreateCollectionOperation::new(
                    collection_pass.to_string(),
                    snapshot_config.clone().into(),
                )?);
            dispatcher
                .submit_collection_meta_op(operation, access, None)
                .await?;
            toc.get_collection(&collection_pass).await?
        }
    };

    let state = collection.state().await;

    // Check config compatibility
    // Check vectors config
    if snapshot_config.params.vectors != state.config.params.vectors {
        return Err(StorageError::bad_input(format!(
            "Snapshot is not compatible with existing collection: Collection vectors: {:?} Snapshot Vectors: {:?}",
            state.config.params.vectors, snapshot_config.params.vectors
        )));
    }
    // Check shard number
    if snapshot_config.params.shard_number != state.config.params.shard_number {
        return Err(StorageError::bad_input(format!(
            "Snapshot is not compatible with existing collection: Collection shard number: {:?} Snapshot shard number: {:?}",
            state.config.params.shard_number, snapshot_config.params.shard_number
        )));
    }

    // Deactivate collection local shards during recovery
    for (shard_id, shard_info) in &state.shards {
        let local_shard_state = shard_info.replicas.get(&this_peer_id);
        match local_shard_state {
            None => {} // Shard is not on this node, skip
            Some(state) => {
                if state != &ReplicaState::Partial {
                    toc.send_set_replica_state_proposal(
                        collection_pass.to_string(),
                        this_peer_id,
                        *shard_id,
                        ReplicaState::Partial,
                        None,
                    )?;
                }
            }
        }
    }

    let priority = priority.unwrap_or_default();

    // Recover shards from the snapshot
    for (shard_id, shard_info) in &state.shards {
        let shards = latest_shard_paths(tmp_collection_dir.path(), *shard_id).await?;

        let snapshot_shard_path =
            shards
                .into_iter()
                .find_map(
                    |(snapshot_shard_path, _version, shard_type)| match shard_type {
                        ShardType::Local => Some(snapshot_shard_path),
                        ShardType::ReplicaSet => Some(snapshot_shard_path),
                        ShardType::Remote { .. } => None,
                        ShardType::Temporary => None,
                    },
                );

        if let Some(snapshot_shard_path) = snapshot_shard_path {
            log::debug!(
                "Recovering shard {} from {}",
                shard_id,
                snapshot_shard_path.display()
            );

            // TODO:
            //   `_do_recover_from_snapshot` is not *yet* analyzed/organized for cancel safety,
            //   but `recover_local_shard_from` requires `cancel::CanellationToken` argument *now*,
            //   so we provide a token that is never triggered (in this case `recover_local_shard_from`
            //   works *exactly* as before the `cancel::CancellationToken` parameter was added to it)
            let recovered = collection
                .recover_local_shard_from(
                    &snapshot_shard_path,
                    *shard_id,
                    cancel::CancellationToken::new(),
                )
                .await?;

            if !recovered {
                log::debug!("Shard {} is not in snapshot", shard_id);
                continue;
            }

            // If this is the only replica, we can activate it
            // If not - de-sync is possible, so we need to run synchronization
            let other_active_replicas: Vec<_> = shard_info
                .replicas
                .iter()
                .filter(|&(&peer_id, &state)| {
                    // Check if there are *other* active replicas, after recovering collection snapshot.
                    // This should include `ReshardingScaleDown` replicas.

                    let is_active = matches!(
                        state,
                        ReplicaState::Active | ReplicaState::ReshardingScaleDown
                    );

                    peer_id != this_peer_id && is_active
                })
                .collect();

            if other_active_replicas.is_empty() {
                // No other active replicas, we can activate this shard
                // as there is no de-sync possible
                activate_shard(toc, &collection, this_peer_id, shard_id).await?;
            } else {
                match priority {
                    SnapshotPriority::NoSync => {
                        activate_shard(toc, &collection, this_peer_id, shard_id).await?;
                    }

                    SnapshotPriority::Snapshot => {
                        // Snapshot is the source of truth, we need to remove all other replicas
                        activate_shard(toc, &collection, this_peer_id, shard_id).await?;

                        let replicas_to_keep = state.config.params.replication_factor.get() - 1;
                        let mut replicas_to_remove = other_active_replicas
                            .len()
                            .saturating_sub(replicas_to_keep as usize);

                        for (peer_id, _) in other_active_replicas {
                            if replicas_to_remove > 0 {
                                // Keep this replica
                                replicas_to_remove -= 1;

                                // Don't need more replicas, remove this one
                                toc.request_remove_replica(
                                    collection_pass.to_string(),
                                    *shard_id,
                                    *peer_id,
                                )?;
                            } else {
                                toc.send_set_replica_state_proposal(
                                    collection_pass.to_string(),
                                    *peer_id,
                                    *shard_id,
                                    ReplicaState::Dead,
                                    None,
                                )?;
                            }
                        }
                    }

                    SnapshotPriority::Replica => {
                        // Replica is the source of truth, we need to sync recovered data with this replica
                        let (replica_peer_id, _state) =
                            other_active_replicas.into_iter().next().unwrap();
                        log::debug!(
                            "Running synchronization for shard {} of collection {} from {}",
                            shard_id,
                            collection_pass,
                            replica_peer_id
                        );

                        // assume that if there is another peers, the server is distributed
                        toc.request_shard_transfer(
                            collection_pass.to_string(),
                            *shard_id,
                            *replica_peer_id,
                            this_peer_id,
                            true,
                            None,
                        )?;
                    }

                    // `ShardTransfer` is only used during snapshot *shard transfer*.
                    // It is only exposed in internal gRPC API and only used for *shard* snapshot recovery.
                    SnapshotPriority::ShardTransfer => unreachable!(),
                }
            }
        }
    }

    // Explicitly trigger optimizers for the collection we have recovered. This prevents them from
    // remaining in grey state if the snapshot is not optimized.
    // See: <ttps://github.com/qdrant/qdrant/issues/5139>
    collection.trigger_optimizers().await;

    // Remove tmp collection dir
    tokio::fs::remove_dir_all(&tmp_collection_dir).await?;

    // Remove snapshot after recovery if downloaded
    if let Err(err) = snapshot_path.close() {
        log::error!("Failed to remove downloaded collection snapshot after recovery: {err}");
    }

    Ok(true)
}
