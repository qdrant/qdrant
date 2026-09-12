use std::collections::{HashMap, HashSet};

use collection::collection::Collection;
use collection::collection::payload_index_schema::PayloadIndexSchema;
use collection::common::sha_256::hashes_equal;
use collection::config::{CollectionConfigInternal, ShardingMethod};
use collection::operations::snapshot_ops::{SnapshotPriority, SnapshotRecover};
use collection::operations::verification::new_unchecked_verification_pass;
use collection::shards::check_shard_path;
use collection::shards::replica_set::replica_set_state::{
    MANUAL_RECOVERY_SHARD_STATE_VERSION, ReplicaSetState, ReplicaState,
};
use collection::shards::shard::{PeerId, ShardId};
use collection::shards::shard_holder::SHARD_KEY_MAPPING_FILE;
use common::fs::read_json;
use common::save_on_disk::SaveOnDisk;
use fs_err::tokio as tokio_fs;
use segment::types::ShardKey;
use shard::files::PAYLOAD_INDEX_CONFIG_FILE;
use shard::snapshots::snapshot_manifest::RecoveryType;

use crate::content_manager::collection_meta_ops::{
    CollectionMetaOperations, CreateCollectionOperation, CreatePayloadIndex, CreateShardKey,
};
use crate::content_manager::snapshots::download::download_snapshot;
use crate::content_manager::snapshots::download_result::DownloadResult;
use crate::dispatcher::Dispatcher;
use crate::rbac::{AccessRequirements, Auth, CollectionPass};
use crate::{StorageError, TableOfContent};

#[derive(serde::Deserialize)]
struct SnapshotShardKeyMappingEntry {
    key: ShardKey,
    shard_ids: HashSet<ShardId>,
}

struct SnapshotShardKeyPlan {
    shard_key: ShardKey,
    placement: Vec<Vec<PeerId>>,
}

async fn snapshot_shard_placement(
    snapshot_collection_dir: &std::path::Path,
    shard_ids: &[ShardId],
) -> Result<Vec<Vec<PeerId>>, StorageError> {
    let mut placement = Vec::with_capacity(shard_ids.len());

    for shard_id in shard_ids {
        let shard_path = check_shard_path(snapshot_collection_dir, *shard_id).await?;
        let replica_state_path = shard_path.join("replica_state.json");

        let replica_state: SaveOnDisk<ReplicaSetState> =
            SaveOnDisk::load_or_init_default(&replica_state_path).map_err(|err| {
                StorageError::service_error(format!(
                    "Failed to load snapshot replica state from {}: {err}",
                    replica_state_path.display(),
                ))
            })?;
        let mut replicas: Vec<_> = replica_state.read().peers().keys().copied().collect();
        replicas.sort_unstable();

        if replicas.is_empty() {
            return Err(StorageError::service_error(format!(
                "Snapshot shard {shard_id} has no replicas in replica_state.json",
            )));
        }

        placement.push(replicas);
    }

    Ok(placement)
}

async fn snapshot_shard_key_plan(
    snapshot_collection_dir: &std::path::Path,
) -> Result<Vec<SnapshotShardKeyPlan>, StorageError> {
    let snapshot_mapping_path = snapshot_collection_dir.join(SHARD_KEY_MAPPING_FILE);
    if !snapshot_mapping_path.exists() {
        return Ok(Vec::new());
    }

    let snapshot_shard_mapping: Vec<SnapshotShardKeyMappingEntry> =
        read_json(&snapshot_mapping_path)?;

    let mut shard_keys: Vec<_> = snapshot_shard_mapping
        .iter()
        .map(|entry| {
            let shard_key = entry.key.clone();
            let mut shard_ids = entry.shard_ids.iter().copied().collect::<Vec<_>>();
            shard_ids.sort_unstable();
            (shard_key, shard_ids)
        })
        .collect();
    shard_keys.sort_by_key(|(_, shard_ids)| shard_ids.first().copied().unwrap_or(0));

    let mut plans = Vec::with_capacity(shard_keys.len());
    for (shard_key, shard_ids) in shard_keys {
        if shard_ids.is_empty() {
            return Err(StorageError::service_error(format!(
                "Snapshot shard mapping for key {shard_key} has no shard ids",
            )));
        }

        let placement = snapshot_shard_placement(snapshot_collection_dir, &shard_ids).await?;
        plans.push(SnapshotShardKeyPlan {
            shard_key,
            placement,
        });
    }

    Ok(plans)
}

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
            collection.name()
        );
        toc.send_set_replica_state_proposal(
            collection.name().to_string(),
            peer_id,
            *shard_id,
            ReplicaState::Active,
            None,
        )?;
    } else {
        log::debug!(
            "Activating shard {} of collection {} locally",
            shard_id,
            collection.name()
        );
        collection
            .set_shard_replica_state(*shard_id, peer_id, ReplicaState::Active, None)
            .await?;
    }
    Ok(())
}

/// # Cancel safety
///
/// This method is cancel safe.
pub async fn do_recover_from_snapshot(
    dispatcher: &Dispatcher,
    collection_name: &str,
    source: SnapshotRecover,
    auth: Auth,
    client: reqwest::Client,
) -> Result<bool, StorageError> {
    let multipass =
        auth.check_global_access(AccessRequirements::new().manage(), "recover_from_snapshot")?;

    let dispatcher = dispatcher.clone();
    let collection_pass = multipass.issue_pass(collection_name).into_static();

    let toc = dispatcher
        .toc(&auth, &new_unchecked_verification_pass())
        .clone();

    let res = toc
        .general_runtime_handle()
        .spawn(async move {
            _do_recover_from_snapshot(dispatcher, auth, collection_pass, source, &client).await
        })
        .await??;

    Ok(res)
}

/// # Cancel safety
///
/// This method is *not* cancel safe.
async fn _do_recover_from_snapshot(
    dispatcher: Dispatcher,
    auth: Auth,
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

    let toc = dispatcher.toc(&auth, &pass);

    // Measure this scope for metrics/telemetry.
    // (This must be a named variable so it doesn't get dropped prematurely!)
    let _measure_guard = toc
        .snapshot_telemetry_collector(collection_pass.name())
        .running_snapshot_recovery
        .measure_scope();

    let this_peer_id = toc.this_peer_id;

    let is_distributed = toc.is_distributed();

    let DownloadResult {
        snapshot: snapshot_data,
        hash: snapshot_hash,
    } = download_snapshot(
        client,
        location,
        // Default temporary path to storage dir, to allow faster recovery within the same volume
        &toc.optional_temp_or_storage_temp_path()?,
        toc.snapshots_path(),
        checksum.is_some(),
    )
    .await?;

    if let Some(checksum) = checksum {
        let Some(snapshot_checksum) = snapshot_hash else {
            return Err(StorageError::service_error(
                "Snapshot checksum was not computed during download",
            ));
        };
        if !hashes_equal(&snapshot_checksum, &checksum) {
            return Err(StorageError::bad_input(format!(
                "Snapshot checksum mismatch: expected {checksum}, got {snapshot_checksum}"
            )));
        }
    }

    let temp_storage_path = toc.optional_temp_or_storage_temp_path()?;

    let tmp_collection_dir = tempfile::Builder::new()
        .prefix(&format!("col-{collection_pass}-recovery-"))
        .tempdir_in(temp_storage_path)?;

    let tmp_collection_dir_clone = tmp_collection_dir.path().to_path_buf();

    let restoring = tokio::task::spawn_blocking(move || {
        Collection::restore_snapshot(
            snapshot_data,
            &tmp_collection_dir_clone,
            this_peer_id,
            is_distributed,
        )?;
        common::fs::bulk_sync_dir(&tmp_collection_dir_clone)?;
        Ok::<(), StorageError>(())
    });
    restoring.await??;

    let snapshot_config = CollectionConfigInternal::load(tmp_collection_dir.path())?;
    snapshot_config.validate_and_warn();

    let payload_index_file = tmp_collection_dir.path().join(PAYLOAD_INDEX_CONFIG_FILE);

    let payload_schema: SaveOnDisk<PayloadIndexSchema> =
        SaveOnDisk::load_or_init_default(&payload_index_file).map_err(|err| {
            StorageError::service_error(format!(
                "Failed to load payload index schema from {payload_index_file:?}: {err}"
            ))
        })?;

    let schema = payload_schema.read().schema.clone();

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
                .submit_collection_meta_op(operation, auth.clone(), None)
                .await?;

            // Since we not just copy files into a collection dir,
            // but create collection in consensus and then copy data into recreated collection,
            // we also need to register all associated payload indexes in consensus.
            for (field_name, field_schema) in schema.iter() {
                let consensus_op =
                    CollectionMetaOperations::CreatePayloadIndex(CreatePayloadIndex {
                        collection_name: collection_pass.to_string(),
                        field_name: field_name.clone(),
                        field_schema: field_schema.clone(),
                    });

                dispatcher
                    .submit_collection_meta_op(consensus_op, auth.clone(), None)
                    .await?;
            }

            toc.get_collection(&collection_pass).await?
        }
    };

    let mut state = collection.state().await;

    // For custom sharding, initialize or reconcile shard keys from snapshot metadata.
    // The whole plan is validated first (mapping + placements), then missing keys are created.
    if snapshot_config.params.sharding_method.unwrap_or_default() == ShardingMethod::Custom {
        let shard_key_plan = snapshot_shard_key_plan(tmp_collection_dir.path()).await?;
        let snapshot_keys: HashSet<_> = shard_key_plan
            .iter()
            .map(|plan| plan.shard_key.clone())
            .collect();
        let existing_keys: HashSet<_> = state.shards_key_mapping.keys().cloned().collect();

        let unexpected_keys: Vec<_> = existing_keys.difference(&snapshot_keys).cloned().collect();
        if !unexpected_keys.is_empty() {
            return Err(StorageError::bad_input(format!(
                "Snapshot is not compatible with existing collection: extra custom shard keys in target collection: {unexpected_keys:?}",
            )));
        }

        let missing_keys = shard_key_plan
            .into_iter()
            .filter(|plan| !existing_keys.contains(&plan.shard_key))
            .collect::<Vec<_>>();

        for plan in missing_keys {
            let consensus_op = CollectionMetaOperations::CreateShardKey(CreateShardKey {
                collection_name: collection_pass.to_string(),
                shard_key: plan.shard_key,
                placement: plan.placement,
                initial_state: None,
            });

            dispatcher
                .submit_collection_meta_op(consensus_op, auth.clone(), None)
                .await?;
        }

        state = collection.state().await;
    }

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

    let is_manual_recovery_state_supported = toc
        .get_channel_service()
        .all_peers_at_version(&MANUAL_RECOVERY_SHARD_STATE_VERSION);

    let recovery_state = if is_manual_recovery_state_supported {
        ReplicaState::ManualRecovery
    } else {
        ReplicaState::Partial
    };

    let local_states_before_recovery: HashMap<_, _> = state
        .shards
        .iter()
        .filter_map(|(&shard_id, shard_info)| {
            shard_info
                .replicas
                .get(&this_peer_id)
                .copied()
                .map(|replica_state| (shard_id, replica_state))
        })
        .collect();

    // Deactivate collection local shards during recovery
    for (shard_id, shard_info) in &state.shards {
        let local_shard_state = shard_info.replicas.get(&this_peer_id);
        match local_shard_state {
            Some(state) if state != &recovery_state => {
                toc.send_set_replica_state_proposal(
                    collection_pass.to_string(),
                    this_peer_id,
                    *shard_id,
                    recovery_state,
                    None,
                )?;
            }
            Some(_) | None => {} // Shard is not on this node, skip
        }
    }

    let priority = priority.unwrap_or_default();

    // Recover shards from the snapshot
    for (shard_id, shard_info) in &state.shards {
        let snapshot_shard_path = check_shard_path(tmp_collection_dir.path(), *shard_id).await?;
        log::debug!(
            "Recovering shard {} from {}",
            shard_id,
            snapshot_shard_path.display(),
        );

        // TODO:
        //   `_do_recover_from_snapshot` is not *yet* analyzed/organized for cancel safety,
        //   but `recover_local_shard_from` requires `cancel::CanellationToken` argument *now*,
        //   so we provide a token that is never triggered (in this case `recover_local_shard_from`
        //   works *exactly* as before the `cancel::CancellationToken` parameter was added to it)
        let recovered = collection
            .recover_local_shard_from(
                &snapshot_shard_path,
                RecoveryType::Full,
                *shard_id,
                cancel::CancellationToken::new(),
            )
            .await?;

        if !recovered {
            log::debug!("Shard {shard_id} is not in snapshot");

            // This peer may have been switched into recovery state before restore. If the snapshot
            // does not contain local data for this shard, revert to previous state so the replica
            // is not left stuck in `Partial`/`ManualRecovery`.
            if let Some(previous_state) = local_states_before_recovery.get(shard_id).copied()
                && previous_state != recovery_state
            {
                if toc.is_distributed() {
                    toc.send_set_replica_state_proposal(
                        collection_pass.to_string(),
                        this_peer_id,
                        *shard_id,
                        previous_state,
                        Some(recovery_state),
                    )?;
                } else {
                    collection
                        .set_shard_replica_state(
                            *shard_id,
                            this_peer_id,
                            previous_state,
                            Some(recovery_state),
                        )
                        .await?;
                }
            }

            continue;
        }

        // Staging delay: Allow observing Partial state before activation
        #[cfg(feature = "staging")]
        {
            let delay_secs: f64 = std::env::var("QDRANT__STAGING__SNAPSHOT_RECOVERY_DELAY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            if delay_secs > 0.0 {
                log::debug!(
                    "Staging: Delaying shard {shard_id} activation for {delay_secs}s (shard is in Partial state)"
                );
                tokio::time::sleep(std::time::Duration::from_secs_f64(delay_secs)).await;
                log::debug!("Staging: Delay complete, proceeding with activation");
            }
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
                        "Running synchronization for shard {shard_id} of collection {collection_pass} from {replica_peer_id}",
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

    // Explicitly trigger optimizers for the collection we have recovered. This prevents them from
    // remaining in grey state if the snapshot is not optimized.
    // See: <https://github.com/qdrant/qdrant/issues/5139>
    collection.trigger_optimizers().await;

    // Remove tmp collection dir
    tokio_fs::remove_dir_all(&tmp_collection_dir).await?;

    Ok(true)
}
