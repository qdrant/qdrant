use std::path::Path;

use collection::collection::Collection;
use collection::config::CollectionConfig;
use collection::operations::snapshot_ops::SnapshotRecover;
use collection::shards::replica_set::ReplicaState;
use collection::shards::shard_config::ShardType;
use collection::shards::shard_versioning::latest_shard_paths;

use crate::content_manager::snapshots::download::{download_snapshot, downloaded_snapshots_dir};
use crate::{StorageError, TableOfContent};

pub async fn do_recover_from_snapshot(
    toc: &TableOfContent,
    collection_name: &str,
    source: SnapshotRecover,
) -> Result<bool, StorageError> {
    let SnapshotRecover { location } = source;

    let collection = toc.get_collection(collection_name).await?;

    let snapshot_download_path = downloaded_snapshots_dir(toc.snapshots_path());
    tokio::fs::create_dir_all(&snapshot_download_path).await?;

    log::debug!(
        "Downloading snapshot from {} to {}",
        location,
        snapshot_download_path.display()
    );

    let snapshot_path = download_snapshot(location, &snapshot_download_path).await?;

    log::debug!("Snapshot downloaded to {}", snapshot_path.display());

    let tmp_collection_dir = Path::new(toc.storage_path())
        .join("tmp_collections")
        .join(collection_name);

    log::debug!(
        "Recovering collection {} from snapshot {}",
        collection_name,
        snapshot_path.display()
    );

    if tmp_collection_dir.exists() {
        tokio::fs::remove_dir_all(&tmp_collection_dir).await?;
    }
    tokio::fs::create_dir_all(&tmp_collection_dir).await?;

    log::debug!("Unpacking snapshot to {}", tmp_collection_dir.display());

    // Unpack snapshot collection to the target folder
    Collection::restore_snapshot(&snapshot_path, &tmp_collection_dir)?;

    let snapshot_config = CollectionConfig::load(&tmp_collection_dir)?;

    let state = collection.state().await;

    // Check config compatibility
    // Check vectors config
    if snapshot_config.params.vectors != state.config.params.vectors {
        return Err(StorageError::bad_input(&format!(
            "Snapshot is not compatible with existing collection: Collection vectors: {:?} Snapshot Vectors: {:?}",
            state.config.params.vectors, snapshot_config.params.vectors
        )));
    }
    // Check shard number
    if snapshot_config.params.shard_number != state.config.params.shard_number {
        return Err(StorageError::bad_input(&format!(
            "Snapshot is not compatible with existing collection: Collection shard number: {:?} Snapshot shard number: {:?}",
            state.config.params.shard_number, snapshot_config.params.shard_number
        )));
    }

    // Deactivate collection local shards during recovery
    let this_peer_id = toc.this_peer_id;

    for (shard_id, shard_info) in &state.shards {
        if shard_info.replicas.get(&this_peer_id) != Some(&ReplicaState::Partial) {
            toc.send_set_replica_state_proposal(
                collection_name.to_string(),
                this_peer_id,
                *shard_id,
                ReplicaState::Partial,
            )?;
        }
    }

    // Recover shards from the snapshot
    for (shard_id, shard_info) in &state.shards {
        let shards = latest_shard_paths(&tmp_collection_dir, *shard_id).await?;

        let snapshot_shard_path = shards
            .into_iter()
            .filter_map(
                |(snapshot_shard_path, _version, shard_type)| match shard_type {
                    ShardType::Local => Some(snapshot_shard_path),
                    ShardType::ReplicaSet => Some(snapshot_shard_path),
                    ShardType::Remote { .. } => None,
                    ShardType::Temporary => None,
                },
            )
            .next();

        if let Some(snapshot_shard_path) = snapshot_shard_path {
            log::debug!(
                "Recovering shard {} from {}",
                shard_id,
                snapshot_shard_path.display()
            );

            let recovered = collection
                .recover_local_shard_from(&snapshot_shard_path, *shard_id)
                .await?;

            if !recovered {
                log::debug!("Shard {} if not in snapshot", shard_id);
                continue;
            }

            // If this is the only replica, we can activate it
            // If not - de-sync is possible, so we need to run synchronization
            let other_active_replicas: Vec<_> = shard_info
                .replicas
                .iter()
                .filter(|(peer_id, state)| {
                    *state == &ReplicaState::Active && **peer_id != this_peer_id
                })
                .collect();

            if other_active_replicas.is_empty() {
                // No other active replicas, we can activate this shard
                // as there is no de-sync possible
                if toc.is_distributed() {
                    log::debug!(
                        "Activating shard {} of collection {} with consensus",
                        shard_id,
                        collection_name
                    );
                    toc.send_set_replica_state_proposal(
                        collection_name.to_string(),
                        this_peer_id,
                        *shard_id,
                        ReplicaState::Active,
                    )?;
                } else {
                    log::debug!(
                        "Activating shard {} of collection {} locally",
                        shard_id,
                        collection_name
                    );
                    collection
                        .set_shard_replica_state(*shard_id, this_peer_id, ReplicaState::Active)
                        .await?;
                }
            } else {
                let (replica_peer_id, _state) = other_active_replicas.into_iter().next().unwrap();
                log::debug!(
                    "Running synchronization for shard {} of collection {} from {}",
                    shard_id,
                    collection_name,
                    replica_peer_id
                );

                // assume that if there is another peers, the server is distributed
                toc.request_shard_transfer(
                    collection_name.to_string(),
                    *shard_id,
                    *replica_peer_id,
                    this_peer_id,
                    true,
                )?;
            }
        }
    }

    // Remove tmp collection dir
    tokio::fs::remove_dir_all(&tmp_collection_dir).await?;

    Ok(true)
}
