use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use super::snapshot::transfer_snapshot;
use super::stream_records::transfer_stream_records;
use super::ShardTransferConsensus;
use crate::common::stoppable_task_async::{spawn_async_cancellable, CancellableAsyncTaskHandle};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::ReplicaState;
use crate::shards::shard::{PeerId, ShardId};
use crate::shards::shard_holder::{LockedShardHolder, ShardHolder};
use crate::shards::CollectionId;

const RETRY_DELAY: Duration = Duration::from_secs(1);
pub(crate) const MAX_RETRY_COUNT: usize = 3;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransfer {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
    /// If this flag is true, this is a replication related transfer of shard from 1 peer to another
    /// Shard on original peer will not be deleted in this case
    pub sync: bool,
    /// Method to transfer shard with. `None` to choose automatically.
    #[serde(default)]
    pub method: Option<ShardTransferMethod>,
}

/// Unique identifier of a transfer, agnostic of transfer method
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ShardTransferKey {
    pub shard_id: ShardId,
    pub from: PeerId,
    pub to: PeerId,
}

impl ShardTransferKey {
    pub fn check(&self, transfer: &ShardTransfer) -> bool {
        self.shard_id == transfer.shard_id && self.from == transfer.from && self.to == transfer.to
    }
}

impl ShardTransfer {
    pub fn key(&self) -> ShardTransferKey {
        ShardTransferKey {
            shard_id: self.shard_id,
            from: self.from,
            to: self.to,
        }
    }
}

/// Methods for transferring a shard from one node to another.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShardTransferMethod {
    /// Stream all shard records in batches until the whole shard is transferred.
    #[default]
    StreamRecords,
    /// Snapshot the shard, transfer and restore it on the receiver.
    Snapshot,
}

/// # Cancel safety
///
/// This function is cancel safe.
#[allow(clippy::too_many_arguments)]
pub async fn transfer_shard(
    transfer_config: ShardTransfer,
    shard_holder: Arc<LockedShardHolder>,
    consensus: &dyn ShardTransferConsensus,
    collection_id: CollectionId,
    collection_name: &str,
    channel_service: ChannelService,
    snapshots_path: &Path,
    temp_dir: &Path,
) -> CollectionResult<()> {
    let shard_id = transfer_config.shard_id;

    // Initiate shard on a remote peer
    let remote_shard = RemoteShard::new(
        shard_id,
        collection_id.clone(),
        transfer_config.to,
        channel_service.clone(),
    );

    // Prepare the remote for receiving the shard, waits for the correct state on the remote
    remote_shard.initiate_transfer().await?;

    match transfer_config.method.unwrap_or_default() {
        // Transfer shard record in batches
        ShardTransferMethod::StreamRecords => {
            transfer_stream_records(shard_holder.clone(), shard_id, remote_shard).await?;
        }

        // Transfer shard as snapshot
        ShardTransferMethod::Snapshot => {
            transfer_snapshot(
                transfer_config,
                shard_holder.clone(),
                shard_id,
                remote_shard,
                channel_service,
                consensus,
                snapshots_path,
                collection_name,
                temp_dir,
            )
            .await?;
        }
    }

    Ok(())
}

/// Return local shard back from the forward proxy
///
/// # Cancel safety
///
/// This function is cancel safe.
pub async fn revert_proxy_shard_to_local(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    // Revert queue proxy if we still have any and forget all collected updates
    replica_set.revert_queue_proxy_local().await;

    // Un-proxify local shard
    replica_set.un_proxify_local().await?;

    Ok(true)
}

pub async fn change_remote_shard_route(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
    old_peer_id: PeerId,
    new_peer_id: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    if replica_set.this_peer_id() != new_peer_id {
        replica_set
            .add_remote(new_peer_id, ReplicaState::Active)
            .await?;
    }

    if !sync {
        // Transfer was a move, we need to remove the old peer
        replica_set.remove_remote(old_peer_id).await?;
    }
    Ok(true)
}

/// Mark partial shard as ready
///
/// Returns `true` if the shard was promoted, `false` if the shard was not found.
pub async fn finalize_partial_shard(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
) -> CollectionResult<bool> {
    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    if !replica_set.has_local_shard().await {
        return Ok(false);
    }

    replica_set.set_replica_state(&replica_set.this_peer_id(), ReplicaState::Active)?;
    Ok(true)
}

/// Promotes wrapped local shard to remote shard
///
/// Returns true if the shard was promoted, false if it was already handled
pub async fn handle_transferred_shard_proxy(
    shard_holder: &ShardHolder,
    shard_id: ShardId,
    to: PeerId,
    sync: bool,
) -> CollectionResult<bool> {
    // TODO: Ensure cancel safety!

    let replica_set = match shard_holder.get_shard(&shard_id) {
        None => return Ok(false),
        Some(replica_set) => replica_set,
    };

    replica_set.add_remote(to, ReplicaState::Active).await?;

    if sync {
        // Keep local shard in the replica set
        replica_set.un_proxify_local().await?;
    } else {
        // Remove local proxy
        //
        // TODO: Ensure cancel safety!
        replica_set.remove_local().await?;
    }

    Ok(true)
}
