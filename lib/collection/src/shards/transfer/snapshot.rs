use std::path::Path;
use std::sync::Arc;

use common::defaults;
use parking_lot::Mutex;
use semver::Version;
use tempfile::TempPath;

use super::transfer_tasks_pool::TransferTaskProgress;
use super::{ShardTransfer, ShardTransferConsensus, TransferStage};
use crate::operations::snapshot_ops::{SnapshotPriority, get_checksum_path};
use crate::operations::types::{CollectionError, CollectionResult};
use crate::shards::CollectionId;
use crate::shards::channel_service::ChannelService;
use crate::shards::remote_shard::RemoteShard;
use crate::shards::replica_set::replica_set_state::ReplicaState;
use crate::shards::shard::ShardId;
use crate::shards::shard_holder::SharedShardHolder;

/// Orchestrate shard snapshot transfer
///
/// This is called on the sender and will arrange all that is needed for the shard snapshot
/// transfer process to a receiver.
///
/// The order of operations here is critical for correctness. Explicit synchronization across nodes
/// is used to ensure data consistency.
///
/// Before this function, this has happened:
///
/// - An empty shard is initialized on the remote
/// - Set the remote shard state to `PartialSnapshot`
///   In `PartialSnapshot` state, the remote shard will ignore all operations and other nodes will
///   prevent sending operations to it. This is critical not to modify the shard while it is being
///   recovered from the snapshot.
///
/// During this function, this happens in order:
///
/// - Queue proxy local shard
///   We queue all new operations to the shard for the remote. Once the remote is ready, we can
///   transfer all these operations to it.
/// - Create shard snapshot
///   Snapshot the shard after the queue proxy is initialized. This snapshot will be used to get
///   the shard into the same state on the remote.
/// - Recover shard snapshot on remote
///   Instruct the remote to download the snapshot from this node over HTTP, then recover it.
/// - Set shard state to `Partial`
///   After recovery, we set the shard state from `PartialSnapshot` to `Partial`. We propose an
///   operation to consensus for this. Our logic explicitly confirms that the remote reaches the
///   `Partial` state. That is critical for the remote to accept incoming operations, that also
///   confirms consensus has accepted accepted our proposal. If this fails it will be retried up to
///   three times.
/// - Transfer queued updates to remote, transform into forward proxy
///   Once the remote is in `Partial` state we can transfer all accumulated updates in the queue
///   proxy to the remote. This ensures all operations reach the recovered shard on the remote to
///   make it consistent again. When all updates are transferred, we transform the queue proxy into
///   a forward proxy to start forwarding new updates to the remote right away.
///   We transfer the queue and transform into a forward proxy right now so that we can catch any
///   errors as early as possible. The forward proxy shard we end up with will not error again once
///   we un-proxify.
/// - Wait for Partial state in our replica set
///   Wait for the remote shard to be set to `Partial` in our local replica set. That way we
///   confirm consensus has also propagated on this node.
/// - Synchronize all nodes
///   After confirming consensus propagation on this node, synchronize all nodes to reach the same
///   consensus state before finalizing the transfer. That way, we ensure we have a consistent
///   replica set state across all nodes. All nodes will have the `Partial` state, which makes the
///   shard participate on all nodes.
///
/// After this function, the following will happen:
///
/// - The local shard is un-proxified
/// - The shard transfer is finished
/// - The remote shard state is set to `Active` through consensus
///
/// # Diagram
///
/// Here's a rough sequence diagram for the shard snasphot transfer process with the consensus,
/// sender and receiver actors:
///
/// ┌───────────┐           ┌───────────┐             ┌───────────┐
/// │ Consensus │           │  Sender   │             │ Receiver  │
/// └───────────┘           └───────────┘             └───────────┘
///       |                       |                         |
///       |  start transfer       |                         |
/// ────►┌─┬──────────────────────|────────────────────────►|──┐
///      │ │                      |                         |  │ shard state:
///      │ │ start transfer       |  init transfer          |  │ Dead→PartialSnapshot
///      └─┴────────────────────►┬─┬──────────────────────►┌─┐◄┘
///       |                      │X│                       │ │
///       |                      │X│                       │ ├─┐
///       |                      │X│ ready                 │ │ │ init local shard
///       |                      │X├───────────────────────┴─┘◄┘
///       |                      │ │                        |
///       |                      │ ├─┐                      |
///       |                      │ │ │ qproxy + snapshot    |
///       |                      │ │◄┘                      |
///       |                      │ │                        |
///       |                      │ │ recover shard by URL   |
///       |                      │X├───────────────────────┬─┐
///       |                      │X│                       │ │
///       |                      │X│                       │ │
///       |                ┌─┐◄─·│X│·──────────────────────┤ │
///       |                │ │   │X│ download snapshot     │ │
///       |                └─┴──·│X│·─────────────────────►│ ├─┐
///       |                      │X│                       │ │ │ apply snapshot
///       |                      │X│ done recovery         │ │ │ delete snapshot
///       |                      │X│◄──────────────────────┴─┘◄┘
///       |  snapshot recovered  │ │                        |
///      ┌─┐◄────────────────────┤ │                        |
///      │ │                     │ │                        |
///      │ │                   ┌─┤X│                        |
///      │ │    wait consensus │ │X│                        |
///      │ │          or retry │ │X│                        |
///      │ │                   │ │X│                        |
///      │ │ continue transfer │ │X│                        |
///      │ ├──────────────────·│ │X│·─────────────────────►┌─┬─┐
///      │ │ continue transfer │ │X│                       │ │ │ shard state:
///      └─┴───────────────────┤►│X├─┐                     │ │ │ PartialSnapshot→Partial
///       |                    │ │X│ │ shard state:        └─┘◄┘
///       |                    │ │X│ │ PartialSnpst→Partial |
///       |                    └►│X│◄┘                      |
///       |                      │ │                        |
///       |                      │ │ transfer queue ops     |
///       |                    ┌►│X├──────────────────────►┌─┬─┐
///       |       send batches │ │X│                       │ │ │ apply operations
///       |                    └─┤X│◄──────────────────────┴─┘◄┘
///       |                      │ │                        |
///       |                      │ ├─┐                      |
///       |                      │ │ │ qproxy→fwd proxy     |
///       |                      │ │◄┘                      |
///       |                      │ │                        |
///       |                      │ │ sync all nodes         |
///       |                      │X├──────────────────────►┌─┬─┐
///       |                      │X│                       │ │ │ wait consensus
///       |                      │X│ node synced           │ │ │ commit+term
///       |                      │X│◄──────────────────────┴─┘◄┘
///       |                      │ │                        |
///       |                      │ ├─┐                      |
///       |  finish transfer     │ │ │ unproxify            |
///      ┌─┐◄────────────────────┴─┘◄┘                      |
///      │ │ transfer finished    |                         |
///      │ ├──────────────────────|───────────────────────►┌─┬─┐
///      │ │ transfer finished    |                        │ │ │ shard state:
///      └─┴────────────────────►┌─┬─┐                     │ │ │ Partial→Active
///       |                      │ │ │ shard state:        └─┘◄┘
///       |                      │ │ │ Partial→Active       |
///       |                      └─┘◄┘                      |
///       |                       |                         |
///
/// # Cancel safety
///
/// This function is cancel safe.
///
/// If cancelled - the remote shard may only be partially recovered/transferred and the local shard
/// may be left in an unexpected state. This must be resolved manually in case of cancellation.
#[allow(clippy::too_many_arguments)]
pub(super) async fn transfer_snapshot(
    transfer_config: ShardTransfer,
    shard_holder: SharedShardHolder,
    progress: Arc<Mutex<TransferTaskProgress>>,
    shard_id: ShardId,
    remote_shard: RemoteShard,
    channel_service: &ChannelService,
    consensus: &dyn ShardTransferConsensus,
    snapshots_path: &Path,
    collection_id: &CollectionId,
    temp_dir: &Path,
) -> CollectionResult<()> {
    let remote_peer_id = remote_shard.peer_id;

    log::debug!(
        "Starting shard {shard_id} transfer to peer {remote_peer_id} using snapshot transfer"
    );

    let shard_holder_read = shard_holder.read().await;
    let local_rest_address = channel_service.current_rest_address(transfer_config.from)?;

    let transferring_shard = shard_holder_read.get_shard(shard_id);
    let Some(replica_set) = transferring_shard else {
        return Err(CollectionError::service_error(format!(
            "Shard {shard_id} cannot be queue proxied because it does not exist"
        )));
    };

    // Queue proxy local shard
    progress.lock().set_stage(TransferStage::Proxifying);
    replica_set
        .queue_proxify_local(remote_shard.clone(), None, progress.clone())
        .await?;

    debug_assert!(
        replica_set.is_queue_proxy().await,
        "Local shard must be a queue proxy",
    );

    // The ability to read streaming snapshot format is introduced in 1.12 (#5179).
    let use_streaming_endpoint =
        channel_service.peer_is_at_version(remote_peer_id, &Version::new(1, 12, 0));

    let mut snapshot_temp_paths = Vec::new();
    let mut shard_download_url = local_rest_address;
    let mut snapshot_checksum = None;

    let encoded_collection_name = urlencoding::encode(collection_id);
    if use_streaming_endpoint {
        log::trace!("Using streaming endpoint for shard snapshot transfer");
        shard_download_url.set_path(&format!(
            "/collections/{encoded_collection_name}/shards/{shard_id}/snapshot",
        ));
    } else {
        // Create shard snapshot
        progress.lock().set_stage(TransferStage::CreatingSnapshot);
        log::trace!("Creating snapshot of shard {shard_id} for shard snapshot transfer");
        let snapshot_description = shard_holder_read
            .create_shard_snapshot(snapshots_path, collection_id, shard_id, temp_dir)
            .await?
            .await?;

        snapshot_checksum = snapshot_description.checksum;

        // TODO: If future is cancelled until `get_shard_snapshot_path` resolves, shard snapshot may not be cleaned up...
        let snapshot_temp_path = shard_holder_read
            .get_shard_snapshot_path(snapshots_path, shard_id, &snapshot_description.name)
            .await
            .map_err(|err| {
                CollectionError::service_error(format!(
                    "Failed to determine snapshot path, cannot continue with shard snapshot recovery: {err}",
                ))
            })?;
        let snapshot_temp_path = TempPath::try_from_path(snapshot_temp_path)?;
        let snapshot_checksum_temp_path =
            TempPath::try_from_path(get_checksum_path(&snapshot_temp_path))?;
        snapshot_temp_paths.push(snapshot_temp_path);
        snapshot_temp_paths.push(snapshot_checksum_temp_path);

        let encoded_snapshot_name = urlencoding::encode(&snapshot_description.name);

        shard_download_url.set_path(&format!(
            "/collections/{encoded_collection_name}/shards/{shard_id}/snapshots/{encoded_snapshot_name}"
        ));
    };

    // Recover shard snapshot on remote
    progress.lock().set_stage(TransferStage::Recovering);
    log::trace!("Transferring and recovering shard {shard_id} snapshot on peer {remote_peer_id}");

    // Since we are providing access to local instance, any of the API keys can be used
    let local_api_key = channel_service
        .api_key
        .as_deref()
        .or(channel_service.alt_api_key.as_deref());

    remote_shard
        .recover_shard_snapshot_from_url(
            collection_id,
            shard_id,
            &shard_download_url,
            SnapshotPriority::ShardTransfer,
            snapshot_checksum,
            // Provide API key here so the remote can access our snapshot
            local_api_key,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Failed to recover shard snapshot on remote: {err}"
            ))
        })?;

    for snapshot_temp_path in snapshot_temp_paths {
        if let Err(err) = snapshot_temp_path.close() {
            log::warn!(
                "Failed to delete shard transfer snapshot after recovery, \
                 snapshot file may be left behind: {err}"
            );
        }
    }

    // Set shard state to Partial
    progress.lock().set_stage(TransferStage::WaitingConsensus);
    log::trace!(
        "Shard {shard_id} snapshot recovered on {remote_peer_id} for snapshot transfer, switching into next stage through consensus",
    );
    consensus
        .recovered_switch_to_partial_confirm_remote(&transfer_config, collection_id, &remote_shard)
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Can't switch shard {shard_id} to Partial state after snapshot transfer: {err}"
            ))
        })?;

    // Transfer queued updates to remote, transform into forward proxy
    progress.lock().set_stage(TransferStage::FlushingQueue);
    log::trace!("Transfer all queue proxy updates and transform into forward proxy");
    replica_set.queue_proxy_into_forward_proxy().await?;

    // Wait for Partial state in our replica set
    // Consensus sync is done right after this function
    progress.lock().set_stage(TransferStage::WaitingConsensus);
    let partial_state = ReplicaState::Partial;
    log::trace!("Wait for local shard to reach {partial_state:?} state");
    replica_set
        .wait_for_state(
            transfer_config.to,
            partial_state,
            defaults::CONSENSUS_META_OP_WAIT,
        )
        .await
        .map_err(|err| {
            CollectionError::service_error(format!(
                "Shard being transferred did not reach {partial_state:?} state in time: {err}",
            ))
        })?;

    log::debug!(
        "Ending shard {shard_id} transfer to peer {remote_peer_id} using snapshot transfer"
    );

    Ok(())
}
