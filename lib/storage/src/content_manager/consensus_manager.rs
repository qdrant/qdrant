use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::ops::Deref;
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use chrono::Utc;
use collection::collection_state;
use collection::common::is_ready::IsReady;
use collection::operations::types::PeerMetadata;
use collection::shards::CollectionId;
use collection::shards::shard::PeerId;
use common::defaults;
use futures::future::join_all;
use parking_lot::{Mutex, RwLock};
use raft::eraftpb::{ConfChange, ConfChangeType, ConfChangeV2, Entry as RaftEntry, EntryType};
use raft::{GetEntriesContext, RaftState, RawNode, SoftState, Storage};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::time::error::Elapsed;
use tokio_util::task::AbortOnDropHandle;
use tonic::transport::Uri;

use super::CollectionContainer;
use super::alias_mapping::AliasMapping;
use super::consensus_ops::{ConsensusOperations, SnapshotStatus};
use super::errors::StorageError;
use crate::content_manager::consensus::applied_log::{AppliedEntryRing, AppliedLog};
use crate::content_manager::consensus::consensus_wal::ConsensusOpWal;
use crate::content_manager::consensus::entry_queue::EntryId;
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus::persistent::Persistent;
use crate::quota::QuotaConfig;
use crate::types::{
    ClusterInfo, ClusterStatus, ConsensusThreadStatus, MessageSendErrors, PeerAddressById,
    PeerInfo, PeerMetadataById, RaftInfo,
};

pub mod prelude {
    use crate::content_manager::toc::TableOfContent;

    pub type ConsensusState = super::ConsensusManager<TableOfContent>;
}

/// Allow us updating our peer metadata once every 60 seconds
const CONSENSUS_PEER_METADATA_UPDATE_INTERVAL: Duration = Duration::from_secs(60);

/// Log a warning if applying a single consensus entry takes longer than this.
///
/// Applying is synchronous and inline, so for that long the peer sends no heartbeats, reports no
/// ticks and applies no further entry — it also keeps serving requests against the replica set
/// state as it was before the entry.
const SLOW_APPLY_REPORT_THRESHOLD: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotData {
    pub collections_data: CollectionsSnapshot,
    #[serde(with = "crate::serialize_peer_addresses")]
    pub address_by_id: PeerAddressById,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata_by_id: PeerMetadataById,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub cluster_metadata: HashMap<String, serde_json::Value>,
    /// `None` when the snapshot was taken by a peer that predates global quotas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_config: Option<QuotaConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct CollectionsSnapshot {
    pub collections: HashMap<CollectionId, collection_state::State>,
    pub aliases: AliasMapping,
}

impl TryFrom<&[u8]> for SnapshotData {
    type Error = serde_cbor::Error;

    fn try_from(bytes: &[u8]) -> Result<SnapshotData, Self::Error> {
        serde_cbor::from_slice(bytes)
    }
}

pub struct ConsensusManager<C: CollectionContainer> {
    pub persistent: RwLock<Persistent>,
    /// Notifies if the current node knows who the leader and is not in the process of election
    /// Otherwise the proposals are not accepted
    pub is_leader_established: Arc<IsReady>,
    wal: Mutex<ConsensusOpWal>,
    /// Raft consensus state, which is not saved on disk.
    /// They will change on restart anyway (role + leader id)
    soft_state: RwLock<Option<SoftState>>,
    /// Storage-related container. Should apply and persist changes not related to consensus
    /// (user changes)
    toc: Arc<C>,
    /// Operation apply notifier.
    /// Fires a signal if some specific operation is applied to the state machine.
    /// Signal is changed on change proposal and triggered if the change was applied by consensus on this peer.
    /// Also sends the result of the operation.
    on_consensus_op_apply:
        Mutex<HashMap<ConsensusOperations, broadcast::Sender<Result<bool, StorageError>>>>,
    /// Propose operation to the consensus.
    /// Sends messages to the consensus thread, which is defined externally, outside of the state.
    /// (e.g. in the `src/consensus.rs`)
    propose_sender: OperationSender,
    /// Status of the consensus thread, changed by the consensus thread
    consensus_thread_status: RwLock<ConsensusThreadStatus>,
    /// Consensus thread errors, changed by the consensus thread
    message_send_failures: RwLock<HashMap<String, MessageSendErrors>>,
    /// Last time we attempted to update the peer metadata
    next_peer_metadata_update_attempt: Mutex<Instant>,
    /// Recently applied entries, for `/profiler/consensus_lag`. Diagnostics only.
    applied_log: AppliedEntryRing,
}

impl<C: CollectionContainer> ConsensusManager<C> {
    pub fn new(
        persistent_state: Persistent,
        toc: Arc<C>,
        propose_sender: OperationSender,
        storage_path: &Path,
    ) -> Result<Self, StorageError> {
        let mut wal = ConsensusOpWal::new(storage_path);

        // When our Raft index and last snapshot index match, the last thing we did is apply a Raft
        // snapshot. It is possible that we crashed before clearing the WAL, so we still do it now.
        // Specifically, if the last operation was applying a snapshot and our WAL does still have
        // older Raft entries, we clear the whole WAL. Consensus will take care of us catching up
        // with the rest.
        // See `apply_snapshot` function and <https://github.com/qdrant/qdrant/pull/7577>.
        let raft_index = persistent_state.state().hard_state.commit;
        let snapshot_index = persistent_state.latest_snapshot_meta.index;
        let last_operation_was_snapshot = raft_index == persistent_state.latest_snapshot_meta.index;
        if last_operation_was_snapshot
            && let Ok(Some(last)) = wal.last_entry()
            && last.index < snapshot_index
        {
            log::warn!(
                "Consensus WAL was not cleared after applying consensus snapshot, clearing it now"
            );
            wal.clear()?;
        }

        Ok(Self {
            persistent: RwLock::new(persistent_state),
            is_leader_established: Arc::new(IsReady::default()),
            wal: Mutex::new(wal),
            soft_state: RwLock::new(None),
            toc,
            on_consensus_op_apply: Default::default(),
            propose_sender,
            consensus_thread_status: RwLock::new(ConsensusThreadStatus::Working {
                last_update: Utc::now(),
            }),
            message_send_failures: Default::default(),
            next_peer_metadata_update_attempt: Mutex::new(Instant::now()),
            applied_log: Default::default(),
        })
    }

    /// Snapshot of the recently applied entries on this peer, oldest first.
    pub fn applied_log(&self) -> AppliedLog {
        // Read the apply queue before taking the ring, so the two locks never nest.
        let (pending_operations, last_applied_index) = {
            let persistent = self.persistent.read();
            (
                persistent.unapplied_entities_count(),
                persistent.last_applied_entry(),
            )
        };
        self.applied_log
            .snapshot(pending_operations, last_applied_index)
    }

    pub fn report_snapshot(
        &self,
        peer_id: u64,
        status: impl Into<SnapshotStatus>,
    ) -> Result<(), StorageError> {
        self.propose_sender
            .send(ConsensusOperations::report_snapshot(peer_id, status))
            .map_err(|_err| {
                StorageError::service_error(
                    "failed to send ReportSnapshot message to consensus thread",
                )
            })
    }

    pub fn record_message_send_failure<E: Error>(&self, peer_address: &Uri, error: E) {
        let mut message_send_failures = self.message_send_failures.write();
        let entry = message_send_failures
            .entry(peer_address.to_string())
            .or_default();
        // Log only first error
        if entry.count == 0 {
            log::warn!("Failed to send message to {peer_address} with error: {error}")
        }
        entry.count += 1;
        entry.latest_error = Some(error.to_string());
        entry.latest_error_timestamp = Some(Utc::now());
    }

    pub fn record_message_send_success(&self, peer_address: &Uri) {
        self.message_send_failures
            .write()
            .remove(&peer_address.to_string());
    }

    pub fn record_consensus_working(&self) {
        *self.consensus_thread_status.write() = ConsensusThreadStatus::Working {
            last_update: Utc::now(),
        }
    }

    pub fn on_consensus_stopped(&self) {
        *self.consensus_thread_status.write() = ConsensusThreadStatus::Stopped
    }

    pub fn on_consensus_thread_err<E: Display>(&self, err: E) {
        *self.consensus_thread_status.write() = ConsensusThreadStatus::StoppedWithErr {
            err: err.to_string(),
        }
    }

    pub fn set_raft_soft_state(&self, state: &SoftState) {
        *self.soft_state.write() = Some(SoftState { ..*state });
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.persistent.read().this_peer_id
    }

    pub fn peers(&self) -> Vec<PeerId> {
        self.persistent
            .read()
            .peer_address_by_id
            .read()
            .keys()
            .copied()
            .collect()
    }

    pub fn first_voter(&self) -> PeerId {
        let state = self.persistent.read();

        match state.first_voter() {
            Some(peer_id) if peer_id != PeerId::MAX => peer_id,
            _ => state.this_peer_id(),
        }
    }

    pub fn set_first_voter(&self, id: PeerId) -> Result<(), StorageError> {
        self.persistent.write().set_first_voter(id)
    }

    pub fn recover_first_voter(&self) -> Result<(), StorageError> {
        // `load_or_init` sets `first_voter` explicitly when reinitializing as first peer,
        // so guard below short circuits and WAL is never read in that case
        if self.persistent.read().first_voter().is_none() {
            log::debug!("Recovering first voter peer...");

            let wal = self.wal.lock();
            let peers = self.peers();

            if let Some(peer_id) = recover_first_voter(&wal, &peers)? {
                log::debug!("Recovered first voter peer {peer_id}");
                self.set_first_voter(peer_id)?;
            }
        }

        Ok(())
    }

    /// Report aggregated information about the cluster.
    /// Useful for API reporting.
    pub fn cluster_status(&self) -> ClusterStatus {
        let persistent = self.persistent.read();
        let hard_state = &persistent.state.hard_state;
        let peers = persistent
            .peer_address_by_id()
            .into_iter()
            .map(|(peer_id, uri)| {
                (
                    peer_id,
                    PeerInfo {
                        uri: uri.to_string(),
                    },
                )
            })
            .collect();
        let pending_operations = persistent.unapplied_entities_count();
        let soft_state = self.soft_state.read();
        let leader = soft_state.as_ref().map(|state| state.leader_id);
        let role = soft_state.as_ref().map(|state| state.raft_state.into());
        let peer_id = persistent.this_peer_id;
        let is_voter = persistent.state.conf_state.get_voters().contains(&peer_id);
        ClusterStatus::Enabled(ClusterInfo {
            peer_id,
            peers,
            raft_info: RaftInfo {
                term: hard_state.term,
                commit: hard_state.commit,
                pending_operations,
                leader,
                role,
                is_voter,
            },
            consensus_thread_status: self.consensus_thread_status.read().clone(),
            message_send_failures: self.message_send_failures.read().clone(),
        })
    }

    /// Handle peer removal operation.
    ///
    /// 1. Try to remove peer
    /// 2. Handle peer removal error
    /// 3. Report to the listeners
    ///
    /// Return if consensus should be stopped.
    pub fn on_peer_remove(&self, peer_id: PeerId) -> Result<bool, StorageError> {
        let mut stop_consensus: bool = false;

        let report = match self.remove_peer(peer_id) {
            Ok(()) => {
                if self.this_peer_id() == peer_id {
                    stop_consensus = true;
                }
                Ok(true)
            }
            #[expect(clippy::wildcard_enum_match_arm, reason = "error handling")]
            Err(err) => match err {
                err @ StorageError::ServiceError { .. } => {
                    return Err(err);
                }
                _ => Err(err),
            },
        };
        let operation = ConsensusOperations::RemovePeer(peer_id);
        let on_apply = self.on_consensus_op_apply.lock().remove(&operation);
        if let Some(on_apply) = on_apply
            && on_apply.send(report).is_err()
        {
            log::warn!(
                "Failed to notify on consensus operation completion: channel receiver is dropped",
            )
        }
        Ok(stop_consensus)
    }

    pub fn set_unapplied_entries(
        &self,
        first_index: EntryId,
        last_index: EntryId,
    ) -> Result<(), raft::Error> {
        self.persistent
            .write()
            .set_unapplied_entries(first_index, last_index)
            .map_err(raft_error_other)
    }

    /// Process the consensus operation, which are already committed.
    /// If return Error - consensus should be stopped with error.
    /// Return `true` if consensus should be stopped (peer removed)
    /// Return `false` if everything is ok.
    pub fn apply_entries<T: Storage>(&self, raw_node: &mut RawNode<T>) -> anyhow::Result<bool> {
        use raft::eraftpb::EntryType;

        self.persistent
            .write()
            .save_if_dirty()
            .context("Failed to save new state of applied entries queue")?;

        loop {
            let unapplied_index = self.persistent.read().current_unapplied_entry();
            let Some(entry_index) = unapplied_index else {
                break;
            };
            log::debug!("Applying committed entry with index {entry_index}");
            let entry = self
                .wal
                .lock()
                .entry(entry_index)
                .context(format!("Failed to get entry at index {entry_index}"))?;
            let apply_started = Instant::now();
            let stop_consensus: bool = if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
                false
            } else {
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let operation_result = self.apply_normal_entry(&entry);
                        match operation_result {
                            Ok(result) => {
                                log::debug!(
                                    "Successfully applied consensus operation entry. Index: {}. Result: {result}",
                                    entry.index,
                                );
                                false
                            }
                            Err(err @ StorageError::ServiceError { .. }) => {
                                // This is a service error - stop consensus. Peer can be restarted when the problem is fixed.
                                return Err(err)
                                    .context("Failed to apply collection meta operation entry");
                            }
                            Err(err) => {
                                log::warn!(
                                    "Failed to apply collection meta operation entry with user error: {err}",
                                );
                                // This is a user error so we can safely consider it applied but with error as it was incorrect.
                                false
                            }
                        }
                    }
                    EntryType::EntryConfChangeV2 => {
                        let stop_consensus = self
                            .apply_conf_change_entry(&entry, raw_node)
                            .context("Failed to apply configuration change entry")?;
                        log::debug!(
                            "Successfully applied configuration change entry. Index: {}. Stop consensus: {}",
                            entry.index,
                            stop_consensus
                        );
                        stop_consensus
                    }
                    ty @ EntryType::EntryConfChange => {
                        return Err(anyhow!("Unexpected entry type: {ty:?}"));
                    }
                }
            };
            let apply_duration = apply_started.elapsed();
            if apply_duration >= SLOW_APPLY_REPORT_THRESHOLD {
                log::warn!(
                    "Slow consensus entry: applying {entry_index} took {apply_duration:.2?}, \
                     stalling the consensus thread for that long",
                );
            }

            if stop_consensus {
                return Ok(stop_consensus);
            }
            self.persistent
                .write()
                .entry_applied()
                .context("Failed to save new state of applied entries queue")?;
            self.applied_log.record(&entry, apply_duration);
        }
        Ok(false) // do not stop consensus
    }

    /// Process the consensus operation, which are already committed.
    /// In this particular function - operations related to the cluster topology change:
    ///
    /// - AddPeer (different states)
    /// - RemovePeer
    pub fn apply_conf_change_entry<T: Storage>(
        &self,
        entry: &RaftEntry,
        raw_node: &mut RawNode<T>,
    ) -> Result<bool, StorageError> {
        let change: ConfChangeV2 = prost_for_raft::Message::decode(entry.get_data())?;

        let conf_state = raw_node.apply_conf_change(&change)?;
        log::debug!("Applied conf state {conf_state:?}");
        self.persistent
            .write()
            .apply_state_update(|state| state.conf_state = conf_state)?;

        let mut stop_consensus: bool = false;
        for single_change in &change.changes {
            match single_change.change_type() {
                ConfChangeType::AddNode => {
                    let context = entry.get_context();

                    if !context.is_empty() {
                        let peer_uri = str::from_utf8(context)
                            .map_err(|err| {
                                StorageError::service_error(format!(
                                    "failed to parse peer URI: {err}"
                                ))
                            })?
                            .parse()
                            .map_err(|err| {
                                StorageError::service_error(format!(
                                    "failed to parse peer URI: {err}"
                                ))
                            })?;

                        self.add_peer(single_change.node_id, peer_uri)?;
                    } else {
                        debug_assert!(
                            self.peer_address_by_id()
                                .contains_key(&single_change.node_id),
                            "Peer should be already known"
                        )
                    }
                }
                ConfChangeType::RemoveNode => {
                    log::debug!("Removing node {}", single_change.node_id);
                    stop_consensus |= self.on_peer_remove(single_change.node_id)?;
                }
                ConfChangeType::AddLearnerNode => {
                    log::debug!("Adding learner node {}", single_change.node_id);
                    if let Ok(peer_uri) = String::from_utf8_lossy(entry.get_context())
                        .deref()
                        .try_into()
                    {
                        let peer_uri: Uri = peer_uri;
                        // Add peer to state
                        self.add_peer(single_change.node_id, peer_uri.clone())?;

                        // Notify the submitter, that operation was performed
                        {
                            let operation = ConsensusOperations::AddPeer {
                                peer_id: single_change.node_id,
                                uri: peer_uri.to_string(),
                            };
                            let on_apply = self.on_consensus_op_apply.lock().remove(&operation);
                            if let Some(on_apply) = on_apply
                                && on_apply.send(Ok(true)).is_err()
                            {
                                log::warn!(
                                    "Failed to notify on consensus operation completion: channel receiver is dropped",
                                )
                            }
                        }
                    } else if entry.get_context().is_empty() {
                        // Allow empty context for compatibility
                        log::warn!(
                            "Outdated peer addition entry found with index: {}",
                            entry.get_index()
                        )
                    } else {
                        // Should not be reachable as it is checked in API
                        return Err(StorageError::service_error("Failed to parse peer uri"));
                    }
                }
            }
        }
        Ok(stop_consensus)
    }

    /// Process the consensus operation, which are already committed.
    /// In this particular function - operations related to user data:
    ///
    /// - CreateCollection
    /// - DropCollection
    /// - Update collection params
    /// - Update collection aliases
    /// - Shards operations (transfer, remove, sync)
    /// - e.t.c
    ///
    pub fn apply_normal_entry(&self, entry: &RaftEntry) -> Result<bool, StorageError> {
        let operation: ConsensusOperations = entry.try_into()?;
        let on_apply = self.on_consensus_op_apply.lock().remove(&operation);
        let result = match operation {
            ConsensusOperations::CollectionMeta(operation) => {
                self.toc.perform_collection_meta_op(*operation)
            }

            ConsensusOperations::AddPeer { .. } | ConsensusOperations::RemovePeer(_) => {
                // RemovePeer or AddPeer should be converted into native ConfChangeV2 message before sending to the Raft.
                // So we do not expect to receive these operations as a normal entry.
                // This is a debug assert so production migrations should be ok.
                // TODO: parse into CollectionMetaOperation as we will not handle other cases here, but this removes compatibility with previous entry storage
                debug_assert!(
                    false,
                    "Do not expect RemovePeer or AddPeer to be directly proposed"
                );
                Ok(false)
            }

            ConsensusOperations::UpdatePeerMetadata { peer_id, metadata } => {
                self.persistent
                    .write()
                    .update_peer_metadata(peer_id, metadata)?;
                Ok(true)
            }

            ConsensusOperations::UpdateClusterMetadata { key, value } => {
                self.persistent
                    .write()
                    .update_cluster_metadata_key(key, value);
                Ok(true)
            }

            ConsensusOperations::SetQuotaConfig(config) => {
                self.toc.set_quota_config(config).map(|()| true)
            }

            ConsensusOperations::RequestSnapshot | ConsensusOperations::ReportSnapshot { .. } => {
                unreachable!()
            }
        };

        if let Some(on_apply) = on_apply
            && on_apply.send(result.clone()).is_err()
        {
            log::warn!(
                "Failed to notify on consensus operation completion: channel receiver is dropped",
            )
        }
        result
    }

    // Outer `Result` is "fatal" error, inner `Result` is "transient"/"local" error.
    pub fn apply_snapshot(
        &self,
        snapshot: &raft::eraftpb::Snapshot,
    ) -> Result<Result<(), StorageError>, StorageError> {
        let meta = snapshot.get_metadata();

        let SnapshotData {
            collections_data,
            address_by_id,
            metadata_by_id,
            cluster_metadata,
            quota_config,
        } = snapshot.get_data().try_into()?;

        self.toc.apply_collections_snapshot(collections_data)?;
        if let Some(quota_config) = quota_config {
            self.toc.set_quota_config(quota_config)?;
        }
        self.persistent.write().update_from_snapshot(
            meta,
            address_by_id,
            metadata_by_id,
            cluster_metadata,
        )?;

        // Clear now obsolete WAL entries after persisting new Raft state
        // This way we prevent a crash due to an empty WAL if we crash right after clearing it,
        // without bumping the Raft state. If we now crash after persisting the new state but
        // before clearing the WAL, we will clear the WAL on next startup by truncating all entries
        // above our commit.
        self.wal.lock().clear()?;

        // Notify any awaiting consensus operations that are now observably satisfied by the
        // snapshot state. Without this, an operation that was proposed locally and then committed
        // remotely can be delivered via snapshot (instead of as a log entry) — in which case it
        // never flows through `apply_conf_change_entry` / `apply_normal_entry`, so the waiter in
        // `propose_consensus_op_with_await` would block until the timeout.
        self.notify_pending_ops_from_snapshot();

        Ok(Ok(()))
    }

    /// Inspect pending awaiters and resolve those whose effect is visible in the current
    /// persistent state. Only operations whose result can be directly read off the snapshot
    /// state are eligible — others remain pending and may still time out.
    fn notify_pending_ops_from_snapshot(&self) {
        let persistent = self.persistent.read();
        let address_by_id = persistent.peer_address_by_id.read();
        let metadata_by_id = persistent.peer_metadata_by_id.read();

        self.on_consensus_op_apply
            .lock()
            .retain(|operation, sender| {
                let satisfied = match operation {
                    ConsensusOperations::AddPeer { peer_id, uri } => {
                        address_by_id
                            .get(peer_id)
                            .map(|known| known.to_string())
                            .as_deref()
                            == Some(uri.as_str())
                    }
                    ConsensusOperations::RemovePeer(peer_id) => {
                        !address_by_id.contains_key(peer_id)
                    }
                    ConsensusOperations::UpdatePeerMetadata { peer_id, metadata } => {
                        metadata_by_id.get(peer_id) == Some(metadata)
                    }
                    ConsensusOperations::UpdateClusterMetadata { key, value } => {
                        persistent.cluster_metadata.get(key) == Some(value)
                    }
                    ConsensusOperations::SetQuotaConfig(config) => {
                        self.toc.quota_config() == *config
                    }
                    // Snapshot state can't be inspected to confirm these are satisfied — leave
                    // their awaiters pending so they fall back to the regular timeout path.
                    ConsensusOperations::CollectionMeta(_)
                    | ConsensusOperations::RequestSnapshot
                    | ConsensusOperations::ReportSnapshot { .. } => false,
                };

                if satisfied {
                    let _ = sender.send(Ok(true));
                    false
                } else {
                    true
                }
            });
    }

    pub fn set_hard_state(&self, hard_state: raft::eraftpb::HardState) -> Result<(), StorageError> {
        self.persistent
            .write()
            .apply_state_update(move |state| state.hard_state = hard_state)
    }

    pub fn set_conf_state(&self, conf_state: raft::eraftpb::ConfState) -> Result<(), StorageError> {
        self.persistent
            .write()
            .apply_state_update(move |state| state.conf_state = conf_state)
    }

    /// Check if the consensus have empty operations log
    pub fn is_new_deployment(&self) -> bool {
        self.hard_state().term == 0
    }

    pub fn hard_state(&self) -> raft::eraftpb::HardState {
        self.persistent.read().state().hard_state.clone()
    }

    pub fn conf_state(&self) -> raft::eraftpb::ConfState {
        self.persistent.read().state().conf_state.clone()
    }

    pub fn set_commit_index(&self, index: u64) -> Result<(), StorageError> {
        self.persistent
            .write()
            .apply_state_update(|state| state.hard_state.commit = index)
    }

    pub fn peer_has_shards(&self, peer_id: PeerId) -> bool {
        self.toc
            .collections_snapshot()
            .collections
            .values()
            .flat_map(|state| state.shards.values())
            .flat_map(|shard_info| shard_info.replicas.keys())
            .any(|&id| id == peer_id)
    }

    pub fn add_peer(&self, peer_id: PeerId, uri: Uri) -> Result<(), StorageError> {
        self.persistent.write().insert_peer(peer_id, uri)
    }

    pub fn remove_peer(&self, peer_id: PeerId) -> Result<(), StorageError> {
        // We sincerely apologize for this piece of code.
        // The `id_to_address` is shared between `channel_pool` and `persistent`,
        // plus we need to make additional removing in the `channel_pool`.
        // So we handle `remove_peer` inside the `toc` and persist changes in the `persistent` after that.
        self.toc.remove_peer(peer_id)?;

        let persistent = self.persistent.read();
        persistent.peer_metadata_by_id.write().remove(&peer_id);
        persistent.save()
    }

    async fn await_receiver(
        &self,
        mut receiver: Receiver<Result<bool, StorageError>>,
        wait_timeout: Duration,
        operation: &ConsensusOperations,
    ) -> Result<bool, StorageError> {
        let Ok(receiver_res) = tokio::time::timeout(wait_timeout, receiver.recv()).await else {
            forget_operation_awaiter(&mut self.on_consensus_op_apply.lock(), operation, receiver);

            return Err(StorageError::service_error(format!(
                "Waiting for consensus operation commit failed. Timeout set at: {} seconds",
                wait_timeout.as_secs_f64(),
            )));
        };

        // 2 possible errors to forward: channel sender dropped OR operation failed
        receiver_res.map_err(|err| {
            StorageError::service_error(format!("Error occurred while waiting for consensus operation. Channel sender dropped ({err})"))
        })?
    }

    pub fn await_for_multiple_operations(
        &self,
        operations: Vec<ConsensusOperations>,
        wait_timeout: Option<Duration>,
    ) -> impl Future<Output = Result<Result<(), StorageError>, Elapsed>> {
        // Register the awaiters eagerly, before the caller proposes the operation that triggers
        // them: the awaited operations are emitted as a side effect of applying that one, and can
        // land before this future is first polled. The guard deregisters them again whenever we
        // stop waiting, including when the caller drops this future without ever polling it.
        let mut awaiters = OperationAwaiters::register(self, operations);

        async move {
            let await_for_all = join_all(awaiters.receivers_mut().map(|r| r.recv()));
            let results = tokio::time::timeout(
                wait_timeout.unwrap_or(defaults::CONSENSUS_META_OP_WAIT),
                await_for_all,
            )
            .await?;

            for result in results {
                match result {
                    Ok(Ok(_)) => (),
                    Ok(Err(err)) => return Ok(Err(err)),
                    Err(err) => return Ok(Err(err.into())),
                }
            }

            Ok(Ok(()))
        }
    }

    /// Wait and block until consensus reaches a `term` and actually applies the `commit`.
    ///
    /// Returns `false` if we have diverged commit/term for example, or if `timeout` elapsed first.
    #[must_use]
    pub async fn wait_for_consensus_commit(
        &self,
        commit: u64,
        term: u64,
        consensus_tick: Duration,
        timeout: Duration,
    ) -> bool {
        let start = Instant::now();

        // TODO: naive approach with spinlock for waiting on commit/term, find better way
        while start.elapsed() < timeout {
            let (current_commit, current_term) = self.persistent.read().applied_commit_term();

            // Okay if on the same term and have at least the specified commit
            let is_ok = current_term == term && current_commit >= commit;
            if is_ok {
                return true;
            }

            // Fail if on a newer term
            let is_fail = current_term > term;
            if is_fail {
                return false;
            }

            tokio::time::sleep(consensus_tick).await
        }

        // Fail on timeout
        false
    }

    /// Send operation to the consensus thread and listen for the result.
    ///
    /// # Arguments
    ///
    /// * `operation` - operation to propose
    /// * `wait_timeout` - How long do we need to wait for the confirmation
    pub async fn propose_consensus_op_with_await(
        &self,
        operation: ConsensusOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        let wait_timeout = wait_timeout.unwrap_or(defaults::CONSENSUS_META_OP_WAIT);

        let is_leader_established = self.is_leader_established.clone();

        let await_ready_for_timeout_future =
            AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
                is_leader_established.await_ready_for_timeout(wait_timeout)
            }));

        let is_leader_established = await_ready_for_timeout_future
            .await
            .map_err(|err| StorageError::service_error(err.to_string()))?;

        if !is_leader_established {
            return Err(StorageError::service_error(format!(
                "Failed to propose operation: leader is not established within {wait_timeout:?}"
            )));
        }

        // one-shot broadcast channel
        let (sender, mut receiver) = broadcast::channel(1);
        {
            // acquire lock to insert new operation to apply
            let mut on_apply_lock = self.on_consensus_op_apply.lock();
            // check that the exact same operation is not already in-flight
            match on_apply_lock.get(&operation) {
                Some(existing_sender) => {
                    // subscribe to existing sender for faster feedback
                    receiver = existing_sender.subscribe()
                }
                None => {
                    // propose operation to consensus thread
                    self.propose_sender.send(operation.clone())?;
                    // insert new sender
                    on_apply_lock.insert(operation.clone(), sender);
                }
            };
        }

        let res = self
            .await_receiver(receiver, wait_timeout, &operation)
            .await?;
        Ok(res)
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.persistent.read().peer_address_by_id()
    }

    pub fn peer_address(&self, peer_id: PeerId) -> Option<Uri> {
        self.persistent
            .read()
            .peer_address_by_id
            .read()
            .get(&peer_id)
            .cloned()
    }

    pub fn peer_count(&self) -> usize {
        self.persistent.read().peer_address_by_id.read().len()
    }

    pub fn append_entries(&self, entries: Vec<RaftEntry>) -> Result<(), StorageError> {
        self.wal.lock().append_entries(entries)
    }

    pub fn last_applied_entry(&self) -> Option<u64> {
        self.persistent.read().last_applied_entry()
    }

    pub fn sync_local_state(&self) -> Result<(), StorageError> {
        self.try_update_peer_metadata();
        self.toc.sync_local_state()
    }

    pub fn clear_wal(&self) -> Result<(), StorageError> {
        self.wal.lock().clear()
    }

    /// Discard committed-but-unapplied Raft log entries inherited from a previous
    /// cluster during first-peer consensus re-initialization (`--reinit`).
    ///
    /// `--reinit` resets this peer's `conf_state` to a single voter (itself) but
    /// leaves the Raft log untouched. If this peer had been removed from consensus
    /// before reinit, its log still holds a committed `RemoveNode(self)` conf-change
    /// (and possibly other topology changes from the old cluster). Replaying those on
    /// top of the reset single-voter config makes Raft abort with "removed all voters".
    ///
    /// Also drops pending WAL log entries that could re-trigger joint-consensus or auto leave
    /// transitions if the node was killed mid transition.
    ///
    /// Drop that tail: physically truncate the WAL to the last applied index, pin
    /// `commit` to it and clear the apply-progress queue, so a fresh single-node
    /// leader has nothing stale left to re-commit and re-apply.
    pub fn clear_unapplied_entries_on_reinit(&self) -> Result<(), StorageError> {
        let last_applied = self.persistent.read().last_applied_entry().unwrap_or(0);

        // Physically drop entries beyond the applied index
        self.wal.lock().truncate_after(last_applied)?;

        // Align persisted commit index and apply-progress queue with the truncated log
        let mut persistent = self.persistent.write();
        persistent.apply_state_update(|state| state.hard_state.commit = last_applied)?;
        // Empty queue that still reports `last_applied` as the last applied entry
        persistent.set_unapplied_entries(last_applied + 1, last_applied)?;

        Ok(())
    }

    pub fn compact_wal(&self, min_entries_to_compact: u64) -> Result<bool, StorageError> {
        if min_entries_to_compact == 0 {
            return Ok(false);
        }

        let Some(first_entry) = self.wal.lock().first_entry()? else {
            return Ok(false);
        };

        let Some(last_applied_index) = self.persistent.read().last_applied_entry() else {
            return Ok(false);
        };

        debug_assert!(
            first_entry.index <= last_applied_index + 1,
            "Raft WAL is missing {} unapplied entries (last applied index: {}, first WAL entry index: {})",
            first_entry.index - last_applied_index - 1,
            last_applied_index,
            first_entry.index,
        );

        if last_applied_index.saturating_sub(first_entry.index) < min_entries_to_compact {
            return Ok(false);
        }

        self.wal.lock().compact(last_applied_index)?;
        Ok(true)
    }

    /// Try to update our peer metadata if it's outdated
    ///
    /// It rate limits updating to `CONSENSUS_PEER_METADATA_UPDATE_INTERVAL`.
    fn try_update_peer_metadata(&self) {
        // Throttle updates to prevent spamming consensus
        if Instant::now() < *self.next_peer_metadata_update_attempt.lock() {
            return;
        }

        if !self.persistent.read().is_our_metadata_outdated() {
            return;
        }

        log::debug!("Proposing consensus peer metadata update for this peer");
        let result = self
            .propose_sender
            .send(ConsensusOperations::UpdatePeerMetadata {
                peer_id: self.this_peer_id(),
                metadata: PeerMetadata::current(),
            });
        if let Err(err) = result {
            log::error!("Failed to propose consensus peer metadata update for this peer: {err}");
        }
        *self.next_peer_metadata_update_attempt.lock() =
            Instant::now() + CONSENSUS_PEER_METADATA_UPDATE_INTERVAL;
    }
}

/// The awaiter map, keyed by the operation each caller is waiting for.
type OnConsensusOpApply =
    HashMap<ConsensusOperations, broadcast::Sender<Result<bool, StorageError>>>;

/// Deregister a caller that gave up waiting for `operation` to be applied.
///
/// The map owns the only `Sender` for an operation, and callers proposing an identical operation
/// deduplicate onto it instead of proposing again. Removing the entry closes the channel for
/// every other waiter, so one caller's timeout would fail their still in-flight operation with
/// `Channel sender dropped`. Only drop the entry once no receiver is left.
///
/// Takes the map already locked and drops `receiver` while it is held, so a caller registering in
/// between never finds an entry nobody is waiting on and subscribes to it anyway.
fn forget_operation_awaiter(
    on_apply_lock: &mut OnConsensusOpApply,
    operation: &ConsensusOperations,
    receiver: Receiver<Result<bool, StorageError>>,
) {
    drop(receiver);
    let no_waiters_left = on_apply_lock
        .get(operation)
        .is_some_and(|sender| sender.receiver_count() == 0);
    if no_waiters_left {
        on_apply_lock.remove(operation);
    }
}

/// Awaiters registered for a batch of consensus operations, deregistered again on drop.
///
/// The map owns the only `Sender` per operation, so an entry whose receivers are all gone is
/// dead weight: later callers deduplicate onto it and then never hear back. Tying deregistration
/// to the guard covers every way of giving up, including the caller dropping the future returned
/// by [`ConsensusManager::await_for_multiple_operations`] without ever polling it.
struct OperationAwaiters<'a, C: CollectionContainer> {
    consensus: &'a ConsensusManager<C>,
    /// One receiver per operation, keyed by the operation so we can deregister it again.
    awaiters: Vec<(ConsensusOperations, Receiver<Result<bool, StorageError>>)>,
}

impl<'a, C: CollectionContainer> OperationAwaiters<'a, C> {
    fn register(consensus: &'a ConsensusManager<C>, operations: Vec<ConsensusOperations>) -> Self {
        // Collected into the guard as we go, so giving up part way still deregisters the rest
        let mut this = Self {
            consensus,
            awaiters: Vec::with_capacity(operations.len()),
        };

        for operation in operations {
            let mut on_apply_lock = consensus.on_consensus_op_apply.lock();
            // check that the exact same operation is not already in-flight
            let receiver = match on_apply_lock.get(&operation) {
                Some(existing_sender) => {
                    debug_assert!(
                        existing_sender.receiver_count() > 0,
                        "Consensus operation must have at least one receiver, \
                         does forget_operation_awaiter() work correctly?",
                    );

                    // subscribe to existing sender for faster feedback
                    existing_sender.subscribe()
                }
                None => {
                    // one-shot broadcast channel
                    let (sender, receiver) = broadcast::channel(1);
                    on_apply_lock.insert(operation.clone(), sender);
                    receiver
                }
            };
            drop(on_apply_lock);

            // Keep the key around so we can deregister again
            this.awaiters.push((operation, receiver));
        }

        this
    }

    fn receivers_mut(&mut self) -> impl Iterator<Item = &mut Receiver<Result<bool, StorageError>>> {
        self.awaiters.iter_mut().map(|(_, receiver)| receiver)
    }
}

impl<C: CollectionContainer> Drop for OperationAwaiters<'_, C> {
    fn drop(&mut self) {
        if self.awaiters.is_empty() {
            return;
        }
        // One lock for the whole batch: creating a collection registers an awaiter per replica,
        // and this runs on the mutex the consensus thread needs for every entry it applies
        let mut on_apply_lock = self.consensus.on_consensus_op_apply.lock();
        for (operation, receiver) in self.awaiters.drain(..) {
            forget_operation_awaiter(&mut on_apply_lock, &operation, receiver);
        }
    }
}

fn recover_first_voter(
    wal: &ConsensusOpWal,
    peers: &[PeerId],
) -> Result<Option<PeerId>, StorageError> {
    let Some(first_entry) = wal.first_entry()? else {
        log::debug!("Skipped recovering first voter peer: WAL is empty");
        return Ok(None);
    };

    let Some(last_entry) = wal.last_entry()? else {
        log::error!(
            "Failed to recover first voter peer: \
             WAL contains first entry, but no last entry"
        );

        return Ok(None);
    };

    if first_entry.index != 1 {
        log::warn!("Failed to recover first voter peer: WAL is truncated");
        return Ok(Some(PeerId::MAX));
    }

    // Try to recover first voter peer from WAL (if it was not removed from cluster yet!):
    // - collect a list of current peers
    // - scroll WAL and *remove* a peer from the list when `AddPeer`/`AddLearnerPeer` operation encountered
    // - if there's exactly one peer left in the list at the end, this peer should be the first voter

    let mut peers: HashSet<_> = peers.iter().copied().collect();

    for index in first_entry.index..last_entry.index + 1 {
        let entry = wal.entry(index)?;

        match entry.get_entry_type() {
            EntryType::EntryConfChangeV2 => {
                let change: ConfChangeV2 = prost_for_raft::Message::decode(entry.get_data())?;

                for change in change.changes {
                    match change.get_change_type() {
                        ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                            peers.remove(&change.get_node_id());
                        }

                        ConfChangeType::RemoveNode => (),
                    }
                }
            }

            EntryType::EntryConfChange => {
                log::warn!(
                    "Encountered deprecated ConfChange message while recovering first voter peer"
                );

                let change: ConfChange = prost_for_raft::Message::decode(entry.get_data())?;

                match change.get_change_type() {
                    ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                        peers.remove(&change.get_node_id());
                    }

                    ConfChangeType::RemoveNode => (),
                }
            }

            EntryType::EntryNormal => (),
        }
    }

    if peers.len() > 1 {
        log::warn!(
            "Failed to recover first voter peer: \
             found multiple peers without ConfChange entry in WAL: \
             {peers:?}"
        );

        return Ok(Some(PeerId::MAX));
    }

    Ok(peers.into_iter().next())
}

/// Implementation of the methods for Raft library to get information from
/// our implementation of the storage.
/// Well tested magic
impl<C: CollectionContainer> Storage for ConsensusManager<C> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.persistent.read().state.clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<RaftEntry>> {
        let max_size: Option<_> = max_size.into();
        let first_index = self.first_index()?;
        if low < first_index {
            log::debug!(
                "Requested entries from {low} to {high} are already compacted (first index: {first_index})"
            );
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        log::debug!("Requesting entries from {low} to {high}");

        if high > self.last_index()? + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                self.last_index()? + 1,
                high
            );
        }
        self.wal.lock().entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let wal_guard = self.wal.lock();
        let persistent = self.persistent.read();
        let snapshot_meta = persistent.latest_snapshot_meta();
        if idx == snapshot_meta.index {
            return Ok(snapshot_meta.term);
        }
        Ok(wal_guard.entry(idx)?.term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let index = match self.wal.lock().first_entry().map_err(raft_error_other)? {
            Some(entry) => entry.index,
            None => self.persistent.read().latest_snapshot_meta().index + 1,
        };
        Ok(index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let index = match self.wal.lock().last_entry().map_err(raft_error_other)? {
            Some(entry) => entry.index,
            None => self.persistent.read().latest_snapshot_meta().index,
        };
        Ok(index)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<raft::eraftpb::Snapshot> {
        let collections_data = self.toc.collections_snapshot();

        // Lock first WAL and then persistent to avoid deadlock
        let wal_guard = self.wal.lock();
        // TODO: Should we lock `persistent` *before* calling `TableOfContent::collections_snapshot`!?
        let persistent = self.persistent.read();

        if persistent.state.hard_state.commit < request_index {
            // TODO: `raft::storage::MemStorage::snapshot` does `snapshot.mut_metadata().index = request_index` in this case... 🤔
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }

        let data = SnapshotData {
            collections_data,
            address_by_id: persistent.peer_address_by_id(),
            metadata_by_id: persistent.peer_metadata_by_id(),
            cluster_metadata: persistent.cluster_metadata.clone(),
            quota_config: Some(self.toc.quota_config()),
        };

        let raft_state = persistent.state();

        // Index of snapshot is the current *commit* index.
        let index = raft_state.hard_state.commit;

        // Term of snapshot is the term of the entry at current commit index. Not the current term!
        //
        // Last committed entry should either be available in the WAL, or, if current node applied
        // Raft snapshot (and so completely compacted the WAL) and no new entries were committed yet,
        // it should be the term of `latest_snapshot_meta`.
        let term = if index == persistent.latest_snapshot_meta.index {
            persistent.latest_snapshot_meta.term
        } else {
            wal_guard.entry(index)?.term
        };

        let meta = raft::eraftpb::SnapshotMetadata {
            conf_state: Some(raft_state.conf_state.clone()),
            index,
            term,
        };

        let snapshot = raft::eraftpb::Snapshot {
            data: serde_cbor::to_vec(&data).map_err(raft_error_other)?,
            metadata: Some(meta),
        };

        Ok(snapshot)
    }
}

#[derive(Clone)]
pub struct ConsensusStateRef(pub Arc<prelude::ConsensusState>);

impl Deref for ConsensusStateRef {
    type Target = prelude::ConsensusState;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl From<prelude::ConsensusState> for ConsensusStateRef {
    fn from(state: prelude::ConsensusState) -> Self {
        Self(Arc::new(state))
    }
}

impl Storage for ConsensusStateRef {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.0.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<RaftEntry>> {
        self.0.entries(low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.0.term(idx)
    }

    fn first_index(&self) -> raft::Result<EntryId> {
        self.0.first_index()
    }

    fn last_index(&self) -> raft::Result<EntryId> {
        self.0.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<raft::eraftpb::Snapshot> {
        self.0.snapshot(request_index, to)
    }
}

pub fn raft_error_other(e: impl std::error::Error) -> raft::Error {
    #[derive(thiserror::Error, Debug)]
    #[error("{0}")]
    struct StrError(String);

    raft::Error::Store(raft::StorageError::Other(Box::new(StrError(e.to_string()))))
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use collection::shards::shard::PeerId;
    use proptest::prelude::*;
    use raft::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeType, ConfChangeV2, Entry, EntryType,
    };
    use raft::storage::{MemStorage, Storage};
    use tempfile::Builder;
    use tokio::sync::broadcast;

    use super::{ConsensusManager, ConsensusOperations};
    use crate::content_manager::CollectionContainer;
    use crate::content_manager::consensus::consensus_wal::ConsensusOpWal;
    use crate::content_manager::consensus::entry_queue::EntryApplyProgressQueue;
    use crate::content_manager::consensus::operation_sender::OperationSender;
    use crate::content_manager::consensus::persistent::Persistent;
    use crate::quota::QuotaConfig;

    #[test]
    fn update_is_applied() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let mut state = Persistent::load_or_init(dir.path(), false, false, None).unwrap();
        assert_eq!(state.state().hard_state.commit, 0);
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);
    }

    #[test]
    fn save_failure() {
        let mut state = Persistent {
            path: "./unexistent_dir/file".into(),
            ..Default::default()
        };
        assert!(
            state
                .apply_state_update(|state| { state.hard_state.commit = 1 })
                .is_err(),
        );
    }

    #[test]
    fn state_is_loaded() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let mut state = Persistent::load_or_init(dir.path(), false, false, None).unwrap();
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);

        let state_loaded = Persistent::load_or_init(dir.path(), false, false, None).unwrap();
        assert_eq!(state_loaded.state().hard_state.commit, 1);
    }

    #[test]
    fn default_peer_id_is_persisted() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let peer_id = Some(101);
        let state = Persistent::load_or_init(dir.path(), false, false, peer_id).unwrap();
        assert_eq!(state.this_peer_id, 101);

        let state_loaded = Persistent::load_or_init(dir.path(), false, false, None).unwrap();
        assert_eq!(state_loaded.this_peer_id, 101);
    }

    #[test]
    fn unapplied_entries() {
        let mut entries = EntryApplyProgressQueue::new(0, 2);
        assert_eq!(entries.current(), Some(0));
        assert_eq!(entries.len(), 3);
        entries.applied();
        assert_eq!(entries.current(), Some(1));
        assert_eq!(entries.len(), 2);
        entries.applied();
        assert_eq!(entries.current(), Some(2));
        assert_eq!(entries.len(), 1);
        entries.applied();
        assert_eq!(entries.current(), None);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn correct_entry_with_offset() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let mut wal = ConsensusOpWal::new(dir.path());
        wal.append_entries(vec![Entry {
            index: 4,
            ..Default::default()
        }])
        .unwrap();
        wal.append_entries(vec![Entry {
            index: 5,
            ..Default::default()
        }])
        .unwrap();
        wal.append_entries(vec![Entry {
            index: 6,
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(wal.entry(5).unwrap().index, 5)
    }

    #[test]
    fn at_least_1_entry() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let mut wal = ConsensusOpWal::new(dir.path());
        wal.append_entries(vec![
            Entry {
                index: 4,
                ..Default::default()
            },
            Entry {
                index: 5,
                ..Default::default()
            },
        ])
        .unwrap();
        // Even when `max_size` is `0` this fn should return at least 1 entry
        assert_eq!(wal.entries(4, 5, Some(0)).unwrap().len(), 1)
    }

    struct NoCollections;

    impl CollectionContainer for NoCollections {
        fn perform_collection_meta_op(
            &self,
            _operation: crate::content_manager::collection_meta_ops::CollectionMetaOperations,
        ) -> Result<bool, crate::content_manager::errors::StorageError> {
            Ok(true)
        }

        fn collections_snapshot(&self) -> super::CollectionsSnapshot {
            super::CollectionsSnapshot::default()
        }

        fn apply_collections_snapshot(
            &self,
            _data: super::CollectionsSnapshot,
        ) -> Result<(), crate::content_manager::errors::StorageError> {
            Ok(())
        }

        fn remove_peer(
            &self,
            _peer_id: PeerId,
        ) -> Result<(), crate::content_manager::errors::StorageError> {
            Ok(())
        }

        fn sync_local_state(&self) -> Result<(), crate::content_manager::errors::StorageError> {
            Ok(())
        }

        fn quota_config(&self) -> QuotaConfig {
            QuotaConfig::default()
        }

        fn set_quota_config(
            &self,
            _config: QuotaConfig,
        ) -> Result<(), crate::content_manager::errors::StorageError> {
            Ok(())
        }
    }

    /// Regression test for the shared awaiter slot.
    ///
    /// Callers proposing an identical operation deduplicate onto one broadcast channel, and the
    /// map holds its only `Sender`. Before the fix, the first caller to time out removed the
    /// entry, closing the channel for the others: they failed with `Channel sender dropped`
    /// even though the operation was still in flight and went on to apply successfully.
    #[tokio::test]
    async fn timeout_keeps_awaiter_alive_for_other_waiters() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let operation = ConsensusOperations::RemovePeer(1);

        // Two callers awaiting the same in-flight operation, as `propose_consensus_op_with_await`
        // would register them: the first inserts the sender, the second subscribes to it.
        let (sender, first_receiver) = broadcast::channel(1);
        let mut second_receiver = sender.subscribe();
        consensus_state
            .on_consensus_op_apply
            .lock()
            .insert(operation.clone(), sender);

        // The first caller gives up.
        let timed_out = consensus_state
            .await_receiver(first_receiver, Duration::from_millis(10), &operation)
            .await;
        assert!(timed_out.is_err(), "first caller should have timed out");

        // The second caller is still waiting, so the awaiter must survive.
        assert!(
            consensus_state
                .on_consensus_op_apply
                .lock()
                .contains_key(&operation),
            "awaiter was dropped while another caller was still waiting",
        );

        // ...and it still gets the result once the operation applies.
        consensus_state
            .on_consensus_op_apply
            .lock()
            .remove(&operation)
            .expect("awaiter is still registered")
            .send(Ok(true))
            .expect("second caller is still subscribed");
        assert!(matches!(second_receiver.recv().await, Ok(Ok(true))));
    }

    /// The last caller to give up must clean the entry up, otherwise the map grows without bound.
    #[tokio::test]
    async fn timeout_removes_awaiter_when_last_waiter_leaves() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let operation = ConsensusOperations::RemovePeer(1);

        let (sender, receiver) = broadcast::channel(1);
        consensus_state
            .on_consensus_op_apply
            .lock()
            .insert(operation.clone(), sender);

        let timed_out = consensus_state
            .await_receiver(receiver, Duration::from_millis(10), &operation)
            .await;
        assert!(timed_out.is_err(), "caller should have timed out");

        assert!(
            !consensus_state
                .on_consensus_op_apply
                .lock()
                .contains_key(&operation),
            "awaiter leaked after its only waiter gave up",
        );
    }

    /// Two callers giving up at the same time must still leave the map clean: each drops its own
    /// receiver before checking, so the last one out sees no waiters left and removes the entry.
    #[tokio::test]
    async fn concurrent_timeouts_leave_no_awaiter_behind() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let operation = ConsensusOperations::RemovePeer(1);

        let (sender, first_receiver) = broadcast::channel(1);
        let second_receiver = sender.subscribe();
        consensus_state
            .on_consensus_op_apply
            .lock()
            .insert(operation.clone(), sender);

        let (first, second) = tokio::join!(
            consensus_state.await_receiver(first_receiver, Duration::from_millis(10), &operation),
            consensus_state.await_receiver(second_receiver, Duration::from_millis(10), &operation),
        );
        assert!(
            first.is_err() && second.is_err(),
            "both should have timed out"
        );

        assert!(
            !consensus_state
                .on_consensus_op_apply
                .lock()
                .contains_key(&operation),
            "awaiter leaked after both waiters gave up",
        );
    }

    /// `await_for_multiple_operations` registers an awaiter per operation, so it has to
    /// deregister them when it gives up, or the map grows on every timed-out batch.
    #[tokio::test]
    async fn multiple_operations_timeout_removes_own_awaiters() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let operations = vec![
            ConsensusOperations::RemovePeer(1),
            ConsensusOperations::RemovePeer(2),
        ];

        let timed_out = consensus_state
            .await_for_multiple_operations(operations.clone(), Some(Duration::from_millis(10)))
            .await;
        assert!(timed_out.is_err(), "batch should have timed out");

        let on_apply_lock = consensus_state.on_consensus_op_apply.lock();
        for operation in &operations {
            assert!(
                !on_apply_lock.contains_key(operation),
                "awaiter leaked after the batch timed out: {operation:?}",
            );
        }
    }

    /// The awaiters are registered before the future is polled, because the caller submits the
    /// operations only once they are in place. `Dispatcher::submit_collection_meta_op` then drops
    /// the future unpolled whenever proposing the operation itself fails, so dropping it must
    /// deregister them too. Otherwise a rejected operation leaves a dead entry behind that the
    /// next identical request deduplicates onto and never hears back from.
    #[tokio::test]
    async fn multiple_operations_dropped_unpolled_removes_own_awaiters() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let operations = vec![
            ConsensusOperations::RemovePeer(1),
            ConsensusOperations::RemovePeer(2),
        ];

        let awaiter = consensus_state
            .await_for_multiple_operations(operations.clone(), Some(Duration::from_millis(10)));
        assert_eq!(
            consensus_state.on_consensus_op_apply.lock().len(),
            operations.len(),
            "awaiters must be registered before the future is polled",
        );
        drop(awaiter);

        let on_apply_lock = consensus_state.on_consensus_op_apply.lock();
        for operation in &operations {
            assert!(
                !on_apply_lock.contains_key(operation),
                "awaiter leaked after the batch was dropped unpolled: {operation:?}",
            );
        }
    }

    /// A timed-out batch must not tear down an awaiter another caller is still waiting on.
    #[tokio::test]
    async fn multiple_operations_timeout_keeps_other_waiters() {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let (consensus_state, _) = setup_storages(vec![], dir.path());

        let shared = ConsensusOperations::RemovePeer(1);
        let own = ConsensusOperations::RemovePeer(2);

        // Another caller is already awaiting `shared`, as `propose_consensus_op_with_await` would
        // have registered it. The batch below deduplicates onto that same sender.
        let (sender, mut other_receiver) = broadcast::channel(1);
        consensus_state
            .on_consensus_op_apply
            .lock()
            .insert(shared.clone(), sender);

        let timed_out = consensus_state
            .await_for_multiple_operations(
                vec![shared.clone(), own.clone()],
                Some(Duration::from_millis(10)),
            )
            .await;
        assert!(timed_out.is_err(), "batch should have timed out");

        {
            let on_apply_lock = consensus_state.on_consensus_op_apply.lock();
            assert!(
                on_apply_lock.contains_key(&shared),
                "awaiter was dropped while another caller was still waiting",
            );
            assert!(
                !on_apply_lock.contains_key(&own),
                "awaiter only the timed-out batch waited on should have been removed",
            );
        }

        // The other caller still gets its result once the operation applies.
        consensus_state
            .on_consensus_op_apply
            .lock()
            .remove(&shared)
            .expect("awaiter is still registered")
            .send(Ok(true))
            .expect("other caller is still subscribed");
        assert!(matches!(other_receiver.recv().await, Ok(Ok(true))));
    }

    fn setup_storages(
        entries: Vec<Entry>,
        path: &std::path::Path,
    ) -> (ConsensusManager<NoCollections>, MemStorage) {
        let persistent = Persistent::load_or_init(path, true, false, None).unwrap();
        let (sender, _) = mpsc::channel();
        let consensus_state = ConsensusManager::new(
            persistent,
            Arc::new(NoCollections),
            OperationSender::new(sender),
            path,
        )
        .expect("initialize consensus manager");
        let mem_storage = MemStorage::new();
        mem_storage.wl().append(entries.as_ref()).unwrap();
        consensus_state.append_entries(entries).unwrap();
        (consensus_state, mem_storage)
    }

    prop_compose! {
        fn gen_entries(min_entries: u64, max_entries: u64)(n in min_entries..max_entries, inc_term_every in 1u64..max_entries) -> Vec<Entry> {
            (1..=n).map(|index| Entry {index, term: 1 + index/inc_term_every, ..Default::default()}).collect::<Vec<Entry>>()
        }
    }

    // Each proptest case creates a persistent WAL on disk, which is very slow on Windows.
    #[cfg(target_os = "windows")]
    const PROPTEST_CASES: u32 = 10;
    #[cfg(not(target_os = "windows"))]
    const PROPTEST_CASES: u32 = 256;

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(PROPTEST_CASES))]

        #[test]
        fn check_first_and_last_indexes(entries in gen_entries(0, 100)) {
            let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
            let (consensus_state, mem_storage) = setup_storages(entries, dir.path());
            prop_assert_eq!(mem_storage.last_index(), consensus_state.last_index());
            prop_assert_eq!(mem_storage.first_index(), consensus_state.first_index());
        }

        #[test]
        fn check_term(entries in gen_entries(0, 100), id in 0u64..100) {
            let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
            let (consensus_state, mem_storage) = setup_storages(entries, dir.path());
            prop_assert_eq!(mem_storage.term(id), consensus_state.term(id))
        }

        #[test]
        fn check_entries(entries in gen_entries(1, 100),
                low in 0u64..100,
                len in 1u64..100,
                max_size in proptest::option::of(proptest::num::u64::ANY)
            ) {
            let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
            let (consensus_state, mem_storage) = setup_storages(entries, dir.path());
            let mut high = low + len;
            let last_index = mem_storage.last_index().unwrap();
            if high > last_index + 1 {
                high = last_index + 1;
            }
            let mut low = low;
            if low > last_index {
                low = last_index;
            }
            let context_1 = raft::storage::GetEntriesContext::empty(false);
            let context_2 = raft::storage::GetEntriesContext::empty(false);
            prop_assert_eq!(mem_storage.entries(low, high, max_size, context_1), consensus_state.entries(low, high, max_size, context_2));
        }
    }

    #[test]
    fn recover_first_voter() {
        let (_dir, wal) = wal(0);
        let peers = vec![1337, 42, 69];
        assert_eq!(
            super::recover_first_voter(&wal, &peers).unwrap(),
            Some(1337)
        );
    }

    #[test]
    fn recover_first_voter_empty() {
        let (_dir, wal) = empty_wal();
        let peers = vec![1337, 42, 69];
        assert_eq!(super::recover_first_voter(&wal, &peers).unwrap(), None);
    }

    #[test]
    fn recover_first_voter_committed() {
        let (_dir, wal) = wal(1);
        let peers = vec![1337, 42, 69];
        assert_eq!(super::recover_first_voter(&wal, &peers).unwrap(), None);
    }

    #[test]
    fn recover_first_voter_truncated() {
        let (_dir, wal) = wal(2);
        let peers = vec![1337, 42, 69];
        assert_eq!(
            super::recover_first_voter(&wal, &peers).unwrap(),
            Some(PeerId::MAX)
        );
    }

    #[test]
    fn recover_first_voter_multiple_peers() {
        let (_dir, wal) = wal(0);
        let peers = vec![1337, 42, 69, 228];
        assert_eq!(
            super::recover_first_voter(&wal, &peers).unwrap(),
            Some(PeerId::MAX)
        );
    }

    fn wal(first_index: u64) -> (tempfile::TempDir, ConsensusOpWal) {
        let (dir, mut wal) = empty_wal();
        wal.append_entries(entries(first_index)).unwrap();
        (dir, wal)
    }

    /// Regression test for the flaky `test_peer_snapshot_bootstrap`.
    ///
    /// Scenario: a peer proposes `AddPeer` and registers an awaiter, but the proposal is
    /// committed remotely and delivered back to us as part of a raft snapshot rather than as a
    /// log entry. Before the fix, the awaiter was never notified — `apply_snapshot` only
    /// updated persistent state and did not touch `on_consensus_op_apply`. The awaiter would
    /// then time out after 10 seconds and `add_peer_to_known` would fail, killing the joining
    /// peer's bootstrap.
    #[test]
    fn apply_snapshot_notifies_pending_add_peer() {
        use std::collections::HashMap;

        use raft::eraftpb::{ConfState, Snapshot, SnapshotMetadata};

        use super::{ConsensusOperations, SnapshotData};
        use crate::types::{PeerAddressById, PeerMetadataById};

        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let persistent = Persistent::load_or_init(dir.path(), true, false, None).unwrap();
        let (sender, _) = mpsc::channel();
        let consensus = ConsensusManager::new(
            persistent,
            Arc::new(NoCollections),
            OperationSender::new(sender),
            dir.path(),
        )
        .unwrap();

        let new_peer_id: PeerId = 7372867103273069;
        let new_peer_uri = "http://127.0.0.1:20924/".to_string();

        // Register an awaiter directly, mimicking what `propose_consensus_op_with_await` does
        // before the proposal is forwarded to the leader.
        let operation = ConsensusOperations::AddPeer {
            peer_id: new_peer_id,
            uri: new_peer_uri.clone(),
        };
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        consensus
            .on_consensus_op_apply
            .lock()
            .insert(operation.clone(), tx);

        // Build a snapshot whose state contains the new peer (this is what arrives over the
        // wire when the leader truncates past our pending entry).
        let mut address_by_id: PeerAddressById = HashMap::new();
        address_by_id.insert(new_peer_id, new_peer_uri.parse().unwrap());

        let snapshot_data = SnapshotData {
            collections_data: super::CollectionsSnapshot::default(),
            address_by_id,
            metadata_by_id: PeerMetadataById::new(),
            cluster_metadata: HashMap::new(),
            quota_config: None,
        };

        let mut conf_state = ConfState::default();
        conf_state.learners.push(new_peer_id);

        let snapshot = Snapshot {
            data: serde_cbor::to_vec(&snapshot_data).unwrap(),
            metadata: Some(SnapshotMetadata {
                conf_state: Some(conf_state),
                index: 15,
                term: 2,
            }),
        };

        consensus.apply_snapshot(&snapshot).unwrap().unwrap();

        // The awaiter should have been notified successfully, and the entry should have been
        // removed from the pending map.
        let result = rx.try_recv().expect("awaiter must be notified by snapshot");
        assert!(result.is_ok(), "expected Ok notification, got {result:?}");
        assert!(
            !consensus
                .on_consensus_op_apply
                .lock()
                .contains_key(&operation),
            "satisfied operation should be removed from pending map",
        );
    }

    /// Companion to the above: a snapshot that does NOT contain the awaited peer must leave
    /// the awaiter untouched, so the caller can still time out / retry rather than being
    /// falsely told the operation succeeded.
    #[test]
    fn apply_snapshot_does_not_notify_unrelated_add_peer() {
        use std::collections::HashMap;

        use raft::eraftpb::{ConfState, Snapshot, SnapshotMetadata};

        use super::{ConsensusOperations, SnapshotData};
        use crate::types::{PeerAddressById, PeerMetadataById};

        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let persistent = Persistent::load_or_init(dir.path(), true, false, None).unwrap();
        let (sender, _) = mpsc::channel();
        let consensus = ConsensusManager::new(
            persistent,
            Arc::new(NoCollections),
            OperationSender::new(sender),
            dir.path(),
        )
        .unwrap();

        let operation = ConsensusOperations::AddPeer {
            peer_id: 12345,
            uri: "http://127.0.0.1:11111/".to_string(),
        };
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        consensus
            .on_consensus_op_apply
            .lock()
            .insert(operation.clone(), tx);

        // Snapshot mentions a *different* peer — our awaited operation is not yet satisfied.
        let mut address_by_id: PeerAddressById = HashMap::new();
        address_by_id.insert(99999, "http://127.0.0.1:22222/".parse().unwrap());

        let snapshot_data = SnapshotData {
            collections_data: super::CollectionsSnapshot::default(),
            address_by_id,
            metadata_by_id: PeerMetadataById::new(),
            cluster_metadata: HashMap::new(),
            quota_config: None,
        };

        let snapshot = Snapshot {
            data: serde_cbor::to_vec(&snapshot_data).unwrap(),
            metadata: Some(SnapshotMetadata {
                conf_state: Some(ConfState::default()),
                index: 1,
                term: 1,
            }),
        };

        consensus.apply_snapshot(&snapshot).unwrap().unwrap();

        assert_matches!(
            rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty),
            "awaiter must not be notified when snapshot does not satisfy the operation",
        );
        assert!(
            consensus
                .on_consensus_op_apply
                .lock()
                .contains_key(&operation),
            "unsatisfied operation should remain pending",
        );
    }

    fn empty_wal() -> (tempfile::TempDir, ConsensusOpWal) {
        let dir = Builder::new().prefix("raft_state_test").tempdir().unwrap();
        let wal = ConsensusOpWal::new(dir.path());
        (dir, wal)
    }

    fn entries(first_index: u64) -> Vec<Entry> {
        use ConfChangeType::*;

        let mut entries = vec![
            conf_change_v2(first_index, &[(AddNode, 1337)]),
            conf_change_v2(
                first_index + 1,
                &[(AddLearnerNode, 42), (AddLearnerNode, 69)],
            ),
            conf_change_v2(first_index + 2, &[(AddNode, 42)]),
            conf_change(first_index + 3, RemoveNode, 228),
            conf_change(first_index + 4, AddLearnerNode, 666),
            conf_change_v2(first_index + 5, &[(AddNode, 69)]),
            conf_change(first_index + 6, AddNode, 666),
        ];

        // Remove first entry if `first_index` is 0, so that second entry would line up with index 1
        if first_index == 0 {
            entries.remove(0);
        }

        entries
    }

    fn conf_change_v2(index: u64, changes: &[(ConfChangeType, PeerId)]) -> Entry {
        let mut conf_change = ConfChangeV2::default();

        for &(change_type, node_id) in changes {
            conf_change.changes.push(ConfChangeSingle {
                change_type: change_type as _,
                node_id,
            });
        }

        Entry {
            index,
            entry_type: EntryType::EntryConfChangeV2 as _,
            data: prost_for_raft::Message::encode_to_vec(&conf_change),
            ..Default::default()
        }
    }

    fn conf_change(index: u64, change_type: ConfChangeType, node_id: PeerId) -> Entry {
        let conf_change = ConfChange {
            change_type: change_type as _,
            node_id,
            ..Default::default()
        };

        Entry {
            index,
            entry_type: EntryType::EntryConfChange as _,
            data: prost_for_raft::Message::encode_to_vec(&conf_change),
            ..Default::default()
        }
    }
}
