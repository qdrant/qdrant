use std::collections::HashMap;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use collection::collection_state;
use collection::shard::{CollectionId, PeerId};
use parking_lot::{Mutex, RwLock};
use raft::eraftpb::{ConfChangeV2, Entry as RaftEntry};
use raft::{GetEntriesContext, RaftState, RawNode, SoftState, Storage};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tonic::transport::Uri;

use super::alias_mapping::AliasMapping;
use super::consensus_ops::ConsensusOperations;
use super::errors::StorageError;
use super::CollectionContainer;
use crate::content_manager::consensus::consensus_wal::ConsensusOpWal;
use crate::content_manager::consensus::entry_queue::EntryId;
use crate::content_manager::consensus::operation_sender::OperationSender;
use crate::content_manager::consensus::persistent::Persistent;
use crate::types::{
    ClusterInfo, ClusterStatus, ConsensusThreadStatus, PeerAddressById, PeerInfo, RaftInfo,
};

pub const DEFAULT_META_OP_WAIT: Duration = Duration::from_secs(10);

pub mod prelude {
    use crate::content_manager::toc::TableOfContent;

    pub type ConsensusState = super::ConsensusState<TableOfContent>;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotData {
    pub collections_data: CollectionsSnapshot,
    #[serde(with = "crate::serialize_peer_addresses")]
    pub address_by_id: PeerAddressById,
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

pub struct ConsensusState<C: CollectionContainer> {
    pub persistent: RwLock<Persistent>,
    wal: Mutex<ConsensusOpWal>,
    soft_state: RwLock<Option<SoftState>>,
    toc: Arc<C>,
    on_consensus_op_apply:
        Mutex<HashMap<ConsensusOperations, oneshot::Sender<Result<bool, StorageError>>>>,
    propose_sender: OperationSender,
    first_voter: RwLock<Option<PeerId>>,
    consensus_thread_status: RwLock<ConsensusThreadStatus>,
}

impl<C: CollectionContainer> ConsensusState<C> {
    pub fn new(
        persistent_state: Persistent,
        toc: Arc<C>,
        propose_sender: OperationSender,
        storage_path: &str,
    ) -> Self {
        Self {
            persistent: RwLock::new(persistent_state),
            wal: Mutex::new(ConsensusOpWal::new(storage_path)),
            soft_state: RwLock::new(None),
            toc,
            on_consensus_op_apply: Default::default(),
            propose_sender,
            first_voter: Default::default(),
            consensus_thread_status: RwLock::new(ConsensusThreadStatus::Working),
        }
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

    pub fn first_voter(&self) -> PeerId {
        match self.first_voter.read().as_ref() {
            Some(id) => *id,
            None => self.this_peer_id(),
        }
    }

    pub fn set_first_voter(&self, id: PeerId) {
        *self.first_voter.write() = Some(id);
    }

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
        })
    }

    pub fn apply_conf_change_entry<T: Storage>(
        &self,
        entry: &RaftEntry,
        raw_node: &mut RawNode<T>,
    ) -> Result<(), StorageError> {
        let change: ConfChangeV2 = prost::Message::decode(entry.get_data())?;
        let conf_state = raw_node.apply_conf_change(&change)?;
        self.persistent
            .write()
            .apply_state_update(|state| state.conf_state = conf_state)?;
        Ok(())
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

    pub fn apply_entries<T: Storage>(&self, raw_node: &mut RawNode<T>) {
        use raft::eraftpb::EntryType;

        if let Err(err) = self.persistent.write().save_if_dirty() {
            log::error!("Failed to save new state of applied entries queue: {err}");
            return;
        }

        loop {
            let unapplied_index = self.persistent.read().current_unapplied_entry();
            let entry_index = match unapplied_index {
                Some(index) => index,
                None => break,
            };
            log::debug!("Applying committed entry with index {entry_index}");
            let entry = match self.wal.lock().entry(entry_index) {
                Ok(entry) => entry,
                Err(err) => {
                    log::error!("Failed to get entry at index {entry_index}: {err}");
                    return;
                }
            };
            let do_increase_applied_index: bool = if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
                true
            } else {
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let operation_result = self.apply_normal_entry(&entry);
                        match operation_result {
                            Ok(result) => {
                                log::debug!(
                                    "Successfully applied consensus operation entry. Index: {}. Result: {result}",
                                    entry.index);
                                true
                            }
                            Err(err @ StorageError::ServiceError { .. }) => {
                                log::error!("Failed to apply collection meta operation entry with service error: {err}");
                                // This is a service error, so we can try to reapply it later.
                                false
                            }
                            Err(err) => {
                                log::warn!("Failed to apply collection meta operation entry with user error: {err}");
                                // This is a user error so we can safely consider it applied but with error as it was incorrect.
                                true
                            }
                        }
                    }
                    EntryType::EntryConfChangeV2 => {
                        match self.apply_conf_change_entry(&entry, raw_node) {
                            Ok(()) => {
                                log::debug!(
                                    "Successfully applied configuration change entry. Index: {}.",
                                    entry.index
                                );
                                true
                            }
                            Err(err) => {
                                log::error!(
                                    "Failed to apply configuration change entry with error: {err}"
                                );
                                false
                            }
                        }
                    }
                    ty => {
                        log::error!("Failed to apply entry: unsupported entry type {ty:?}");
                        false
                    }
                }
            };

            if do_increase_applied_index {
                if let Err(err) = self.persistent.write().entry_applied() {
                    log::error!("Failed to save new state of applied entries queue: {err}");
                    return;
                }
            } else {
                return;
            }
        }
    }

    pub fn apply_normal_entry(&self, entry: &RaftEntry) -> Result<bool, StorageError> {
        let operation: ConsensusOperations = entry.try_into()?;
        let on_apply = self.on_consensus_op_apply.lock().remove(&operation);
        let result = match operation {
            ConsensusOperations::CollectionMeta(operation) => {
                self.toc.perform_collection_meta_op(*operation)
            }
            ConsensusOperations::AddPeer(peer_id, uri) => self
                .add_peer(
                    peer_id,
                    uri.parse().map_err(|err| StorageError::ServiceError {
                        description: format!("Failed to parse Uri: {err}"),
                    })?,
                )
                .map(|()| true),
            ConsensusOperations::RemovePeer(peer_id) => self.remove_peer(peer_id).map(|()| true),
        };
        if let Some(on_apply) = on_apply {
            if on_apply.send(result.clone()).is_err() {
                log::warn!("Failed to notify on consensus operation completion: channel receiver is dropped")
            }
        }
        result
    }

    pub fn apply_snapshot(&self, snapshot: &raft::eraftpb::Snapshot) -> Result<(), StorageError> {
        let meta = snapshot.get_metadata();
        if raft::Storage::first_index(self)? > meta.index {
            return Err(StorageError::ServiceError {
                description: "Snapshot out of date".to_string(),
            });
        }
        let data: SnapshotData = snapshot.get_data().try_into()?;
        self.toc.apply_collections_snapshot(data.collections_data)?;
        self.wal.lock().0.clear()?;
        self.persistent
            .write()
            .update_from_snapshot(meta, data.address_by_id)?;
        Ok(())
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

    pub fn add_peer(&self, peer_id: PeerId, uri: Uri) -> Result<(), StorageError> {
        self.persistent.write().insert_peer(peer_id, uri)
    }

    pub fn remove_peer(&self, peer_id: PeerId) -> Result<(), StorageError> {
        if self.toc.peer_has_shards(peer_id) {
            return Err(StorageError::BadRequest {
                description: format!("Cannot remove peer {peer_id} as there are shards on it"),
            });
        }
        self.persistent.write().remove_peer(peer_id)
    }

    pub async fn propose_consensus_op(
        &self,
        operation: ConsensusOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        let (sender, receiver) = oneshot::channel();
        {
            let mut on_apply_lock = self.on_consensus_op_apply.lock();
            self.propose_sender.send(&operation)?;
            on_apply_lock.insert(operation, sender);
        }
        let wait_timeout = wait_timeout.unwrap_or(DEFAULT_META_OP_WAIT);
        tokio::time::timeout(wait_timeout, receiver)
            .await
            .map_err(
                |_: tokio::time::error::Elapsed| StorageError::ServiceError {
                    description: format!(
                        "Waiting for consensus operation commit failed. Timeout set at: {} seconds",
                        wait_timeout.as_secs_f64()
                    ),
                },
                // ?? - forwards 2 possible errors: sender dropped, operation failed
            )??
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.persistent.read().peer_address_by_id()
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
}

impl<C: CollectionContainer> Storage for ConsensusState<C> {
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
        if low < self.first_index()? {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

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
        let persistent = self.persistent.read();
        let snapshot_meta = persistent.latest_snapshot_meta();
        if idx == snapshot_meta.index {
            return Ok(snapshot_meta.term);
        }
        Ok(self.wal.lock().entry(idx)?.term)
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
        let persistent = self.persistent.read();
        let raft_state = persistent.state().clone();
        if raft_state.hard_state.commit >= request_index {
            let snapshot = SnapshotData {
                collections_data,
                address_by_id: persistent.peer_address_by_id(),
            };
            Ok(raft::eraftpb::Snapshot {
                data: serde_cbor::to_vec(&snapshot).map_err(raft_error_other)?,
                metadata: Some(raft::eraftpb::SnapshotMetadata {
                    conf_state: Some(raft_state.conf_state),
                    index: raft_state.hard_state.commit,
                    term: raft_state.hard_state.term,
                }),
            })
        } else {
            Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ))
        }
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
    use std::sync::{mpsc, Arc};

    use proptest::prelude::*;
    use raft::eraftpb::Entry;
    use raft::storage::{MemStorage, Storage};

    use super::ConsensusState;
    use crate::content_manager::consensus::consensus_wal::ConsensusOpWal;
    use crate::content_manager::consensus::entry_queue::EntryApplyProgressQueue;
    use crate::content_manager::consensus::operation_sender::OperationSender;
    use crate::content_manager::consensus::persistent::Persistent;
    use crate::content_manager::CollectionContainer;

    #[test]
    fn update_is_applied() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path(), false).unwrap();
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
        assert!(state
            .apply_state_update(|state| { state.hard_state.commit = 1 })
            .is_err());
    }

    #[test]
    fn state_is_loaded() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path(), false).unwrap();
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);

        let state_loaded = Persistent::load_or_init(dir.path(), false).unwrap();
        assert_eq!(state_loaded.state().hard_state.commit, 1);
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
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut wal = ConsensusOpWal::new(dir.path().to_str().unwrap());
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
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut wal = ConsensusOpWal::new(dir.path().to_str().unwrap());
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

        fn peer_has_shards(&self, _: u64) -> bool {
            false
        }
    }

    fn setup_storages(
        entries: Vec<Entry>,
        path: &std::path::Path,
    ) -> (ConsensusState<NoCollections>, MemStorage) {
        let persistent = Persistent::load_or_init(path, true).unwrap();
        let (sender, _) = mpsc::channel();
        let consensus_state = ConsensusState::new(
            persistent,
            Arc::new(NoCollections),
            OperationSender::new(sender),
            path.to_str().unwrap(),
        );
        let mem_storage = MemStorage::new();
        mem_storage.wl().append(entries.as_ref()).unwrap();
        consensus_state.append_entries(entries).unwrap();
        (consensus_state, mem_storage)
    }

    prop_compose! {
        fn gen_entries(min_entries: u64, max_entries: u64)(n in min_entries..max_entries, inc_term_every in 1u64..max_entries) -> Vec<Entry> {
            (1..(n+1)).into_iter().map(|index| Entry {index, term: 1 + index/inc_term_every, ..Default::default()}).collect::<Vec<Entry>>()
        }
    }

    proptest! {
        #[test]
        fn check_first_and_last_indexes(entries in gen_entries(0, 100)) {
            let dir = tempdir::TempDir::new("raft_state_test").unwrap();
            let (consensus_state, mem_storage) = setup_storages(entries, dir.path());
            prop_assert_eq!(mem_storage.last_index(), consensus_state.last_index());
            prop_assert_eq!(mem_storage.first_index(), consensus_state.first_index());
        }

        #[test]
        fn check_term(entries in gen_entries(0, 100), id in 0u64..100) {
            let dir = tempdir::TempDir::new("raft_state_test").unwrap();
            let (consensus_state, mem_storage) = setup_storages(entries, dir.path());
            prop_assert_eq!(mem_storage.term(id), consensus_state.term(id))
        }

        #[test]
        fn check_entries(entries in gen_entries(1, 100),
                low in 0u64..100,
                len in 1u64..100,
                max_size in proptest::option::of(proptest::num::u64::ANY)
            ) {
            let dir = tempdir::TempDir::new("raft_state_test").unwrap();
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
}
