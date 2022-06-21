use std::cmp;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::ops::Deref;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use crate::types::{ClusterInfo, ClusterStatus, PeerAddressById, PeerInfo, RaftInfo};
use atomicwrites::{AtomicFile, OverwriteBehavior::AllowOverwrite};
use collection::{CollectionId, PeerId};
use parking_lot::{Mutex, RwLock};
use raft::eraftpb::ConfChangeV2;
use raft::RawNode;
use raft::{eraftpb::Entry as RaftEntry, SoftState, Storage};
use raft::{
    eraftpb::{ConfState, HardState},
    RaftState,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tonic::transport::Uri;
use wal::Wal;

use super::alias_mapping::AliasMapping;
use super::consensus_ops::ConsensusOperations;
use super::errors::StorageError;
use super::toc::TableOfContent;

const COLLECTIONS_META_WAL_DIR: &str = "collections_meta_wal";
const DEFAULT_META_OP_WAIT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotData {
    pub collections_data: CollectionsSnapshot,
    #[serde(with = "crate::serialize_peer_addresses")]
    pub address_by_id: PeerAddressById,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CollectionsSnapshot {
    pub collections: HashMap<CollectionId, collection::State>,
    pub aliases: AliasMapping,
}

impl TryFrom<&[u8]> for SnapshotData {
    type Error = serde_cbor::Error;

    fn try_from(bytes: &[u8]) -> Result<SnapshotData, Self::Error> {
        serde_cbor::from_slice(bytes)
    }
}

struct ConsensusOpWal(Wal);

impl ConsensusOpWal {
    pub fn new(storage_path: &str) -> Self {
        let collections_meta_wal_path = Path::new(storage_path).join(&COLLECTIONS_META_WAL_DIR);
        create_dir_all(&collections_meta_wal_path)
            .expect("Can't create Collections meta Wal directory");
        ConsensusOpWal(Wal::open(collections_meta_wal_path).unwrap())
    }

    fn entry(&self, id: u64) -> raft::Result<RaftEntry> {
        // Raft entries are expected to have index starting from 1
        if id < 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let first_entry = self
            .first_entry()
            .map_err(raft_error_other)?
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?;
        // Due to snapshots there might be different offsets between wal index and raft entry index
        let offset = first_entry.index - self.0.first_index();
        <RaftEntry as prost::Message>::decode(
            self.0
                .entry(id - offset)
                .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?
                .as_ref(),
        )
        .map_err(raft_error_other)
    }

    fn entries(&self, low: u64, high: u64, max_size: Option<u64>) -> raft::Result<Vec<RaftEntry>> {
        (low..high)
            .take(max_size.unwrap_or(high - low + 1) as usize)
            .map(|id| self.entry(id))
            .collect()
    }

    pub fn first_entry(&self) -> Result<Option<RaftEntry>, StorageError> {
        let first_index = self.0.first_index();
        let entry = self
            .0
            .entry(first_index)
            .map(|entry| <RaftEntry as prost::Message>::decode(entry.as_ref()));
        Ok(entry.transpose()?)
    }

    pub fn last_entry(&self) -> Result<Option<RaftEntry>, StorageError> {
        let last_index = self.0.last_index();
        let entry = self
            .0
            .entry(last_index)
            .map(|entry| <RaftEntry as prost::Message>::decode(entry.as_ref()));
        Ok(entry.transpose()?)
    }

    pub fn append_entries(&mut self, entries: Vec<RaftEntry>) -> Result<(), StorageError> {
        use prost::Message;

        for entry in entries {
            log::debug!("Appending entry: {entry:?}");
            let mut buf = vec![];
            entry.encode(&mut buf)?;
            self.0.append(&buf)?;
        }
        Ok(())
    }
}

pub struct ConsensusState {
    pub persistent: RwLock<Persistent>,
    wal: Mutex<ConsensusOpWal>,
    soft_state: RwLock<Option<SoftState>>,
    toc: Arc<TableOfContent>,
    on_consensus_op_apply:
        Mutex<HashMap<ConsensusOperations, oneshot::Sender<Result<bool, StorageError>>>>,
    propose_sender: Mutex<Sender<Vec<u8>>>,
}

impl ConsensusState {
    pub fn new(
        persistent_state: Persistent,
        toc: Arc<TableOfContent>,
        propose_sender: Sender<Vec<u8>>,
    ) -> Self {
        Self {
            persistent: RwLock::new(persistent_state),
            wal: Mutex::new(ConsensusOpWal::new(toc.storage_path())),
            soft_state: RwLock::new(None),
            toc,
            on_consensus_op_apply: Default::default(),
            propose_sender: Mutex::new(propose_sender),
        }
    }

    pub fn set_raft_soft_state(&self, state: &SoftState) {
        *self.soft_state.write() = Some(SoftState { ..*state });
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.persistent.read().this_peer_id
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
        ClusterStatus::Enabled(ClusterInfo {
            peer_id,
            peers,
            raft_info: RaftInfo {
                term: hard_state.term,
                commit: hard_state.commit,
                pending_operations,
                leader,
                role,
            },
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
        first_index: u64,
        last_index: u64,
    ) -> Result<(), raft::Error> {
        self.persistent
            .write()
            .set_unapplied_entries(first_index, last_index)
            .map_err(raft_error_other)
    }

    pub fn apply_entries<T: Storage>(&self, raw_node: &mut RawNode<T>) -> Result<(), raft::Error> {
        use raft::eraftpb::EntryType;

        loop {
            let unapplied_index = self.persistent.read().current_unapplied_entry();
            let entry_index = match unapplied_index {
                Some(index) => index,
                None => break,
            };
            log::debug!("Applying committed entry with index {entry_index}");
            let entry = self.wal.lock().entry(entry_index)?;
            if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
            } else {
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let operation_result = self.apply_normal_entry(&entry);
                        match operation_result {
                            Ok(result) => log::debug!(
                                "Successfully applied consensus operation entry. Index: {}. Result: {result}",
                                entry.index
                            ),
                            Err(err) => {
                                log::error!("Failed to apply collection meta operation entry with error: {err}")
                            }
                         }
                    }
                    EntryType::EntryConfChangeV2 => {
                        match self.apply_conf_change_entry(&entry, raw_node) {
                            Ok(()) => log::debug!(
                                "Successfully applied configuration change entry. Index: {}.",
                                entry.index
                            ),
                            Err(err) => {
                                log::error!(
                                    "Failed to apply configuration change entry with error: {err}"
                                )
                            }
                        }
                    }
                    ty => log::error!("Failed to apply entry: unsupported entry type {ty:?}"),
                }
            }

            self.persistent
                .write()
                .entry_applied()
                .map_err(raft_error_other)?;
        }
        Ok(())
    }

    pub fn apply_normal_entry(&self, entry: &RaftEntry) -> Result<bool, StorageError> {
        let operation: ConsensusOperations = entry.try_into()?;
        let on_apply = self.on_consensus_op_apply.lock().remove(&operation);
        let result = match operation {
            ConsensusOperations::CollectionMeta(operation) => {
                self.toc.perform_collection_meta_op_sync(*operation)
            }
            ConsensusOperations::AddPeer(peer_id, uri) => self
                .add_peer(
                    peer_id,
                    uri.parse().map_err(|err| StorageError::ServiceError {
                        description: format!("Failed to parse Uri: {err}"),
                    })?,
                )
                .map(|()| true),
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
        let mut persistent = self.persistent.write();
        persistent.set_peer_address_by_id(data.address_by_id)?;
        persistent.apply_state_update(move |state| {
            state.conf_state = meta.get_conf_state().clone();
            state.hard_state.term = cmp::max(state.hard_state.term, meta.term);
            state.hard_state.commit = meta.index
        })?;
        persistent
            .apply_progress_queue
            .set_from_snapshot(meta.index);
        Ok(())
    }

    pub fn set_hard_state(&self, hard_state: raft::eraftpb::HardState) -> Result<(), StorageError> {
        self.persistent
            .write()
            .apply_state_update(move |state| state.hard_state = hard_state)
    }

    pub fn hard_state(&self) -> raft::eraftpb::HardState {
        self.persistent.read().state().hard_state.clone()
    }

    pub fn set_commit_index(&self, index: u64) -> Result<(), StorageError> {
        self.persistent
            .write()
            .apply_state_update(|state| state.hard_state.commit = index)
    }

    pub fn add_peer(&self, peer_id: PeerId, uri: Uri) -> Result<(), StorageError> {
        self.persistent.write().insert_peer(peer_id, uri)
    }

    pub async fn propose_consensus_op(
        &self,
        operation: ConsensusOperations,
        wait_timeout: Option<Duration>,
    ) -> Result<bool, StorageError> {
        let serialized = serde_cbor::to_vec(&operation)?;
        let (sender, receiver) = oneshot::channel();
        self.on_consensus_op_apply.lock().insert(operation, sender);
        self.propose_sender.lock().send(serialized)?;
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

    pub fn is_new_deployment(&self) -> bool {
        self.persistent.read().is_new()
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.persistent.read().peer_address_by_id()
    }

    pub fn append_entries(&self, entries: Vec<RaftEntry>) -> Result<(), StorageError> {
        self.wal.lock().append_entries(entries)
    }
}

impl Storage for ConsensusState {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.persistent.read().state.clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<RaftEntry>> {
        let max_size: Option<_> = max_size.into();
        self.wal.lock().entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let persistent = self.persistent.read();
        let raft_state = persistent.state();
        if idx == raft_state.hard_state.commit {
            return Ok(raft_state.hard_state.term);
        }
        Ok(self.wal.lock().entry(idx)?.term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let index = match self.wal.lock().first_entry().map_err(raft_error_other)? {
            Some(entry) => entry.index,
            None => self.persistent.read().state().hard_state.commit + 1,
        };
        Ok(index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let index = match self.wal.lock().last_entry().map_err(raft_error_other)? {
            Some(entry) => entry.index,
            None => self.persistent.read().state().hard_state.commit,
        };
        Ok(index)
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<raft::eraftpb::Snapshot> {
        let collections_data = self.toc.collections_snapshot_sync();
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
pub struct ConsensusStateRef(pub Arc<ConsensusState>);

impl Deref for ConsensusStateRef {
    type Target = ConsensusState;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl From<ConsensusState> for ConsensusStateRef {
    fn from(state: ConsensusState) -> Self {
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
    ) -> raft::Result<Vec<RaftEntry>> {
        self.0.entries(low, high, max_size)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.0.term(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.0.first_index()
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.0.last_index()
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<raft::eraftpb::Snapshot> {
        self.0.snapshot(request_index)
    }
}

const STATE_FILE_NAME: &str = "raft_state";

type Current = u64;
type Last = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
struct EntryApplyProgressQueue(Option<(Current, Last)>);

impl EntryApplyProgressQueue {
    /// Return oldest un-applied entry id if any
    fn current(&self) -> Option<u64> {
        match self.0 {
            Some((current_index, last_index)) => {
                if current_index > last_index {
                    None
                } else {
                    Some(current_index)
                }
            }
            None => None,
        }
    }

    fn applied(&mut self) {
        match &mut self.0 {
            Some((current_index, _)) => {
                *current_index += 1;
            }
            None => (),
        }
    }

    fn get_last_applied(&self) -> Option<u64> {
        match &self.0 {
            Some((0, _)) => None,
            Some((current, _)) => Some(current - 1),
            None => None,
        }
    }

    fn set_from_snapshot(&mut self, snapshot_at_commit: u64) {
        self.0 = Some((snapshot_at_commit + 1, snapshot_at_commit))
    }

    pub fn len(&self) -> usize {
        match self.0 {
            None => 0,
            Some((current, last)) => (last as isize - current as isize + 1) as usize,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Persistent {
    #[serde(with = "RaftStateDef")]
    state: RaftState,
    apply_progress_queue: EntryApplyProgressQueue,
    #[serde(with = "serialize_peer_addresses")]
    pub peer_address_by_id: Arc<RwLock<PeerAddressById>>,
    this_peer_id: u64,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    new: bool,
}

impl Persistent {
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    pub fn load_or_init(
        storage_path: impl AsRef<Path>,
        first_peer: bool,
    ) -> Result<Self, StorageError> {
        create_dir_all(storage_path.as_ref())?;
        let path = storage_path.as_ref().join(STATE_FILE_NAME);
        let state = if path.exists() {
            log::info!("Loading raft state from {}", path.display());
            Self::load(path)?
        } else {
            log::info!("Initializing new raft state at {}", path.display());
            Self::init(path, first_peer)?
        };
        log::debug!("State: {:?}", state);
        Ok(state)
    }

    pub fn is_new(&self) -> bool {
        self.new
    }

    pub fn unapplied_entities_count(&self) -> usize {
        self.apply_progress_queue.len()
    }

    pub fn apply_state_update(
        &mut self,
        update: impl FnOnce(&mut RaftState),
    ) -> Result<(), StorageError> {
        let mut state = self.state.clone();
        update(&mut state);
        self.state = state;
        self.save()
    }

    pub fn current_unapplied_entry(&self) -> Option<u64> {
        self.apply_progress_queue.current()
    }

    pub fn entry_applied(&mut self) -> Result<(), StorageError> {
        self.apply_progress_queue.applied();
        self.save()
    }

    pub fn set_unapplied_entries(
        &mut self,
        first_index: u64,
        last_index: u64,
    ) -> Result<(), StorageError> {
        self.apply_progress_queue = EntryApplyProgressQueue(Some((first_index, last_index)));
        self.save()
    }

    pub fn set_peer_address_by_id(
        &mut self,
        peer_address_by_id: PeerAddressById,
    ) -> Result<(), StorageError> {
        *self.peer_address_by_id.write() = peer_address_by_id;
        self.save()
    }

    pub fn insert_peer(&mut self, peer_id: PeerId, address: Uri) -> Result<(), StorageError> {
        if let Some(prev_peer_address) = self
            .peer_address_by_id
            .write()
            .insert(peer_id, address.clone())
        {
            log::warn!("Replaced address of peer {peer_id} from {prev_peer_address} to {address}");
        } else {
            log::debug!("Added peer with id {peer_id} and address {address}")
        }
        self.save()
    }

    pub fn last_applied_entry(&self) -> Option<u64> {
        self.apply_progress_queue.get_last_applied()
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.peer_address_by_id.read().clone()
    }

    pub fn this_peer_id(&self) -> u64 {
        self.this_peer_id
    }

    /// ## Arguments
    /// `path` - full name of the file where state will be saved
    ///
    /// `first_peer` - if this is a first peer in a new deployment (e.g. it does not bootstrap from anyone)
    /// It is `None` if distributed deployment is disabled
    fn init(path: PathBuf, first_peer: bool) -> Result<Self, StorageError> {
        let this_peer_id = rand::random();
        let learners = if first_peer {
            vec![this_peer_id]
        } else {
            // `Some(false)` - Leave empty the network topology for the peer, if it is not starting a network itself.
            // This way it will not be able to become a leader and commit data
            // until it joins an existing network.
            vec![]
        };
        let state = Self {
            state: RaftState {
                hard_state: HardState::default(),
                // For network with 1 node, set it as voter.
                // First vec is voters, second is learners.
                conf_state: ConfState::from((learners, vec![])),
            },
            apply_progress_queue: Default::default(),
            peer_address_by_id: Default::default(),
            r#new: true,
            this_peer_id,
            path,
        };
        state.save()?;
        Ok(state)
    }

    fn load(path: PathBuf) -> Result<Self, StorageError> {
        let file = File::open(&path)?;
        let mut state: Self = serde_cbor::from_reader(&file)?;
        state.path = path;
        state.r#new = false;
        Ok(state)
    }

    fn save(&self) -> Result<(), StorageError> {
        Ok(AtomicFile::new(&self.path, AllowOverwrite).write(|file| {
            let writer = BufWriter::new(file);
            serde_cbor::to_writer(writer, self)
        })?)
    }
}

mod serialize_peer_addresses {
    use http::Uri;
    use parking_lot::RwLock;
    use serde::{self, Deserializer, Serializer};
    use std::{collections::HashMap, sync::Arc};

    use crate::{serialize_peer_addresses, types::PeerAddressById};

    pub fn serialize<S>(
        addresses: &Arc<RwLock<PeerAddressById>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_peer_addresses::serialize(&*addresses.read(), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<RwLock<PeerAddressById>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let addresses: HashMap<u64, Uri> = serialize_peer_addresses::deserialize(deserializer)?;
        Ok(Arc::new(RwLock::new(addresses)))
    }
}

/// Definition of struct to help with serde serialization.
/// Should be used only in `[serde(with=...)]`
#[derive(Serialize, Deserialize)]
#[serde(remote = "RaftState")]
struct RaftStateDef {
    #[serde(with = "HardStateDef")]
    hard_state: HardState,
    #[serde(with = "ConfStateDef")]
    conf_state: ConfState,
}

/// Definition of struct to help with serde serialization.
/// Should be used only in `[serde(with=...)]`
#[derive(Serialize, Deserialize)]
#[serde(remote = "HardState")]
struct HardStateDef {
    term: u64,
    vote: u64,
    commit: u64,
}

/// Definition of struct to help with serde serialization.
/// Should be used only in `[serde(with=...)]`
#[derive(Serialize, Deserialize)]
#[serde(remote = "ConfState")]
struct ConfStateDef {
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}

pub fn raft_error_other(e: impl std::error::Error) -> raft::Error {
    #[derive(thiserror::Error, Debug)]
    #[error("{0}")]
    struct StrError(String);

    raft::Error::Store(raft::StorageError::Other(Box::new(StrError(e.to_string()))))
}

#[cfg(test)]
mod tests {
    use super::{EntryApplyProgressQueue, Persistent};

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
        let mut entries = EntryApplyProgressQueue(Some((0, 2)));
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
}
