use std::cmp;
use std::fs::{create_dir_all, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use atomicwrites::{AllowOverwrite, AtomicFile};
use collection::operations::types::PeerMetadata;
use collection::shards::shard::PeerId;
use http::Uri;
use parking_lot::RwLock;
use raft::eraftpb::{ConfState, HardState, SnapshotMetadata};
use raft::RaftState;
use serde::{Deserialize, Serialize};

use crate::content_manager::consensus::entry_queue::{EntryApplyProgressQueue, EntryId};
use crate::types::{PeerAddressById, PeerMetadataById};
use crate::StorageError;

// Deprecated, use `STATE_FILE_NAME` instead
const STATE_FILE_NAME_CBOR: &str = "raft_state";

const STATE_FILE_NAME: &str = "raft_state.json";

/// State of the Raft consensus, which should be saved between restarts.
/// State of the collections, aliases and transfers are stored as regular storage.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Persistent {
    /// last known state of the Raft consensus
    #[serde(with = "RaftStateDef")]
    pub state: RaftState,
    /// Store last applied snapshot index, required in case if there are no raft change log except
    /// for this last snapshot ID (term + commit)
    #[serde(default)] // TODO quick fix to avoid breaking the compat. with 0.8.1
    pub latest_snapshot_meta: SnapshotMetadataSer,
    /// Operations to applied, consensus consider them committed, but this peer didn't apply them yet
    #[serde(default)]
    pub apply_progress_queue: EntryApplyProgressQueue,
    /// Last known cluster topology
    #[serde(with = "serialize_peer_addresses")]
    pub peer_address_by_id: Arc<RwLock<PeerAddressById>>,
    #[serde(default)]
    pub peer_metadata_by_id: Arc<RwLock<PeerMetadataById>>,
    pub this_peer_id: PeerId,
    #[serde(skip)]
    pub path: PathBuf,
    /// Tracks if there are some unsaved changes due to the failure on save
    #[serde(skip)]
    pub dirty: AtomicBool,
}

impl Persistent {
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    pub fn latest_snapshot_meta(&self) -> &SnapshotMetadataSer {
        &self.latest_snapshot_meta
    }

    pub fn update_from_snapshot(
        &mut self,
        meta: &SnapshotMetadata,
        address_by_id: PeerAddressById,
        metadata_by_id: PeerMetadataById,
    ) -> Result<(), StorageError> {
        *self.peer_address_by_id.write() = address_by_id;
        *self.peer_metadata_by_id.write() = metadata_by_id;
        self.state.conf_state = meta.get_conf_state().clone();
        self.state.hard_state.term = cmp::max(self.state.hard_state.term, meta.term);
        self.state.hard_state.commit = meta.index;
        self.apply_progress_queue.set_from_snapshot(meta.index);
        self.latest_snapshot_meta = meta.into();
        self.save()
    }

    /// Returns state and if it was initialized for the first time
    pub fn load_or_init(
        storage_path: impl AsRef<Path>,
        first_peer: bool,
    ) -> Result<Self, StorageError> {
        create_dir_all(storage_path.as_ref())?;
        let path_legacy = storage_path.as_ref().join(STATE_FILE_NAME_CBOR);
        let path_json = storage_path.as_ref().join(STATE_FILE_NAME);
        let state = if path_json.exists() {
            log::info!("Loading raft state from {}", path_json.display());
            Self::load_json(path_json)?
        } else if path_legacy.exists() {
            log::info!("Loading raft state from {}", path_legacy.display());
            let mut state = Self::load(path_legacy)?;
            // migrate to json
            state.path = path_json;
            state.save()?;
            state
        } else {
            log::info!("Initializing new raft state at {}", path_json.display());
            Self::init(path_json, first_peer)?
        };

        log::debug!("State: {:?}", state);
        Ok(state)
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

    pub fn current_unapplied_entry(&self) -> Option<EntryId> {
        self.apply_progress_queue.current()
    }

    pub fn entry_applied(&mut self) -> Result<(), StorageError> {
        self.apply_progress_queue.applied();
        self.save()
    }

    pub fn set_unapplied_entries(
        &mut self,
        first_index: EntryId,
        last_index: EntryId,
    ) -> Result<(), StorageError> {
        self.apply_progress_queue.set(first_index, last_index);
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

    pub fn update_peer_metadata(
        &mut self,
        peer_id: PeerId,
        metadata: PeerMetadata,
    ) -> Result<(), StorageError> {
        if let Some(prev_metadata) = self
            .peer_metadata_by_id
            .write()
            .insert(peer_id, metadata.clone())
        {
            log::info!(
                "Replaced metadata of peer {peer_id} from {prev_metadata:?} to {metadata:?}"
            );
        } else {
            log::debug!("Added metadata for peer with id {peer_id}: {metadata:?}")
        }
        self.save()
    }

    pub fn last_applied_entry(&self) -> Option<u64> {
        self.apply_progress_queue.get_last_applied()
    }

    pub fn peer_address_by_id(&self) -> PeerAddressById {
        self.peer_address_by_id.read().clone()
    }

    pub fn peer_metadata_by_id(&self) -> PeerMetadataById {
        self.peer_metadata_by_id.read().clone()
    }

    pub fn is_our_metadata_outdated(&self) -> bool {
        self.peer_metadata_by_id
            .read()
            .get(&self.this_peer_id())
            .map_or(true, |metadata| metadata.is_different_version())
    }

    pub fn this_peer_id(&self) -> PeerId {
        self.this_peer_id
    }

    /// ## Arguments
    /// `path` - full name of the file where state will be saved
    ///
    /// `first_peer` - if this is a first peer in a new deployment (e.g. it does not bootstrap from anyone)
    /// It is `None` if distributed deployment is disabled
    fn init(path: PathBuf, first_peer: bool) -> Result<Self, StorageError> {
        // Do not generate too big peer ID, to avoid problems with serialization
        // (especially in json format)
        let this_peer_id = rand::random::<PeerId>() % (1 << 53);
        let voters = if first_peer {
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
                conf_state: ConfState::from((voters, vec![])),
            },
            apply_progress_queue: Default::default(),
            peer_address_by_id: Default::default(),
            peer_metadata_by_id: Default::default(),
            this_peer_id,
            path,
            latest_snapshot_meta: Default::default(),
            dirty: AtomicBool::new(false),
        };
        state.save()?;
        Ok(state)
    }

    fn load(path: PathBuf) -> Result<Self, StorageError> {
        let file = File::open(&path)?;
        let mut state: Self = serde_cbor::from_reader(&file)?;
        state.path = path;
        Ok(state)
    }

    fn load_json(path: PathBuf) -> Result<Self, StorageError> {
        let file = File::open(&path)?;
        let mut state: Self = serde_json::from_reader(&file)?;
        state.path = path;
        Ok(state)
    }

    pub fn save(&self) -> Result<(), StorageError> {
        let result = AtomicFile::new(&self.path, AllowOverwrite).write(|file| {
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, self)
        });
        log::trace!("Saved state: {:?}", self);
        self.dirty.store(result.is_err(), Ordering::Relaxed);
        Ok(result?)
    }

    pub fn save_if_dirty(&mut self) -> Result<(), StorageError> {
        if self.dirty.load(Ordering::Relaxed) {
            self.save()?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SnapshotMetadataSer {
    pub term: u64,
    /// Aka: commit
    pub index: u64,
}

impl From<&SnapshotMetadata> for SnapshotMetadataSer {
    fn from(meta: &SnapshotMetadata) -> Self {
        Self {
            term: meta.term,
            index: meta.index,
        }
    }
}

mod serialize_peer_addresses {
    use std::collections::HashMap;
    use std::sync::Arc;

    use http::Uri;
    use parking_lot::RwLock;
    use serde::{self, Deserializer, Serializer};

    use crate::serialize_peer_addresses;
    use crate::types::PeerAddressById;

    pub fn serialize<S>(
        addresses: &Arc<RwLock<PeerAddressById>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_peer_addresses::serialize(&addresses.read(), serializer)
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
