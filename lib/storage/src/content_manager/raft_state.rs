use std::sync::Arc;
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use crate::types::PeerAddressById;
use atomicwrites::{AtomicFile, OverwriteBehavior::AllowOverwrite};
use collection::PeerId;
use raft::{
    eraftpb::{ConfState, HardState},
    RaftState,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Uri;

use super::errors::StorageError;

const STATE_FILE_NAME: &str = "raft_state";

type Current = u64;
type Last = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
struct UnappliedEntries(Option<(Current, Last)>);

impl UnappliedEntries {
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

    pub fn len(&self) -> usize {
        match self.0 {
            None => 0,
            Some((current, last)) => (last.saturating_sub(current) + 1) as usize,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Persistent {
    #[serde(with = "RaftStateDef")]
    state: RaftState,
    unapplied_entries: UnappliedEntries,
    #[serde(with = "serialize_peer_addresses")]
    pub peer_address_by_id: Arc<std::sync::RwLock<PeerAddressById>>,
    this_peer_id: u64,
    #[serde(skip)]
    path: PathBuf,
}

impl Persistent {
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    pub fn load_or_init(
        storage_path: impl AsRef<Path>,
        first_peer: Option<bool>,
    ) -> Result<Self, StorageError> {
        let path = storage_path.as_ref().join(STATE_FILE_NAME);
        if path.exists() {
            log::info!("Loading raft state from {}", path.display());
            let state = Self::load(path)?;
            log::info!("State: {:?}", state.state());
            Ok(state)
        } else {
            log::info!("Initializing new raft state at {}", path.display());
            let state = Self::init(path, first_peer)?;
            log::info!("State: {:?}", state.state());
            Ok(state)
        }
    }

    pub fn unapplied_entities_count(&self) -> usize {
        self.unapplied_entries.len()
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
        self.unapplied_entries.current()
    }

    pub fn entry_applied(&mut self) -> Result<(), StorageError> {
        self.unapplied_entries.applied();
        self.save()
    }

    pub fn set_unapplied_entries(
        &mut self,
        first_index: u64,
        last_index: u64,
    ) -> Result<(), StorageError> {
        self.unapplied_entries = UnappliedEntries(Some((first_index, last_index)));
        self.save()
    }

    pub fn set_peer_address_by_id(
        &mut self,
        peer_address_by_id: PeerAddressById,
    ) -> Result<(), StorageError> {
        *self.peer_address_by_id.write()? = peer_address_by_id;
        self.save()
    }

    pub fn insert_peer(&mut self, peer_id: PeerId, address: Uri) -> Result<(), StorageError> {
        if let Some(prev_peer_address) = self
            .peer_address_by_id
            .write()?
            .insert(peer_id, address.clone())
        {
            log::warn!("Replaced address of peer {peer_id} from {prev_peer_address} to {address}");
        }
        self.save()
    }

    pub fn peer_address_by_id(&self) -> Result<PeerAddressById, StorageError> {
        let peer_address_by_id = &self.peer_address_by_id.read()?;
        Ok((*peer_address_by_id).clone())
    }

    pub fn this_peer_id(&self) -> u64 {
        self.this_peer_id
    }

    /// ## Arguments
    /// `path` - full name of the file where state will be saved
    ///
    /// `first_peer` - if this is a first peer in a new deployment (e.g. it does not bootstrap from anyone)
    /// It is `None` if distributed deployment is disabled
    fn init(path: PathBuf, first_peer: Option<bool>) -> Result<Self, StorageError> {
        let this_peer_id = rand::random();
        let learners = if let Some(true) = first_peer {
            vec![this_peer_id]
        } else {
            // `Some(false)` - Leave empty the network topology for the peer, if it is not starting a network itself.
            // This way it will not be able to become a leader and commit data
            // until it joins an existing network.
            //
            // `None` - in this case, as distributed deployment is disabled, it doesn't matter what the state is.
            // But for general consistency reasons - better to leave it empty.
            vec![]
        };
        let state = Self {
            state: RaftState {
                hard_state: HardState::default(),
                // For network with 1 node, set it as voter.
                // First vec is voters, second is learners.
                conf_state: ConfState::from((learners, vec![])),
            },
            unapplied_entries: Default::default(),
            peer_address_by_id: Default::default(),
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
    use serde::{self, ser, Deserializer, Serializer};
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    };

    use crate::{serialize_peer_addresses, types::PeerAddressById};

    pub fn serialize<S>(
        addresses: &Arc<RwLock<PeerAddressById>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let addresses: HashMap<u64, Uri> = addresses
            .read()
            .map_err(|_| ser::Error::custom("RwLock is poisoned"))?
            .clone();
        serialize_peer_addresses::serialize(&addresses, serializer)
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

#[cfg(test)]
mod tests {
    use crate::content_manager::raft_state::UnappliedEntries;

    use super::Persistent;

    #[test]
    fn update_is_applied() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path(), None).unwrap();
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
        let mut state = Persistent::load_or_init(dir.path(), None).unwrap();
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);

        let state_loaded = Persistent::load_or_init(dir.path(), None).unwrap();
        assert_eq!(state_loaded.state().hard_state.commit, 1);
    }

    #[test]
    fn unapplied_entries() {
        let mut entries = UnappliedEntries(Some((0, 2)));
        assert_eq!(entries.current(), Some(0));
        entries.applied();
        assert_eq!(entries.current(), Some(1));
        entries.applied();
        assert_eq!(entries.current(), Some(2));
        entries.applied();
        assert_eq!(entries.current(), None);
    }
}
