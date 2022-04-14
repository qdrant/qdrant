use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

use atomicwrites::{AtomicFile, OverwriteBehavior::AllowOverwrite};
use prost::Message;
use raft::{
    eraftpb::{ConfState, HardState},
    RaftState,
};
use serde::{Deserialize, Serialize};

use super::errors::StorageError;

const STATE_FILE_NAME: &str = "raft_state";

type Current = u64;
type Last = u64;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
struct UnappliedEntries(Option<(Current, Last)>);

impl UnappliedEntries {
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
}

pub struct Persistent {
    state: RaftState,
    unapplied_entries: UnappliedEntries,
    path: PathBuf,
}

impl Persistent {
    pub fn state(&self) -> &RaftState {
        &self.state
    }

    pub fn load_or_init(storage_path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = storage_path.as_ref().join(STATE_FILE_NAME);
        if path.exists() {
            log::info!("Loading raft state from {}", path.display());
            let state = Self::load(path)?;
            log::info!("State: {:?}", state.state());
            Ok(state)
        } else {
            log::info!("Initializing new raft state at {}", path.display());
            let state = Self::init(path)?;
            log::info!("State: {:?}", state.state());
            Ok(state)
        }
    }

    pub fn apply_state_update(
        &mut self,
        update: impl FnOnce(&mut RaftState),
    ) -> Result<(), StorageError> {
        let mut state = self.state.clone();
        update(&mut state);
        save(&state, self.unapplied_entries, &self.path)?;
        // If saved correctly - apply state
        self.state = state;
        Ok(())
    }

    pub fn current_unapplied_entry(&self) -> Option<u64> {
        self.unapplied_entries.current()
    }

    pub fn entry_applied(&mut self) -> Result<(), StorageError> {
        let mut unapplied_entries = self.unapplied_entries;
        unapplied_entries.applied();
        save(&self.state, unapplied_entries, &self.path)?;
        // If saved correctly - apply state
        self.unapplied_entries = unapplied_entries;
        Ok(())
    }

    pub fn set_unapplied_entries(
        &mut self,
        first_index: u64,
        last_index: u64,
    ) -> Result<(), StorageError> {
        let unapplied_entries = UnappliedEntries(Some((first_index, last_index)));
        save(&self.state, unapplied_entries, &self.path)?;
        // If saved correctly - apply update
        self.unapplied_entries = unapplied_entries;
        Ok(())
    }

    fn init(path: PathBuf) -> Result<Self, StorageError> {
        let state = RaftState {
            hard_state: HardState::default(),
            // For network with 1 node, set it as learner. Node id is 1.
            conf_state: ConfState::from((vec![1], vec![])),
        };
        save(&state, UnappliedEntries::default(), &path)?;
        // If saved correctly - return state
        Ok(Persistent {
            state,
            path,
            unapplied_entries: UnappliedEntries::default(),
        })
    }

    fn load(path: PathBuf) -> Result<Self, StorageError> {
        let file = File::open(&path)?;
        let state: SerializableState = serde_cbor::from_reader(&file)?;
        Ok(Self {
            state: state.state()?,
            unapplied_entries: state.unapplied_entries,
            path,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SerializableState {
    hard_state: Vec<u8>,
    conf_state: Vec<u8>,
    unapplied_entries: UnappliedEntries,
}

impl SerializableState {
    pub fn new(
        state: &RaftState,
        unapplied_entries: UnappliedEntries,
    ) -> Result<Self, prost::EncodeError> {
        let mut hard_state = vec![];
        state.hard_state.encode(&mut hard_state)?;
        let mut conf_state = vec![];
        state.conf_state.encode(&mut conf_state)?;
        Ok(SerializableState {
            hard_state,
            conf_state,
            unapplied_entries,
        })
    }

    pub fn state(&self) -> Result<RaftState, prost::DecodeError> {
        Ok(RaftState {
            hard_state: HardState::decode(self.hard_state.as_slice())?,
            conf_state: ConfState::decode(self.conf_state.as_slice())?,
        })
    }
}

fn save(
    state: &RaftState,
    unapplied_entries: UnappliedEntries,
    path: impl AsRef<Path>,
) -> Result<(), StorageError> {
    let state = SerializableState::new(state, unapplied_entries)?;
    AtomicFile::new(path, AllowOverwrite).write(|file| {
        let writer = BufWriter::new(file);
        serde_cbor::to_writer(writer, &state)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use raft::RaftState;

    use crate::content_manager::raft_state::UnappliedEntries;

    use super::Persistent;

    #[test]
    fn update_is_applied() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path()).unwrap();
        assert_eq!(state.state().hard_state.commit, 0);
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);
    }

    #[test]
    fn update_is_not_applied_on_save_failure() {
        let mut state = Persistent {
            state: RaftState::default(),
            path: "./unexistent_dir/file".into(),
            unapplied_entries: UnappliedEntries::default(),
        };
        assert_eq!(state.state().hard_state.commit, 0);
        assert!(state
            .apply_state_update(|state| { state.hard_state.commit = 1 })
            .is_err());
        assert_eq!(state.state().hard_state.commit, 0);
    }

    #[test]
    fn state_is_loaded() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path()).unwrap();
        state
            .apply_state_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);

        let state_loaded = Persistent::load_or_init(dir.path()).unwrap();
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
