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

pub struct Persistent {
    state: RaftState,
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

    pub fn apply_update(
        &mut self,
        update: impl FnOnce(&mut RaftState),
    ) -> Result<(), StorageError> {
        let mut state = self.state.clone();
        update(&mut state);
        save(&state, &self.path)?;
        // If saved correctly - apply state
        self.state = state;
        Ok(())
    }

    fn init(path: PathBuf) -> Result<Self, StorageError> {
        let state = RaftState {
            hard_state: HardState::default(),
            // For network with 1 node, set it as learner. Node id is 1.
            conf_state: ConfState::from((vec![1], vec![])),
        };
        save(&state, &path)?;
        // If saved correctly - return state
        Ok(Persistent { state, path })
    }

    fn load(path: PathBuf) -> Result<Self, StorageError> {
        let file = File::open(&path)?;
        let state: BinaryState = serde_cbor::from_reader(&file)?;
        Ok(Self {
            state: state.try_into()?,
            path,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BinaryState {
    hard_state: Vec<u8>,
    conf_state: Vec<u8>,
}

impl TryFrom<BinaryState> for RaftState {
    type Error = prost::DecodeError;

    fn try_from(state: BinaryState) -> Result<Self, Self::Error> {
        Ok(RaftState {
            hard_state: HardState::decode(state.hard_state.as_slice())?,
            conf_state: ConfState::decode(state.conf_state.as_slice())?,
        })
    }
}

impl TryFrom<&RaftState> for BinaryState {
    type Error = prost::EncodeError;

    fn try_from(state: &RaftState) -> Result<Self, Self::Error> {
        let mut hard_state = vec![];
        state.hard_state.encode(&mut hard_state)?;
        let mut conf_state = vec![];
        state.conf_state.encode(&mut conf_state)?;
        Ok(BinaryState {
            hard_state,
            conf_state,
        })
    }
}

fn save(state: &RaftState, path: impl AsRef<Path>) -> Result<(), StorageError> {
    let state: BinaryState = state.try_into()?;
    AtomicFile::new(path, AllowOverwrite).write(|file| {
        let writer = BufWriter::new(file);
        serde_cbor::to_writer(writer, &state)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use raft::RaftState;

    use super::Persistent;

    #[test]
    fn update_is_applied() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path()).unwrap();
        assert_eq!(state.state().hard_state.commit, 0);
        state
            .apply_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);
    }

    #[test]
    fn update_is_not_applied_on_save_failure() {
        let mut state = Persistent {
            state: RaftState::default(),
            path: "./unexistent_dir/file".into(),
        };
        assert_eq!(state.state().hard_state.commit, 0);
        assert!(state
            .apply_update(|state| { state.hard_state.commit = 1 })
            .is_err());
        assert_eq!(state.state().hard_state.commit, 0);
    }

    #[test]
    fn state_is_loaded() {
        let dir = tempdir::TempDir::new("raft_state_test").unwrap();
        let mut state = Persistent::load_or_init(dir.path()).unwrap();
        state
            .apply_update(|state| state.hard_state.commit = 1)
            .unwrap();
        assert_eq!(state.state().hard_state.commit, 1);

        let state_loaded = Persistent::load_or_init(dir.path()).unwrap();
        assert_eq!(state_loaded.state().hard_state.commit, 1);
    }
}
