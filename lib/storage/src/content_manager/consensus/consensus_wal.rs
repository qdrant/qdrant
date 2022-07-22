use std::fs::create_dir_all;
use std::path::Path;

use itertools::Itertools;
use prost::Message;
use raft::eraftpb::Entry as RaftEntry;
use raft::util::limit_size;
use wal::Wal;

use crate::content_manager::consensus_state;
use crate::StorageError;

const COLLECTIONS_META_WAL_DIR: &str = "collections_meta_wal";

pub struct ConsensusOpWal(pub Wal);

impl ConsensusOpWal {
    pub fn new(storage_path: &str) -> Self {
        let collections_meta_wal_path = Path::new(storage_path).join(&COLLECTIONS_META_WAL_DIR);
        create_dir_all(&collections_meta_wal_path)
            .expect("Can't create Collections meta Wal directory");
        ConsensusOpWal(Wal::open(collections_meta_wal_path).unwrap())
    }

    pub fn entry(&self, id: u64) -> raft::Result<RaftEntry> {
        // Raft entries are expected to have index starting from 1
        if id < 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let first_entry = self
            .first_entry()
            .map_err(consensus_state::raft_error_other)?
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?;
        if id < first_entry.index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        // Due to snapshots there might be different offsets between wal index and raft entry index
        let offset = first_entry.index - self.0.first_index();
        <RaftEntry as prost::Message>::decode(
            self.0
                .entry(id - offset)
                .ok_or(raft::Error::Store(raft::StorageError::Unavailable))?
                .as_ref(),
        )
        .map_err(consensus_state::raft_error_other)
    }

    pub fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: Option<u64>,
    ) -> raft::Result<Vec<RaftEntry>> {
        let mut entries = (low..high).map(|id| self.entry(id)).try_collect()?;
        limit_size(&mut entries, max_size);
        Ok(entries)
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
        for entry in entries {
            log::debug!("Appending entry: {entry:?}");
            let mut buf = vec![];
            entry.encode(&mut buf)?;
            self.0.append(&buf)?;
        }
        Ok(())
    }
}
