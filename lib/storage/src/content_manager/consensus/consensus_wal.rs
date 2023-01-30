use std::fs::create_dir_all;
use std::path::Path;

use itertools::Itertools;
use prost::Message;
use raft::eraftpb::Entry as RaftEntry;
use raft::util::limit_size;
use wal::Wal;

use crate::content_manager::consensus_manager;
use crate::{ConsensusOperations, StorageError};

const COLLECTIONS_META_WAL_DIR: &str = "collections_meta_wal";

pub struct ConsensusOpWal(Wal);

impl ConsensusOpWal {
    pub fn new(storage_path: &str) -> Self {
        let collections_meta_wal_path = Path::new(storage_path).join(COLLECTIONS_META_WAL_DIR);
        create_dir_all(&collections_meta_wal_path)
            .expect("Can't create Collections meta Wal directory");
        ConsensusOpWal(Wal::open(collections_meta_wal_path).unwrap())
    }

    pub fn clear(&mut self) -> Result<(), StorageError> {
        Ok(self.0.clear()?)
    }

    pub fn entry(&self, id: u64) -> raft::Result<RaftEntry> {
        // Raft entries are expected to have index starting from 1
        if id < 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let first_entry = self
            .first_entry()
            .map_err(consensus_manager::raft_error_other)?
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
        .map_err(consensus_manager::raft_error_other)
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

    /// Difference between raft index and WAL record number.
    /// Difference might be different because of consensus snapshot.
    fn index_offset(&self) -> Result<Option<u64>, StorageError> {
        let last_known_index = self.0.first_index();
        let first_entry = self.first_entry()?;
        let offset = first_entry.map(|entry| entry.index - last_known_index);
        Ok(offset)
    }

    pub fn append_entries(&mut self, entries: Vec<RaftEntry>) -> Result<(), StorageError> {
        for entry in entries {
            let operation_opt = ConsensusOperations::try_from(&entry).ok();

            let index = entry.index;
            let current_index = self.0.last_index();
            let index_offset = self.index_offset()?;

            if let Some(offset) = index_offset {
                // Assume we can't skip index numbers in WAL except for snapshot
                // Example: 2 <= 0 + 1 + 1
                debug_assert!(
                    index <= current_index + offset + 1,
                    "Expected no index skip: {index} <= {current_index} + {offset}"
                );

                if index < current_index + offset {
                    // If there is a conflict, example:
                    // Offset = 1
                    // raft index = 10
                    // wal index = 11
                    // expected_wal_index = 10 - 1 = 9
                    // 10 < 11 + 1
                    if index < offset {
                        return Err(StorageError::service_error(&format!(
                            "Wal index conflict, raft index: {index}, wal index: {current_index}, offset: {offset}"
                        )));
                    }
                    log::debug!(
                        "Truncate conflicting WAL entries from index {}, raft: {}",
                        index - offset,
                        index
                    );
                    self.0.truncate(index - offset)?;
                } // else:
                  // Offset = 1
                  // raft index = 11
                  // wal index = 10
                  // expected_wal_index = 11 - 1 = 10
                  // 11 < 10 + 1
            } else {
                // There is no offset => there are no records in WAL
                // If there are no records, conflict is impossible
                // So we do nothing
            }

            if let Some(operation) = operation_opt {
                let term = entry.term;
                log::debug!(
                    "Appending operation: term: {term}, index: {index} entry: {operation:?}"
                );
            } else {
                log::debug!("Appending entry: {entry:?}");
            }

            let mut buf = vec![];
            entry.encode(&mut buf)?;
            #[allow(unused_variables)]
            let wal_index = self.0.append(&buf)?;
            #[cfg(debug_assertions)]
            if let Some(offset) = index_offset {
                debug_assert!(wal_index == index - offset);
            } else {
                debug_assert!(wal_index == 0)
            }
        }
        // flush consensus WAL to disk
        self.0.flush_open_segment()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use raft::eraftpb::Entry;

    use super::*;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_log_compaction_rewrite() {
        init_logger();
        let entries_orig = vec![
            Entry {
                entry_type: 0,
                term: 1,
                index: 1,
                data: vec![1, 2, 3],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 2,
                data: vec![1, 2, 3],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 3,
                data: vec![1, 2, 3],
                context: vec![],
                sync_log: false,
            },
        ];

        let entries_new = vec![
            Entry {
                entry_type: 0,
                term: 1,
                index: 2,
                data: vec![2, 2, 2],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 3,
                data: vec![3, 3, 3],
                context: vec![],
                sync_log: false,
            },
        ];

        let temp_dir = tempfile::tempdir().unwrap();

        let mut wal = ConsensusOpWal::new(temp_dir.path().to_str().unwrap());
        wal.append_entries(entries_orig).unwrap();
        wal.append_entries(entries_new.clone()).unwrap();

        let result_entries = wal.entries(1, 4, None).unwrap();
        assert_eq!(result_entries.len(), 3);

        assert_eq!(result_entries[0].data, vec![1, 2, 3]);
        assert_eq!(result_entries[1].data, vec![2, 2, 2]);
        assert_eq!(result_entries[2].data, vec![3, 3, 3]);

        wal.clear().unwrap();
        wal.append_entries(entries_new).unwrap();
        assert_eq!(wal.index_offset().unwrap(), Some(2));

        let broken_entry = vec![Entry {
            entry_type: 0,
            term: 1,
            index: 1, // Index 1 can't be overwritten, because it is already compacted
            data: vec![5, 5, 5],
            context: vec![],
            sync_log: false,
        }];

        // Some errors can't be corrected
        assert!(matches!(
            wal.append_entries(broken_entry),
            Err(StorageError::ServiceError { .. })
        ));
    }

    #[test]
    fn test_log_rewrite() {
        init_logger();
        let entries_orig = vec![
            Entry {
                entry_type: 0,
                term: 1,
                index: 1,
                data: vec![1, 1, 1],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 2,
                data: vec![1, 1, 1],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 3,
                data: vec![1, 1, 1],
                context: vec![],
                sync_log: false,
            },
        ];

        let entries_new = vec![
            Entry {
                entry_type: 0,
                term: 1,
                index: 2,
                data: vec![2, 2, 2],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 3,
                data: vec![2, 2, 2],
                context: vec![],
                sync_log: false,
            },
            Entry {
                entry_type: 0,
                term: 1,
                index: 4,
                data: vec![2, 2, 2],
                context: vec![],
                sync_log: false,
            },
        ];

        let temp_dir = tempfile::tempdir().unwrap();
        let mut wal = ConsensusOpWal::new(temp_dir.path().to_str().unwrap());

        // append original entries
        wal.append_entries(entries_orig).unwrap();
        assert_eq!(wal.0.num_segments(), 1);
        assert_eq!(wal.0.num_entries(), 3);
        assert_eq!(wal.index_offset().unwrap(), Some(1));
        assert_eq!(wal.first_entry().unwrap().unwrap().index, 1);
        assert_eq!(wal.last_entry().unwrap().unwrap().index, 3);

        let result_entries = wal.entries(1, 4, None).unwrap();
        assert_eq!(result_entries.len(), 3);
        assert_eq!(result_entries[0].data, vec![1, 1, 1]);
        assert_eq!(result_entries[1].data, vec![1, 1, 1]);
        assert_eq!(result_entries[2].data, vec![1, 1, 1]);

        // drop wal to check persistence
        drop(wal);
        let mut wal = ConsensusOpWal::new(temp_dir.path().to_str().unwrap());

        // append overlapping entries
        wal.append_entries(entries_new).unwrap();
        assert_eq!(wal.0.num_segments(), 1);
        assert_eq!(wal.0.num_entries(), 4);
        assert_eq!(wal.index_offset().unwrap(), Some(1));
        assert_eq!(wal.first_entry().unwrap().unwrap().index, 1);
        assert_eq!(wal.last_entry().unwrap().unwrap().index, 4);

        let result_entries = wal.entries(1, 5, None).unwrap();
        assert_eq!(result_entries.len(), 4);
        assert_eq!(result_entries[0].data, vec![1, 1, 1]); // survived the truncation
        assert_eq!(result_entries[1].data, vec![2, 2, 2]);
        assert_eq!(result_entries[2].data, vec![2, 2, 2]);
        assert_eq!(result_entries[3].data, vec![2, 2, 2]);

        // drop wal to check persistence
        drop(wal);
        let wal = ConsensusOpWal::new(temp_dir.path().to_str().unwrap());
        assert_eq!(wal.0.num_segments(), 1);
        assert_eq!(wal.0.num_entries(), 4); // fails here because we lost data!
        assert_eq!(wal.index_offset().unwrap(), Some(1));
        assert_eq!(wal.first_entry().unwrap().unwrap().index, 1);
        assert_eq!(wal.last_entry().unwrap().unwrap().index, 4);
    }
}
