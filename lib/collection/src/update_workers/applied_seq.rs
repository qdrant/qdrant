use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use common::save_on_disk::SaveOnDisk;
use fs_err as fs;
#[cfg(test)]
use io::file_operations::read_json;
use serde::{Deserialize, Serialize};

use crate::operations::types::CollectionResult;

/// How often the `applied_seq` is persisted
pub const APPLIED_SEQ_SAVE_INTERVAL: u64 = 64;

const APPLIED_SEQ_FILE: &str = "applied_seq.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct AppliedSeq {
    pub op_num: u64,
}

impl AppliedSeq {
    fn new(op_num: u64) -> Self {
        Self { op_num }
    }
}

#[derive(Debug)]
pub struct AppliedSeqHandler {
    /// the AppliedSeq atomic file
    file: Option<SaveOnDisk<AppliedSeq>>,
    /// path of the underlying persisted file
    path: PathBuf,
    /// precise in-memory op_num (can be larger than value persisted in file)
    /// invalid value if the file is not present
    op_num: AtomicU64,
    /// tracking update for interval based persistence
    update_count: AtomicU64,
}

impl AppliedSeqHandler {
    /// Get the current in-memory op_num.
    pub fn op_num(&self) -> Option<u64> {
        if self.file.is_some() {
            Some(self.op_num.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    /// Path for the applied_seq json file
    /// Not included in snapshots
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    #[cfg(test)]
    fn persisted_op_num(&self) -> u64 {
        read_json::<AppliedSeq>(&self.path).unwrap().op_num
    }

    /// Load or create the underlying applied seq file.
    ///
    /// `wal_first_index` is expected to be the next used WAL index.
    pub fn load_or_init(shard_path: &Path, wal_first_index: u64) -> Self {
        let last_applied_wal_index = wal_first_index.saturating_sub(1);
        let update_count = AtomicU64::new(0);
        let path = shard_path.join(APPLIED_SEQ_FILE);
        let file_was_already_present = path.exists();

        let loaded_file: Result<SaveOnDisk<AppliedSeq>, _> =
            SaveOnDisk::load_or_init(&path, || AppliedSeq::new(last_applied_wal_index));
        match loaded_file {
            Ok(file) => {
                let existing_op_num = file.read().op_num;
                debug_assert!(
                    existing_op_num <= last_applied_wal_index,
                    "persisted applied_seq:{existing_op_num} cannot be larger than the last_applied_wal_index:{last_applied_wal_index}"
                );
                let op_num = AtomicU64::new(existing_op_num);
                Self {
                    file: Some(file),
                    path,
                    op_num,
                    update_count,
                }
            }
            Err(err) => {
                if file_was_already_present {
                    log::error!("Error while loading existing applied_seq at {path:?} {err}");
                    // delete file as it is malformed
                    if let Err(err) = fs::remove_file(&path) {
                        log::error!("Could not deleted malformed applied_seq file {path:?} {err}");
                        let op_num = AtomicU64::new(last_applied_wal_index);
                        Self {
                            file: None,
                            path,
                            op_num,
                            update_count,
                        }
                    } else {
                        // try again to create the file from scratch
                        Self::load_or_init(shard_path, wal_first_index)
                    }
                } else {
                    log::error!("Error while creating new applied_seq at {path:?} {err}");
                    let op_num = AtomicU64::new(last_applied_wal_index);
                    Self {
                        file: None,
                        path,
                        op_num,
                        update_count,
                    }
                }
            }
        }
    }

    fn save(&self, op_num: u64) -> CollectionResult<()> {
        if let Some(file) = self.file.as_ref() {
            file.write(|current| current.op_num = op_num)?;
        }
        Ok(())
    }

    /// Update the underlying file every `APPLIED_SEQ_SAVE_INTERVAL` updates.
    /// Always update memory representation
    pub fn update(&self, op_num: u64) -> CollectionResult<()> {
        // update in-memory
        self.op_num.store(op_num, Ordering::Relaxed);
        let prev_count = self.update_count.fetch_add(1, Ordering::Relaxed);
        if prev_count == 0 {
            return Ok(());
        }
        // update on disk according to interval to amortize fsync
        if prev_count.is_multiple_of(APPLIED_SEQ_SAVE_INTERVAL) {
            self.save(op_num)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use crate::update_workers::applied_seq::{APPLIED_SEQ_SAVE_INTERVAL, AppliedSeqHandler};

    #[test]
    fn nothing_persisted_on_init() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 10);
        assert_eq!(handler.op_num(), Some(9));
        // nothing persisted yet because no updates observed
        assert!(!handler.path().exists());
    }

    #[test]
    fn persists_at_interval() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 1);
        for i in 0..APPLIED_SEQ_SAVE_INTERVAL {
            handler.update(i).unwrap();
            assert_eq!(handler.op_num(), Some(i));
            // nothing persisted yet because less updates than interval
            assert!(!handler.path().exists(), "exists too early at {i}");
        }

        // one more update to reach APPLIED_SEQ_SAVE_INTERVAL
        let op_num = 12345;
        handler.update(op_num).unwrap();
        assert_eq!(handler.op_num(), Some(op_num));
        assert!(handler.path().exists());
        // ensure written to disk
        assert_eq!(handler.persisted_op_num(), op_num);
    }

    #[test]
    fn read_existing_value_on_init() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 1);
        for i in 0..APPLIED_SEQ_SAVE_INTERVAL * 10 {
            handler.update(i).unwrap();
            assert_eq!(handler.op_num(), Some(i));
        }

        let op_num = 640;
        assert_eq!(APPLIED_SEQ_SAVE_INTERVAL * 10, op_num);
        handler.update(op_num).unwrap();
        assert_eq!(handler.op_num(), Some(op_num));
        assert!(handler.path().exists());
        // ensure written to disk
        assert_eq!(handler.persisted_op_num(), op_num);

        // drop and reload
        drop(handler);
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 650);
        assert_eq!(handler.op_num(), Some(op_num));
    }

    #[test]
    fn handles_file_corruption() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 1);
        let path = handler.path.clone();

        for i in 0..APPLIED_SEQ_SAVE_INTERVAL * 10 {
            handler.update(i).unwrap();
            assert_eq!(handler.op_num(), Some(i));
        }

        let op_num = 640;
        assert_eq!(APPLIED_SEQ_SAVE_INTERVAL * 10, op_num);
        handler.update(op_num).unwrap();
        assert_eq!(handler.op_num(), Some(op_num));
        assert!(handler.path().exists());
        // ensure written to disk
        assert_eq!(handler.persisted_op_num(), op_num);

        // drop and reload
        drop(handler);

        // open in append mode
        let mut file = fs_err::OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .unwrap();

        // corrupt data on disk by appending random bytes
        file.write_all(&[0, 1, 0, 1, 0, 1]).unwrap();
        drop(file);

        // reopen handler without crashing
        let handler = AppliedSeqHandler::load_or_init(dir.path(), 650);

        // regenerate new file with correct WAL op_num
        assert!(handler.file.is_some());
        assert_eq!(handler.op_num(), Some(649));
    }
}
