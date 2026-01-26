use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use common::save_on_disk::SaveOnDisk;
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
    pub timestamp: u64,
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

    pub fn load_or_init(shard_path: &Path) -> Self {
        let update_count = AtomicU64::new(0);
        let path = shard_path.join(APPLIED_SEQ_FILE);
        // loading file can fail but we do not want to bubble up the error
        // to enable `op_num` to restore from WAL
        let loaded_file: Result<SaveOnDisk<AppliedSeq>, _> =
            SaveOnDisk::load_or_init_default(&path);
        match loaded_file {
            Ok(file) => {
                let existing_op_num = file.read().op_num;
                let op_num = AtomicU64::new(existing_op_num);
                Self {
                    file: Some(file),
                    path,
                    op_num,
                    update_count,
                }
            }
            Err(err) => {
                log::error!("Error while loading applied_seq at {path:?} {err}");
                // internal op_num placeholder, not exposed
                let op_num = AtomicU64::new(0);
                Self {
                    file: None,
                    path,
                    op_num,
                    update_count,
                }
            }
        }
    }

    fn save(&self, op_num: u64) -> CollectionResult<()> {
        if let Some(file) = self.file.as_ref() {
            file.write(|current| {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                current.op_num = op_num;
                current.timestamp = timestamp;
            })?;
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
        let handler = AppliedSeqHandler::load_or_init(dir.path());
        assert_eq!(handler.op_num(), Some(0));
        // nothing persisted yet because no updates observed
        assert!(!handler.path().exists());
    }

    #[test]
    fn persists_at_interval() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path());
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
        let handler = AppliedSeqHandler::load_or_init(dir.path());
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
        let handler = AppliedSeqHandler::load_or_init(dir.path());
        assert_eq!(handler.op_num(), Some(op_num));
    }

    #[test]
    fn handles_file_corruption() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path());
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
        let handler = AppliedSeqHandler::load_or_init(dir.path());

        // detects malformed file
        assert!(handler.file.is_none());
        assert!(handler.op_num().is_none());
    }
}
