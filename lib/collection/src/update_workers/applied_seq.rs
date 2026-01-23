use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use common::save_on_disk::SaveOnDisk;
#[cfg(test)]
use io::file_operations::read_json;
use serde::{Deserialize, Serialize};

/// How often the `applied_seq` is persisted
pub const APPLIED_SEQ_SAVE_INTERVAL: u64 = 64;

const APPLIED_SEQ_FILE: &str = "applied_seq.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct AppliedSeq {
    op_num: u64,
    timestamp: u64,
}

#[derive(Debug)]
pub struct AppliedSeqHandler {
    file: SaveOnDisk<AppliedSeq>,
    path: PathBuf,
    op_num: AtomicU64,
    update_count: AtomicU64,
}

impl AppliedSeqHandler {
    pub fn op_num(&self) -> u64 {
        self.op_num.load(Ordering::Relaxed)
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
        let path = shard_path.join(APPLIED_SEQ_FILE);
        let file = SaveOnDisk::load_or_init_default(&path).unwrap();

        let op_num = AtomicU64::new(0);
        let update_count = AtomicU64::new(0);
        Self {
            file,
            path,
            op_num,
            update_count,
        }
    }

    fn save(&self, op_num: u64) {
        self.file
            .write(|current| {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                debug_assert!(current.timestamp <= timestamp);
                current.op_num = op_num;
                current.timestamp = timestamp;
            })
            .unwrap();
    }

    /// Update the underlying file every `APPLIED_SEQ_SAVE_INTERVAL` updates.
    /// Always update memory representation
    pub fn update(&self, op_num: u64) {
        // update in-memory
        self.op_num.store(op_num, Ordering::Relaxed);
        let prev_count = self.update_count.fetch_add(1, Ordering::Relaxed);
        // update on disk according to interval to amortize fsync
        if prev_count + 1 >= APPLIED_SEQ_SAVE_INTERVAL {
            self.save(op_num);
            self.update_count.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::update_workers::applied_seq::{APPLIED_SEQ_SAVE_INTERVAL, AppliedSeqHandler};

    #[test]
    fn nothing_persisted_on_init() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path());
        assert_eq!(handler.op_num(), 0);
        // nothing persisted yet because no updates observed
        assert!(!handler.path().exists());
    }

    #[test]
    fn persists_at_interval() {
        let dir = TempDir::with_prefix("applied_seq").unwrap();
        let handler = AppliedSeqHandler::load_or_init(dir.path());
        for i in 1..APPLIED_SEQ_SAVE_INTERVAL {
            handler.update(i);
            assert_eq!(handler.op_num(), i);
            // nothing persisted yet because less updates than interval
            assert!(!handler.path().exists());
        }

        // one more update to reach APPLIED_SEQ_SAVE_INTERVAL
        let op_num = 12345;
        handler.update(op_num);
        assert_eq!(handler.op_num(), op_num);
        assert!(handler.path().exists());
        // ensure written to disk
        assert_eq!(handler.persisted_op_num(), op_num);
    }
}
