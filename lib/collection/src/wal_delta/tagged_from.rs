use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// File in the WAL directory that persists [`TaggedFrom`]
///
/// The WAL ignores files in its directory that don't look like its segments.
const TAGGED_FROM_FILE: &str = "tagged-from";

/// The first WAL record from which every record has a clock tag
///
/// A WAL delta can't include records without a clock tag, so the delta scan stops at the first
/// untagged record it meets. A WAL delta can start no lower than this record.
///
/// It can be higher than the real boundary, never lower:
/// - writing an untagged record moves it past that record
/// - if it was never persisted, it starts at the first record the shard read on load
/// - if the end of the WAL is dropped, it can stay past the end, until new records reach it
///
/// It's persisted in the WAL directory, so it moves with the WAL, and a snapshot without the WAL
/// doesn't carry it. It must be persisted before the WAL is acknowledged: then the records below
/// the acknowledged index, which the shard doesn't read on load, are covered by the persisted
/// value.
#[derive(Debug)]
pub struct TaggedFrom {
    record: u64,

    /// Whether the record has changed since the last time it was persisted
    changed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct TaggedFromState {
    tagged_from: u64,
}

impl TaggedFrom {
    pub fn new(record: u64) -> Self {
        Self {
            record,
            changed: true,
        }
    }

    /// Load from the WAL directory, or start at `first_read_record` if it was never persisted
    ///
    /// `first_read_record` must be the first WAL record the shard reads on load. Untagged records
    /// below it are not seen, so we can't tell whether there are any.
    pub fn load_or_new(wal_path: &Path, first_read_record: u64) -> std::io::Result<Self> {
        let result = common::fs::read_json::<TaggedFromState>(&Self::path(wal_path));

        match result {
            Ok(TaggedFromState { tagged_from }) => Ok(Self {
                record: tagged_from,
                changed: false,
            }),

            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Ok(Self::new(first_read_record))
            }

            Err(err) => Err(err),
        }
    }

    pub fn store_if_changed(&mut self, wal_path: &Path) -> std::io::Result<()> {
        if !self.changed {
            return Ok(());
        }

        let state = TaggedFromState {
            tagged_from: self.record,
        };

        common::fs::atomic_save_json(&Self::path(wal_path), &state)?;
        self.changed = false;

        Ok(())
    }

    /// The record `op_num` has no clock tag
    pub fn mark_untagged(&mut self, op_num: u64) {
        let next = op_num + 1;

        if next > self.record {
            self.record = next;
            self.changed = true;
        }
    }

    /// How many records a WAL delta scan can read backwards from the end of the WAL
    ///
    /// The scan stops at the start of the WAL or before this record, whichever comes first.
    /// `first_index` is the first record still in the WAL, including acknowledged records, and
    /// `num_entries` is how many records it has.
    pub fn tagged_records(&self, first_index: u64, num_entries: u64) -> u64 {
        let end = first_index + num_entries;
        end.saturating_sub(self.record.max(first_index))
    }

    fn path(wal_path: &Path) -> PathBuf {
        wal_path.join(TAGGED_FROM_FILE)
    }
}

impl Default for TaggedFrom {
    /// Every record of a new, empty WAL starting at record 0 is tagged
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_mark_untagged_only_moves_forward() {
        let mut tagged_from = TaggedFrom::new(10);

        tagged_from.mark_untagged(15);
        assert_eq!(tagged_from.record, 16);

        tagged_from.mark_untagged(12);
        assert_eq!(tagged_from.record, 16);
    }

    #[test]
    fn test_tagged_records() {
        // WAL has records 100..=199
        let first_index = 100;
        let num_entries = 100;

        // Untagged record in the WAL, at 130
        let tagged_from = TaggedFrom::new(131);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 69);

        // Untagged record already truncated from the WAL
        let tagged_from = TaggedFrom::new(50);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 100);

        // Untagged record is the last one
        let tagged_from = TaggedFrom::new(200);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 0);

        // The end of the WAL was dropped after an untagged record
        let tagged_from = TaggedFrom::new(250);
        assert_eq!(tagged_from.tagged_records(first_index, num_entries), 0);

        // Empty WAL
        let tagged_from = TaggedFrom::new(0);
        assert_eq!(tagged_from.tagged_records(100, 0), 0);
    }

    #[test]
    fn test_store_and_load() {
        let dir = Builder::new().prefix("tagged_from").tempdir().unwrap();

        // Never persisted, start at the first record read on load, and persist it on store
        let mut tagged_from = TaggedFrom::load_or_new(dir.path(), 150).unwrap();
        assert_eq!(tagged_from.record, 150);
        assert!(tagged_from.changed);

        tagged_from.store_if_changed(dir.path()).unwrap();
        assert!(!tagged_from.changed);

        // Persisted, ignore the first record read on load
        tagged_from.mark_untagged(170);
        tagged_from.store_if_changed(dir.path()).unwrap();

        let tagged_from = TaggedFrom::load_or_new(dir.path(), 180).unwrap();
        assert_eq!(tagged_from.record, 171);
        assert!(!tagged_from.changed);
    }
}
