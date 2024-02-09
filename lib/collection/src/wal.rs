use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::result;
use std::thread::JoinHandle;

use io::file_operations::{atomic_save_json, read_json};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wal::{Wal, WalOptions};

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
#[error("{0}")]
pub enum WalError {
    #[error("Can't init WAL: {0}")]
    InitWalError(String),
    #[error("Can't write WAL: {0}")]
    WriteWalError(String),
    #[error("Can't truncate WAL: {0}")]
    TruncateWalError(String),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct TestInternalStruct1 {
    data: usize,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct TestInternalStruct2 {
    a: i32,
    b: i32,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(untagged)]
enum TestRecord {
    Struct1(TestInternalStruct1),
    Struct2(TestInternalStruct2),
}

type Result<T> = result::Result<T, WalError>;

#[derive(Debug, Deserialize, Serialize)]
struct WalState {
    pub ack_index: u64,
}

impl WalState {
    pub fn new(ack_index: u64) -> Self {
        Self { ack_index }
    }
}

/// Write-Ahead-Log wrapper with built-in type parsing.
/// Stores sequences of records of type `R` in binary files.
///
/// Each stored record is enumerated with sequential number.
/// Sequential number can be used to read stored records starting from some IDs,
/// for removing old, no longer required, records.
pub struct SerdeWal<R> {
    record: PhantomData<R>,
    wal: Wal,
    options: WalOptions,
    first_index: Option<u64>,
}

const FIRST_INDEX_FILE: &str = "first-index";

impl<'s, R: DeserializeOwned + Serialize + Debug> SerdeWal<R> {
    pub fn new(dir: &str, wal_options: WalOptions) -> Result<SerdeWal<R>> {
        let wal = Wal::with_options(dir, &wal_options)
            .map_err(|err| WalError::InitWalError(format!("{err:?}")))?;

        let first_index_path = Path::new(dir).join(FIRST_INDEX_FILE);

        let first_index = if first_index_path.exists() {
            let wal_state: WalState = read_json(&first_index_path).map_err(|err| {
                WalError::InitWalError(format!("failed to read first-index file: {err}"))
            })?;

            let first_index = wal_state
                .ack_index
                .max(wal.first_index())
                .min(wal.last_index());
            Some(first_index)
        } else {
            None
        };

        Ok(SerdeWal {
            record: PhantomData,
            wal,
            options: wal_options,
            first_index,
        })
    }

    /// Write a record to the WAL but does guarantee durability.
    pub fn write(&mut self, entity: &R) -> Result<u64> {
        // ToDo: Replace back to faster rmp, once this https://github.com/serde-rs/serde/issues/2055 solved
        let binary_entity = serde_cbor::to_vec(&entity).unwrap();
        self.wal
            .append(&binary_entity)
            .map_err(|err| WalError::WriteWalError(format!("{err:?}")))
    }

    pub fn read_all(&'s self) -> impl Iterator<Item = (u64, R)> + 's {
        self.read(self.first_index())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u64 {
        self.wal
            .num_entries()
            .saturating_sub(self.truncated_prefix_entries_num())
    }

    // WAL operates in *segments*, so when `Wal::prefix_truncate` is called (during `SerdeWal::ack`),
    // WAL is not truncated precisely up to the `until_index`, but up to the nearest segment with
    // `last_index` that is less-or-equal than `until_index`.
    //
    // Consider the pseudo-graphic illustration of the WAL that was truncated up to index 35:
    //
    // | -------- | -------- | ===='++++ | ++++++++ | ++++++++ | ++++++++ |
    // 10         20         30    35    40         50         60         70
    //
    // - ' marks the index 35 that has been truncated-to
    // - --- marks segments 10-30 that has been physically deleted
    // - +++ marks segments 35-70 that are still valid
    // - and === marks part of segment 30-35, that is still physically present on disk,
    //   but that is "logically" deleted
    //
    // `truncated_prefix_entries_num` returns the length of the "logically deleted" part of the WAL.
    fn truncated_prefix_entries_num(&self) -> u64 {
        self.first_index().saturating_sub(self.wal.first_index())
    }

    pub fn read(&'s self, start_from: u64) -> impl Iterator<Item = (u64, R)> + 's {
        let first_index = self.first_index();
        let len = self.len();

        (start_from..(first_index + len)).map(move |idx| {
            let record_bin = self.wal.entry(idx).expect("Can't read entry from WAL");
            let record: R = serde_cbor::from_slice(&record_bin)
                .or_else(|_err| rmp_serde::from_slice(&record_bin))
                .expect("Can't deserialize entry, probably corrupted WAL on version mismatch");
            (idx, record)
        })
    }

    /// Read all records from newest to oldest
    ///
    /// Normally this only iterates over our logical WAL, meaning all acknowledged records are
    /// excluded. If `with_physical` is set to `true`, it will also iterate over all acknowledged
    /// records we still have on disk.
    pub fn read_from_last(&'s self, with_physical: bool) -> impl Iterator<Item = (u64, R)> + 's {
        let first_index = if with_physical {
            self.first_physical_index()
        } else {
            self.first_index()
        };
        let last_index = self.last_index();

        (first_index..=last_index).rev().map(move |idx| {
            let record_bin = self.wal.entry(idx).expect("Can't read entry from WAL");
            let record: R = serde_cbor::from_slice(&record_bin)
                .or_else(|_err| rmp_serde::from_slice(&record_bin))
                .expect("Can't deserialize entry, probably corrupted WAL on version mismatch");
            (idx, record)
        })
    }

    /// Inform WAL, that records older than `until_index` are no longer required.
    /// If it is possible, WAL will remove unused files.
    ///
    /// # Arguments
    ///
    /// * `until_index` - the newest no longer required record sequence number
    pub(super) fn ack(&mut self, until_index: u64) -> Result<()> {
        // Truncate WAL
        self.wal
            .prefix_truncate(until_index)
            .map_err(|err| WalError::TruncateWalError(format!("{err:?}")))?;

        // Acknowledge index should not decrease
        let minimal_first_index = self.first_index.unwrap_or(self.wal.first_index());
        let new_first_index = Some(
            until_index
                .max(minimal_first_index)
                .min(self.wal.last_index()),
        );

        // Update current `first_index`
        if self.first_index != new_first_index {
            self.first_index = new_first_index;
            // Persist current `first_index` value on disk
            // TODO: Should we log this error and continue instead of failing?
            self.flush_first_index()?;
        }

        Ok(())
    }

    fn flush_first_index(&self) -> Result<()> {
        let Some(first_index) = self.first_index else {
            return Ok(());
        };

        atomic_save_json(
            &self.path().join(FIRST_INDEX_FILE),
            &WalState::new(first_index),
        )
        .map_err(|err| {
            WalError::TruncateWalError(format!("failed to write first-index file: {err:?}"))
        })?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.wal
            .flush_open_segment()
            .map_err(|err| WalError::WriteWalError(format!("{err:?}")))
    }

    pub fn flush_async(&mut self) -> JoinHandle<std::io::Result<()>> {
        self.wal.flush_open_segment_async()
    }

    pub fn path(&self) -> &Path {
        self.wal.path()
    }

    /// First index that is still available in physical WAL.
    pub fn first_physical_index(&self) -> u64 {
        self.wal.first_index()
    }

    /// First index that is still available in logical WAL.
    pub fn first_index(&self) -> u64 {
        self.first_index
            .unwrap_or_else(|| self.first_physical_index())
    }

    /// Last index that is still available in logical WAL.
    pub fn last_index(&self) -> u64 {
        self.wal.last_index()
    }

    pub fn segment_capacity(&self) -> usize {
        self.options.segment_capacity
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "windows"))]
    use std::fs;
    #[cfg(not(target_os = "windows"))]
    use std::os::unix::fs::MetadataExt;

    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_wal() {
        let dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let capacity = 32 * 1024 * 1024;
        let wal_options = WalOptions {
            segment_capacity: capacity,
            segment_queue_len: 0,
        };

        let mut serde_wal: SerdeWal<TestRecord> =
            SerdeWal::new(dir.path().to_str().unwrap(), wal_options).unwrap();

        let record = TestRecord::Struct1(TestInternalStruct1 { data: 10 });

        serde_wal.write(&record).expect("Can't write");

        #[cfg(not(target_os = "windows"))]
        {
            let metadata = fs::metadata(dir.path().join("open-1").to_str().unwrap()).unwrap();
            println!("file size: {}", metadata.size());
            assert_eq!(metadata.size() as usize, capacity);
        };

        for (_idx, rec) in serde_wal.read(0) {
            println!("{rec:?}");
        }

        let record = TestRecord::Struct2(TestInternalStruct2 { a: 12, b: 13 });

        serde_wal.write(&record).expect("Can't write");

        let mut read_iterator = serde_wal.read(0);

        let (idx1, record1) = read_iterator.next().unwrap();
        let (idx2, record2) = read_iterator.next().unwrap();

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);

        match record1 {
            TestRecord::Struct1(x) => assert_eq!(x.data, 10),
            TestRecord::Struct2(_) => panic!("Wrong structure"),
        }

        match record2 {
            TestRecord::Struct1(_) => panic!("Wrong structure"),
            TestRecord::Struct2(x) => {
                assert_eq!(x.a, 12);
                assert_eq!(x.b, 13);
            }
        }
    }
}
