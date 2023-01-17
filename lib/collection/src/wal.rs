use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::result;
use std::thread::JoinHandle;

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

/// Write-Ahead-Log wrapper with built-in type parsing.
/// Stores sequences of records of type `R` in binary files.
///
/// Each stored record is enumerated with sequential number.
/// Sequential number can be used to read stored records starting from some IDs,
/// for removing old, no longer required, records.
pub struct SerdeWal<R> {
    record: PhantomData<R>,
    wal: Wal,
}

impl<'s, R: DeserializeOwned + Serialize + Debug> SerdeWal<R> {
    pub fn new(dir: &str, wal_options: &WalOptions) -> Result<SerdeWal<R>> {
        let wal = Wal::with_options(dir, wal_options)
            .map_err(|err| WalError::InitWalError(format!("{:?}", err)))?;
        Ok(SerdeWal {
            record: PhantomData,
            wal,
        })
    }

    /// Write a record to the WAL but does guarantee durability.
    pub fn write(&mut self, entity: &R) -> Result<u64> {
        // ToDo: Replace back to faster rmp, once this https://github.com/serde-rs/serde/issues/2055 solved
        let binary_entity = serde_cbor::to_vec(&entity).unwrap();
        self.wal
            .append(&binary_entity)
            .map_err(|err| WalError::WriteWalError(format!("{:?}", err)))
    }

    pub fn read_all(&'s self) -> impl Iterator<Item = (u64, R)> + 's {
        self.read(self.wal.first_index())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u64 {
        self.wal.num_entries()
    }

    pub fn read(&'s self, start_from: u64) -> impl Iterator<Item = (u64, R)> + 's {
        let first_index = self.wal.first_index();
        let num_entries = self.wal.num_entries();

        (start_from..(first_index + num_entries)).map(move |idx| {
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
    ///
    pub fn ack(&mut self, until_index: u64) -> Result<()> {
        self.wal
            .prefix_truncate(until_index)
            .map_err(|err| WalError::TruncateWalError(format!("{:?}", err)))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.wal
            .flush_open_segment()
            .map_err(|err| WalError::WriteWalError(format!("{:?}", err)))
    }

    pub fn flush_async(&mut self) -> JoinHandle<std::io::Result<()>> {
        self.wal.flush_open_segment_async()
    }

    pub fn path(&self) -> &Path {
        self.wal.path()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_wal() {
        let dir = Builder::new().prefix("wal_test").tempdir().unwrap();
        let wal_options = WalOptions {
            segment_capacity: 32 * 1024 * 1024,
            segment_queue_len: 0,
        };

        let mut serde_wal: SerdeWal<TestRecord> =
            SerdeWal::new(dir.path().to_str().unwrap(), &wal_options).unwrap();

        let record = TestRecord::Struct1(TestInternalStruct1 { data: 10 });

        serde_wal.write(&record).expect("Can't write");

        let metadata = fs::metadata(dir.path().join("open-1").to_str().unwrap()).unwrap();

        println!("file size: {}", metadata.size());
        assert_eq!(metadata.size() as usize, wal_options.segment_capacity);

        for (_idx, rec) in serde_wal.read(0) {
            println!("{:?}", rec);
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
