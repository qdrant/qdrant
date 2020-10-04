extern crate serde_cbor;
extern crate wal;

use std::marker::PhantomData;
use std::result;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use thiserror::Error;
use wal::Wal;
use wal::WalOptions;
use std::fmt::Debug;


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

pub struct SerdeWal<R> {
    record: PhantomData<R>,
    wal: Wal,
}

impl<'s, R: DeserializeOwned + Serialize + Debug> SerdeWal<R> {
    pub fn new(dir: &str, wal_options: &WalOptions) -> Result<SerdeWal<R>> {
        let wal = Wal::with_options(dir, wal_options)
            .map_err(|err| WalError::InitWalError(format!("{:?}", err)))?;
        return Ok(SerdeWal {
            record: PhantomData,
            wal,
        });
    }

    pub fn write(&mut self, entity: &R) -> Result<u64> {
        let binary_entity = rmp_serde::to_vec(&entity).unwrap();
        self.wal
            .append(&binary_entity)
            .map_err(|err| WalError::WriteWalError(format!("{:?}", err)))
    }

    pub fn read_all(&'s self) -> impl Iterator<Item=(u64, R)> + 's {
        self.read(self.wal.first_index())
    }

    pub fn len(&self) -> u64 {
        self.wal.num_entries()
    }

    pub fn read(&'s self, start_from: u64) -> impl Iterator<Item=(u64, R)> + 's {
        let first_index = self.wal.first_index();
        let num_entries = self.wal.num_entries();

        let iter = (start_from..(first_index + num_entries))
            .map(move |idx| {
                let record_bin = self.wal.entry(idx).expect("Can't read entry from WAL");
                let record: R = rmp_serde::from_read_ref(&record_bin.to_vec())
                    .expect("Can't deserialize entry, probably corrupted WAL on version mismatch");
                (idx, record)
            });

        return iter;
    }

    pub fn ack(&mut self, until_index: u64) -> Result<()> {
        self.wal.prefix_truncate(until_index).map_err(|err| WalError::TruncateWalError(format!("{:?}", err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate tempdir;

    use tempdir::TempDir;

    #[test]
    fn test_wal() {
        let dir = TempDir::new("wal_test").unwrap();
        let wal_options = WalOptions {
            segment_capacity: 1000,
            segment_queue_len: 0,
        };

        let mut serde_wal: SerdeWal<TestRecord> = SerdeWal::new(dir.path().to_str().unwrap(), &wal_options).unwrap();

        let record = TestRecord::Struct1(TestInternalStruct1 { data: 10 });

        serde_wal.write(&record).expect("Can't write");

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
            },
        }
    }
}
