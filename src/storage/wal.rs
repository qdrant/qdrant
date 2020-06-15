extern crate serde_cbor;
extern crate wal;

use std::marker::PhantomData;
use std::result;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use thiserror::Error;
use wal::Wal;
use wal::WalOptions;


#[derive(Error, Debug)]
pub enum WalError {
    #[error("WAL init error")]
    InitWalError(String),
    #[error("WAL writing error")]
    WriteWalError(String),
}

#[derive(Debug, Deserialize, Serialize)]
struct TestRecord {
    pub data: i32,
}

type Result<T> = result::Result<T, WalError>;

pub struct SerdeWal<R> {
    record: PhantomData<R>,
    wal: Wal,
}

impl<'s, R: DeserializeOwned + Serialize> SerdeWal<R> {
    pub fn new(dir: &str, wal_options: &WalOptions) -> Result<SerdeWal<R>> {
        let wal = Wal::with_options(dir, wal_options)
            .map_err(|err| WalError::InitWalError(format!("{:?}", err)))?;
        return Ok(SerdeWal {
            record: PhantomData,
            wal,
        });
    }

    pub fn write(&mut self, entity: &R) -> Result<u64> {
        let binary_entity = serde_cbor::to_vec(&entity).unwrap();
        self.wal
            .append(&binary_entity)
            .map_err(|err| WalError::WriteWalError(format!("Can't append to WAL: {:?}", err)))
    }

    pub fn read(&'s self, start_from: u64) -> impl Iterator<Item = (u64, R)> + 's {
        let first_index = self.wal.first_index();
        let num_entries = self.wal.num_entries();
        let iter = (first_index..(first_index + num_entries))
            .skip_while(move |&idx| idx < start_from)
            .map(move |idx| {
                let record_bin = self.wal.entry(idx).expect("Can't read entry from WAL");
                let record: R = serde_cbor::from_slice(&record_bin)
                    .expect("Can't deserialize entry, probably corrupted WAL on version mismatch");
                (idx, record)
            });

        return iter;
    }

    pub fn ack(&mut self, until_index: u64) -> Result<()> {
        self.wal.prefix_truncate(until_index).map_err(|err| WalError::WriteWalError(format!("Can't truncate WA:: {:?}", err)))
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

        let record = TestRecord { data: 10 };

        serde_wal.write(&record).expect("Can't write");

        for (_idx, rec) in serde_wal.read(0) {
            println!("{:?}", rec.data);
        }

        let record = TestRecord { data: 11 };

        serde_wal.write(&record).expect("Can't write");

        let mut read_iterator = serde_wal.read(0);

        let record1 = read_iterator.next();
        let record2 = read_iterator.next();

        assert!(record2.is_some());

        match record1 {
            Some((idx, record)) => {
                assert_eq!(idx, 0);
                assert_eq!(record.data, 10)
            },
            None => panic!("Record expected")
        }

        match record2 {
            Some((idx, record)) => {
                assert_eq!(idx, 1);
                assert_eq!(record.data, 11)
            },
            None => panic!("Record expected")
        }
    }
}
