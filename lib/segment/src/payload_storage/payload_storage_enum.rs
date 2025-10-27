use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
#[cfg(feature = "testing")]
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::mmap_payload_storage::MmapPayloadStorage;
#[cfg(feature = "rocksdb")]
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
#[cfg(feature = "rocksdb")]
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::types::Payload;

#[derive(Debug)]
pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemoryPayloadStorage(InMemoryPayloadStorage),
    #[cfg(feature = "rocksdb")]
    SimplePayloadStorage(SimplePayloadStorage),
    #[cfg(feature = "rocksdb")]
    OnDiskPayloadStorage(OnDiskPayloadStorage),
    MmapPayloadStorage(MmapPayloadStorage),
}

#[cfg(feature = "testing")]
impl From<InMemoryPayloadStorage> for PayloadStorageEnum {
    fn from(a: InMemoryPayloadStorage) -> Self {
        PayloadStorageEnum::InMemoryPayloadStorage(a)
    }
}

#[cfg(feature = "rocksdb")]
impl From<SimplePayloadStorage> for PayloadStorageEnum {
    fn from(a: SimplePayloadStorage) -> Self {
        PayloadStorageEnum::SimplePayloadStorage(a)
    }
}

#[cfg(feature = "rocksdb")]
impl From<OnDiskPayloadStorage> for PayloadStorageEnum {
    fn from(a: OnDiskPayloadStorage) -> Self {
        PayloadStorageEnum::OnDiskPayloadStorage(a)
    }
}

impl From<MmapPayloadStorage> for PayloadStorageEnum {
    fn from(a: MmapPayloadStorage) -> Self {
        PayloadStorageEnum::MmapPayloadStorage(a)
    }
}

impl PayloadStorage for PayloadStorageEnum {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.overwrite(point_id, payload, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => {
                s.overwrite(point_id, payload, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => {
                s.overwrite(point_id, payload, hw_counter)
            }
            PayloadStorageEnum::MmapPayloadStorage(s) => s.overwrite(point_id, payload, hw_counter),
        }
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.set(point_id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.set(point_id, payload, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.set(point_id, payload, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.set(point_id, payload, hw_counter),
        }
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
            PayloadStorageEnum::MmapPayloadStorage(s) => {
                s.set_by_key(point_id, payload, key, hw_counter)
            }
        }
    }

    fn get(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.get(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.get(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get(point_id, hw_counter),
        }
    }

    fn get_sequential(
        &self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get_sequential(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.get_sequential(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.get_sequential(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get_sequential(point_id, hw_counter),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.delete(point_id, key, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.delete(point_id, key, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.delete(point_id, key, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.delete(point_id, key, hw_counter),
        }
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.clear(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.clear(point_id, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.clear(point_id, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear(point_id, hw_counter),
        }
    }

    #[cfg(test)]
    fn clear_all(&mut self, hw_counter: &HardwareCounterCell) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.clear_all(hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.clear_all(hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.clear_all(hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear_all(hw_counter),
        }
    }

    fn flusher(&self) -> (Flusher, Flusher) {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.flusher(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.flusher(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.flusher(),
        }
    }

    fn iter<F>(&self, callback: F, hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.iter(callback, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.iter(callback, hw_counter),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.iter(callback, hw_counter),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.iter(callback, hw_counter),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.files(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.files(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.files(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.files(),
        }
    }

    fn immutable_files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.immutable_files(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.immutable_files(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.immutable_files(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.immutable_files(),
        }
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get_storage_size_bytes(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.get_storage_size_bytes(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get_storage_size_bytes(),
        }
    }

    fn is_on_disk(&self) -> bool {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.is_on_disk(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(s) => s.is_on_disk(),
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.is_on_disk(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.is_on_disk(),
        }
    }
}

impl PayloadStorageEnum {
    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(_) => {}
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(_) => {}
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(_) => {}
            PayloadStorageEnum::MmapPayloadStorage(s) => s.populate()?,
        }
        Ok(())
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(_) => {}
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::SimplePayloadStorage(_) => {}
            #[cfg(feature = "rocksdb")]
            PayloadStorageEnum::OnDiskPayloadStorage(_) => {}
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear_cache()?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::Builder;

    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};
    use crate::types::Payload;

    #[test]
    #[cfg(feature = "rocksdb")]
    fn test_storage() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let hw_counter = HardwareCounterCell::new();

        let mut storage: PayloadStorageEnum = SimplePayloadStorage::open(db).unwrap().into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        assert!(!storage.get(100, &hw_counter).unwrap().is_empty());
        storage.clear_all(&hw_counter).unwrap();
        assert_eq!(storage.get(100, &hw_counter).unwrap(), Default::default());
    }

    #[rstest]
    fn test_mmap_storage(#[values(false, true)] populate: bool) {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();

        let hw_counter = HardwareCounterCell::new();

        let mut storage: PayloadStorageEnum =
            MmapPayloadStorage::open_or_create(dir.path().to_path_buf(), populate)
                .unwrap()
                .into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.clear_all(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        assert!(!storage.get(100, &hw_counter).unwrap().is_empty());
        storage.clear_all(&hw_counter).unwrap();
        assert_eq!(storage.get(100, &hw_counter).unwrap(), Default::default());
    }

    #[test]
    #[cfg(feature = "rocksdb")]
    fn test_on_disk_storage() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let hw_counter = HardwareCounterCell::new();

        {
            let mut storage: PayloadStorageEnum =
                SimplePayloadStorage::open(db.clone()).unwrap().into();
            let payload: Payload = serde_json::from_str(
                r#"{
                "name": "John Doe",
                "age": 52,
                "location": {
                    "city": "Melbourne",
                    "geo": {
                        "lon": 144.9631,
                        "lat": 37.8136
                    }
                }
            }"#,
            )
            .unwrap();

            storage.overwrite(100, &payload, &hw_counter).unwrap();

            let partial_payload: Payload = serde_json::from_str(r#"{ "age": 53 }"#).unwrap();
            storage.set(100, &partial_payload, &hw_counter).unwrap();

            storage
                .delete(100, &JsonPath::new("location.geo"), &hw_counter)
                .unwrap();

            let res = storage.get(100, &hw_counter).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));
        }

        {
            let mut storage: PayloadStorageEnum = OnDiskPayloadStorage::open(db).unwrap().into();

            let res = storage.get(100, &hw_counter).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");

            let partial_payload: Payload =
                serde_json::from_str(r#"{ "hobby": "vector search" }"#).unwrap();
            storage.set(100, &partial_payload, &hw_counter).unwrap();

            storage
                .delete(100, &JsonPath::new("location.city"), &hw_counter)
                .unwrap();
            storage
                .delete(100, &JsonPath::new("location"), &hw_counter)
                .unwrap();

            let res = storage.get(100, &hw_counter).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("hobby"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");
        }
    }

    #[test]
    #[cfg(feature = "rocksdb")]
    fn test_get_storage_size() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut storage = SimplePayloadStorage::open(db.clone()).unwrap();

        let hw_counter = HardwareCounterCell::new();

        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

        let point_id = 0;
        let payload: Payload = serde_json::from_str(
            r#"{
            "name": "John Doe",
            "age": 52,
            "location": {
                "city": "Melbourne",
                "geo": {
                    "lon": 144.9631,
                    "lat": 37.8136
                }
            }
        }"#,
        )
        .unwrap();

        let raw_payload_size = serde_cbor::to_vec(&point_id).unwrap().len() as u64
            + serde_json::to_vec(&payload).unwrap().len() as u64;

        assert_eq!(raw_payload_size, 98);

        // insert payload
        storage.overwrite(point_id, &payload, &hw_counter).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

        // needs a flush to impact the storage size
        storage.flush_all().unwrap();
        // large value contains initial cost of infra (SSTable, etc.), not stable across different OS
        let storage_size = storage.get_storage_size_bytes().unwrap();
        assert!(
            storage_size > 1000 && storage_size < 1300,
            "storage_size = {storage_size}"
        );

        // check how it scales
        for _ in 1..=100 {
            storage.overwrite(point_id, &payload, &hw_counter).unwrap();
        }

        storage.flush_all().unwrap();
        // loose assertion because value not stable across different OS
        let storage_size = storage.get_storage_size_bytes().unwrap();
        assert!(
            storage_size > 2000 && storage_size < 2500,
            "storage_size = {storage_size}"
        );
    }
}
