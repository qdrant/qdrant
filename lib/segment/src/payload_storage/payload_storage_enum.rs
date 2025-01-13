use std::path::PathBuf;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::json_path::JsonPath;
#[cfg(feature = "testing")]
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::mmap_payload_storage::MmapPayloadStorage;
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::Payload;

#[derive(Debug)]
pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemoryPayloadStorage(InMemoryPayloadStorage),
    SimplePayloadStorage(SimplePayloadStorage),
    OnDiskPayloadStorage(OnDiskPayloadStorage),
    MmapPayloadStorage(MmapPayloadStorage),
}

#[cfg(feature = "testing")]
impl From<InMemoryPayloadStorage> for PayloadStorageEnum {
    fn from(a: InMemoryPayloadStorage) -> Self {
        PayloadStorageEnum::InMemoryPayloadStorage(a)
    }
}

impl From<SimplePayloadStorage> for PayloadStorageEnum {
    fn from(a: SimplePayloadStorage) -> Self {
        PayloadStorageEnum::SimplePayloadStorage(a)
    }
}

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
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.overwrite(point_id, payload),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.overwrite(point_id, payload),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.overwrite(point_id, payload),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.overwrite(point_id, payload),
        }
    }

    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.set(point_id, payload),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.set(point_id, payload),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.set(point_id, payload),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.set(point_id, payload),
        }
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.set_by_key(point_id, payload, key),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.set_by_key(point_id, payload, key),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.set_by_key(point_id, payload, key),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.set_by_key(point_id, payload, key),
        }
    }

    fn get(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.get(point_id),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.get(point_id),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get(point_id),
        }
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &JsonPath) -> OperationResult<Vec<Value>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.delete(point_id, key),
        }
    }

    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.clear(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.clear(point_id),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.clear(point_id),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.clear(point_id),
        }
    }

    fn wipe(&mut self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.flusher(),
        }
    }

    fn iter<F>(&self, callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.iter(callback),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.iter(callback),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.iter(callback),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.iter(callback),
        }
    }

    fn files(&self) -> Vec<PathBuf> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.files(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.files(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.files(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.files(),
        }
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.get_storage_size_bytes(),
            PayloadStorageEnum::MmapPayloadStorage(s) => s.get_storage_size_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::types::Payload;

    #[test]
    fn test_storage() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut storage: PayloadStorageEnum = SimplePayloadStorage::open(db).unwrap().into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.set(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.set(100, &payload).unwrap();
        assert!(!storage.get(100).unwrap().is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.get(100).unwrap(), Default::default());
    }

    #[test]
    fn test_on_disk_storage() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

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

            storage.overwrite(100, &payload).unwrap();

            let partial_payload: Payload = serde_json::from_str(r#"{ "age": 53 }"#).unwrap();
            storage.set(100, &partial_payload).unwrap();

            storage.delete(100, &JsonPath::new("location.geo")).unwrap();

            let res = storage.get(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));
        }

        {
            let mut storage: PayloadStorageEnum = OnDiskPayloadStorage::open(db).unwrap().into();

            let res = storage.get(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");

            let partial_payload: Payload =
                serde_json::from_str(r#"{ "hobby": "vector search" }"#).unwrap();
            storage.set(100, &partial_payload).unwrap();

            storage
                .delete(100, &JsonPath::new("location.city"))
                .unwrap();
            storage.delete(100, &JsonPath::new("location")).unwrap();

            let res = storage.get(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("hobby"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");
        }
    }

    #[test]
    fn test_get_storage_size() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut storage = SimplePayloadStorage::open(db.clone()).unwrap();

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
        storage.overwrite(point_id, &payload).unwrap();
        assert_eq!(storage.get_storage_size_bytes().unwrap(), 0);

        // needs a flush to impact the storage size
        storage.flusher()().unwrap();
        // large value contains initial cost of infra (SSTable, etc.), not stable across different OS
        let storage_size = storage.get_storage_size_bytes().unwrap();
        assert!(
            storage_size > 1000 && storage_size < 1300,
            "storage_size = {storage_size}"
        );

        // check how it scales
        for _ in 1..=100 {
            storage.overwrite(point_id, &payload).unwrap();
        }

        storage.flusher()().unwrap();
        // loose assertion because value not stable across different OS
        let storage_size = storage.get_storage_size_bytes().unwrap();
        assert!(
            storage_size > 2000 && storage_size < 2300,
            "storage_size = {storage_size}"
        );
    }
}
