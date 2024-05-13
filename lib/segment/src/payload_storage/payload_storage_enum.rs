use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::json_path::JsonPath;
#[cfg(feature = "testing")]
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::on_disk_payload_storage::OnDiskPayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::Payload;

pub enum PayloadStorageEnum {
    #[cfg(feature = "testing")]
    InMemoryPayloadStorage(InMemoryPayloadStorage),
    SimplePayloadStorage(SimplePayloadStorage),
    OnDiskPayloadStorage(OnDiskPayloadStorage),
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

impl PayloadStorageEnum {
    pub fn iter<F>(&self, callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.iter(callback),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.iter(callback),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.iter(callback),
        }
    }
}

impl PayloadStorage for PayloadStorageEnum {
    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.assign(point_id, payload),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.assign(point_id, payload),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.assign(point_id, payload),
        }
    }

    fn assign_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => {
                s.assign_by_key(point_id, payload, key)
            }
            PayloadStorageEnum::SimplePayloadStorage(s) => s.assign_by_key(point_id, payload, key),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.assign_by_key(point_id, payload, key),
        }
    }

    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.payload(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.payload(point_id),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.payload(point_id),
        }
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &JsonPath) -> OperationResult<Vec<Value>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.delete(point_id, key),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.drop(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.drop(point_id),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.drop(point_id),
        }
    }

    fn wipe(&mut self) -> OperationResult<()> {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.wipe(),
        }
    }

    fn flusher(&self) -> Flusher {
        match self {
            #[cfg(feature = "testing")]
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.flusher(),
            PayloadStorageEnum::OnDiskPayloadStorage(s) => s.flusher(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};
    use crate::json_path::path;
    use crate::types::Payload;

    #[test]
    fn test_storage() {
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut storage: PayloadStorageEnum = SimplePayloadStorage::open(db).unwrap().into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        assert!(!storage.payload(100).unwrap().is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100).unwrap(), Default::default());
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

            storage.assign_all(100, &payload).unwrap();

            let partial_payload: Payload = serde_json::from_str(r#"{ "age": 53 }"#).unwrap();
            storage.assign(100, &partial_payload).unwrap();

            storage.delete(100, &path("location.geo")).unwrap();

            let res = storage.payload(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));
        }

        {
            let mut storage: PayloadStorageEnum = OnDiskPayloadStorage::open(db).unwrap().into();

            let res = storage.payload(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("location"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");

            let partial_payload: Payload =
                serde_json::from_str(r#"{ "hobby": "vector search" }"#).unwrap();
            storage.assign(100, &partial_payload).unwrap();

            storage.delete(100, &path("location.city")).unwrap();
            storage.delete(100, &path("location")).unwrap();

            let res = storage.payload(100).unwrap();

            assert!(res.0.contains_key("age"));
            assert!(res.0.contains_key("hobby"));
            assert!(res.0.contains_key("name"));

            eprintln!("res = {res:#?}");
        }
    }
}
