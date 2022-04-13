use crate::entry::entry_point::OperationResult;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};
use serde_json::Value;

pub enum PayloadStorageEnum {
    InMemoryPayloadStorage(InMemoryPayloadStorage),
    SimplePayloadStorage(SimplePayloadStorage),
}

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

impl PayloadStorage for PayloadStorageEnum {
    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.assign(point_id, payload),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.assign(point_id, payload),
        }
    }

    fn payload(&self, point_id: PointOffsetType) -> Payload {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.payload(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.payload(point_id),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<Value>> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.delete(point_id, key),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.delete(point_id, key),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.drop(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.drop(point_id),
        }
    }

    fn wipe(&mut self) -> OperationResult<()> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.wipe(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.wipe(),
        }
    }

    fn flush(&self) -> OperationResult<()> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.flush(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.flush(),
        }
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        match self {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.iter_ids(),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.iter_ids(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Payload;
    use tempdir::TempDir;

    #[test]
    fn test_storage() {
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage: PayloadStorageEnum =
            SimplePayloadStorage::open(dir.path()).unwrap().into();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        assert!(!storage.payload(100).is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100), Default::default());
    }
}
