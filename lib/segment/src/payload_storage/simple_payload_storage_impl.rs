use std::collections::HashMap;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

impl PayloadStorage for SimplePayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge(payload),
            None => {
                self.payload.insert(point_id, payload.to_owned());
            }
        }

        self.update_storage(&point_id)?;

        Ok(())
    }

    fn assign_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &str,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge_by_key(payload, key),
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key)?;
                self.payload.insert(point_id, dest_payload);
                Ok(())
            }
        }
    }

    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self.payload.get(&point_id) {
            Some(payload) => Ok(payload.to_owned()),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    self.update_storage(&point_id)?;
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let res = self.payload.remove(&point_id);
        self.update_storage(&point_id)?;
        Ok(res)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.payload = HashMap::new();
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{open_db, DB_VECTOR_CF};

    #[test]
    fn test_wipe() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let mut storage = SimplePayloadStorage::open(db).unwrap();
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
    fn test_assign_payload_from_serde_json() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "boolean": "true",
            "floating": 30.5,
            "string_array": ["hello", "world"],
            "boolean_array": ["true", "false"],
            "float_array": [1.0, 2.0],
            "integer_array": [1, 2],
            "geo_data": {"type": "geo", "value": {"lon": 1.0, "lat": 1.0}},
            "metadata": {
                "height": 50,
                "width": 60,
                "temperature": 60.5,
                "nested": {
                    "feature": 30.5
                },
                "integer_array": [1, 2]
            }
        }"#;

        let payload: Payload = serde_json::from_str(data).unwrap();
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = SimplePayloadStorage::open(db).unwrap();
        storage.assign(100, &payload).unwrap();
        let pload = storage.payload(100).unwrap();
        assert_eq!(pload, payload);
    }
}
