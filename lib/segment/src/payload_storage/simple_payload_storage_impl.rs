use std::collections::HashMap;

use serde_json::Value;

use crate::common::rocksdb_operations::{db_options, DB_PAYLOAD_CF};
use crate::entry::entry_point::OperationResult;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};

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
    ) -> OperationResult<Option<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                if res.is_some() {
                    self.update_storage(&point_id)?;
                }
                Ok(res)
            }
            None => Ok(None),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let res = self.payload.remove(&point_id);
        self.update_storage(&point_id)?;
        Ok(res)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        let mut store_ref = self.store.borrow_mut();
        self.payload = HashMap::new();
        store_ref.drop_cf(DB_PAYLOAD_CF)?;
        store_ref.create_cf(DB_PAYLOAD_CF, &db_options())?;
        Ok(())
    }

    fn flush(&self) -> OperationResult<()> {
        let store_ref = self.store.borrow();
        let cf_handle = store_ref.cf_handle(DB_PAYLOAD_CF).unwrap();
        Ok(store_ref.flush_cf(cf_handle)?)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_operations::open_db;

    #[test]
    fn test_wipe() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path()).unwrap();

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
        let db = open_db(dir.path()).unwrap();
        let mut storage = SimplePayloadStorage::open(db).unwrap();
        storage.assign(100, &payload).unwrap();
        let pload = storage.payload(100).unwrap();
        assert_eq!(pload, payload);
    }
}
