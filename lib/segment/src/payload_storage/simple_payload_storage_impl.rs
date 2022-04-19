use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};
use std::collections::HashMap;

use rocksdb::Options;
use serde_json::Value;

use crate::entry::entry_point::OperationResult;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::payload_storage::PayloadStorage;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_NAME: &str = "payload";

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

    fn payload(&self, point_id: PointOffsetType) -> Payload {
        match self.payload.get(&point_id) {
            Some(payload) => payload.to_owned(),
            None => Default::default(),
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
        self.payload = HashMap::new();
        self.store.drop_cf(DB_NAME)?;
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        self.store.create_cf(DB_NAME, &options)?;
        Ok(())
    }

    fn flush(&self) -> OperationResult<()> {
        let cf_handle = self.store.cf_handle(DB_NAME).unwrap();
        Ok(self.store.flush_cf(cf_handle)?)
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.payload.keys().copied())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_wipe() {
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage = SimplePayloadStorage::open(dir.path()).unwrap();
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
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage = SimplePayloadStorage::open(dir.path()).unwrap();
        storage.assign(100, &payload).unwrap();
        let pload = storage.payload(100);
        assert_eq!(pload, payload);
    }
}
