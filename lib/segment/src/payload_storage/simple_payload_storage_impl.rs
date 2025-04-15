use std::path::PathBuf;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::Flusher;
use crate::common::operation_error::OperationResult;
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorage;
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

impl PayloadStorage for SimplePayloadStorage {
    fn overwrite(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        self.payload.insert(point_id, payload.to_owned());
        self.update_storage(point_id, hw_counter)?;
        Ok(())
    }

    fn set(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge(payload),
            None => {
                self.payload.insert(point_id, payload.to_owned());
            }
        }

        self.update_storage(point_id, hw_counter)?;

        Ok(())
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge_by_key(payload, key),
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                self.payload.insert(point_id, dest_payload);
            }
        }

        self.update_storage(point_id, hw_counter)?;

        Ok(())
    }

    fn get(&self, point_id: PointOffsetType, _: &HardwareCounterCell) -> OperationResult<Payload> {
        match self.payload.get(&point_id) {
            Some(payload) => Ok(payload.to_owned()),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Vec<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                if !res.is_empty() {
                    self.update_storage(point_id, hw_counter)?;
                }
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn clear(
        &mut self,
        point_id: PointOffsetType,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<Option<Payload>> {
        let res = self.payload.remove(&point_id);
        self.update_storage(point_id, hw_counter)?;
        Ok(res)
    }

    #[cfg(test)]
    fn wipe(&mut self, _: &HardwareCounterCell) -> OperationResult<()> {
        self.payload = ahash::AHashMap::new();
        self.db_wrapper.recreate_column_family()
    }

    fn flusher(&self) -> Flusher {
        self.db_wrapper.flusher()
    }

    fn iter<F>(&self, mut callback: F, _hw_counter: &HardwareCounterCell) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for (key, val) in self.payload.iter() {
            let do_continue = callback(*key, val)?;
            if !do_continue {
                return Ok(());
            }
        }
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        self.db_wrapper.get_storage_size_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tempfile::Builder;

    use super::*;
    use crate::common::rocksdb_wrapper::{DB_VECTOR_CF, open_db};

    #[test]
    fn test_wipe() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();

        let hw_counter = HardwareCounterCell::new();
        let mut storage = SimplePayloadStorage::open(db).unwrap();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.wipe(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        storage.wipe(&hw_counter).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        assert!(!storage.get(100, &hw_counter).unwrap().is_empty());
        storage.wipe(&hw_counter).unwrap();
        assert_eq!(storage.get(100, &hw_counter).unwrap(), Default::default());
    }

    #[test]
    fn test_set_by_key_consistency() {
        let dir = Builder::new().prefix("db_dir").tempdir().unwrap();

        let expected_payload: Payload =
            serde_json::from_str(r#"{"name": {"name": "Dohn Joe"}}"#).unwrap();

        let hw_counter = HardwareCounterCell::new();

        {
            let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
            let mut storage = SimplePayloadStorage::open(db).unwrap();

            let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
            storage.set(100, &payload, &hw_counter).unwrap();

            let new_payload: Payload = serde_json::from_str(r#"{"name": "Dohn Joe"}"#).unwrap();

            storage
                .set_by_key(
                    100,
                    &new_payload,
                    &JsonPath::from_str("name").unwrap(),
                    &hw_counter,
                )
                .unwrap();

            // Here it's `expected_payload`
            assert_eq!(storage.get(100, &hw_counter).unwrap(), expected_payload);
        }

        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let storage = SimplePayloadStorage::open(db).unwrap();
        assert_eq!(storage.get(100, &hw_counter).unwrap(), expected_payload); // Here must be `expected_payload` as well
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

        let hw_counter = HardwareCounterCell::new();
        let payload: Payload = serde_json::from_str(data).unwrap();
        let dir = Builder::new().prefix("storage_dir").tempdir().unwrap();
        let db = open_db(dir.path(), &[DB_VECTOR_CF]).unwrap();
        let mut storage = SimplePayloadStorage::open(db).unwrap();
        storage.set(100, &payload, &hw_counter).unwrap();
        let pload = storage.get(100, &hw_counter).unwrap();
        assert_eq!(pload, payload);
    }
}
