use crate::types::{Payload, PayloadKeyTypeRef, PointOffsetType};
use std::collections::HashMap;
use std::path::Path;

use rocksdb::{IteratorMode, Options, DB};
use serde_json::Value;

use crate::entry::entry_point::OperationResult;
use crate::payload_storage::PayloadStorage;

const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_NAME: &str = "payload";

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, Payload>,
    store: DB,
}

impl SimplePayloadStorage {
    pub fn open(path: &Path) -> OperationResult<Self> {
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let store = DB::open_cf(&options, path, [DB_NAME])?;

        let mut payload_map: HashMap<PointOffsetType, Payload> = Default::default();

        let cf_handle = store.cf_handle(DB_NAME).unwrap();
        for (key, val) in store.iterator_cf(cf_handle, IteratorMode::Start) {
            let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
            let payload: Payload = serde_cbor::from_slice(&val).unwrap();

            //TODO(gvelo): handle schema properly
            //SimplePayloadStorage::update_schema(schema_store.clone(), &payload).unwrap();

            payload_map.insert(point_id, payload);
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            store,
        })
    }

    fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        let cf_handle = self.store.cf_handle(DB_NAME).unwrap();
        match self.payload.get(point_id) {
            None => self
                .store
                .delete_cf(cf_handle, serde_cbor::to_vec(&point_id).unwrap())?,
            Some(payload) => self.store.put_cf(
                cf_handle,
                serde_cbor::to_vec(&point_id).unwrap(),
                serde_cbor::to_vec(payload).unwrap(),
            )?,
        };
        Ok(())
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}

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
