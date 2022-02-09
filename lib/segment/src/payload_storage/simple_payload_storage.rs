use crate::types::{
    PayloadKeyType, PayloadKeyTypeRef, PayloadSchemaType, PayloadType, PointOffsetType, TheMap,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rocksdb::{IteratorMode, Options, DB};

use crate::entry::entry_point::OperationResult;
use crate::payload_storage::schema_storage::SchemaStorage;
use crate::payload_storage::PayloadStorage;

/// Since sled is used for reading only during the initialization, large read cache is not required
const DB_CACHE_SIZE: usize = 10 * 1024 * 1024; // 10 mb
const DB_NAME: &str = "payload";

/// In-memory implementation of `PayloadStorage`.
/// Persists all changes to disk using `store`, but only uses this storage during the initial load
pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>>,
    store: DB,
    schema_store: Arc<SchemaStorage>,
}

impl SimplePayloadStorage {
    pub fn open(path: &Path, schema_store: Arc<SchemaStorage>) -> OperationResult<Self> {
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let store = DB::open_cf(&options, path, [DB_NAME])?;

        let mut payload_map: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>> =
            Default::default();

        let cf_handle = store.cf_handle(DB_NAME).unwrap();
        for (key, val) in store.iterator_cf(cf_handle, IteratorMode::Start) {
            let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
            let payload: TheMap<PayloadKeyType, PayloadType> =
                serde_cbor::from_slice(&val).unwrap();
            SimplePayloadStorage::update_schema(schema_store.clone(), &payload).unwrap();
            payload_map.insert(point_id, payload);
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            store,
            schema_store,
        })
    }

    fn update_schema(
        schema: Arc<SchemaStorage>,
        payload: &TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<()> {
        for (key, value) in payload.iter() {
            schema.update_schema_value(key, value)?;
        }
        Ok(())
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

    pub fn payload_ptr(
        &self,
        point_id: PointOffsetType,
    ) -> Option<&TheMap<PayloadKeyType, PayloadType>> {
        self.payload.get(&point_id)
    }
}

impl PayloadStorage for SimplePayloadStorage {
    fn assign(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
        payload: PayloadType,
    ) -> OperationResult<()> {
        self.schema_store.update_schema_value(key, &payload)?;

        if let Some(point_payload) = self.payload.get_mut(&point_id) {
            point_payload.insert(key.to_owned(), payload);
        } else {
            let mut new_payload = TheMap::default();
            new_payload.insert(key.to_owned(), payload);
            self.payload.insert(point_id, new_payload);
        }

        self.update_storage(&point_id)?;

        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType> {
        match self.payload.get(&point_id) {
            Some(payload) => payload.clone(),
            None => TheMap::new(),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<PayloadType>> {
        let point_payload = self.payload.get_mut(&point_id).unwrap();
        let res = point_payload.remove(key);
        self.update_storage(&point_id)?;
        Ok(res)
    }

    fn drop(
        &mut self,
        point_id: PointOffsetType,
    ) -> OperationResult<Option<TheMap<PayloadKeyType, PayloadType>>> {
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

    fn schema(&self) -> TheMap<PayloadKeyType, PayloadSchemaType> {
        self.schema_store.as_map()
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
        let mut storage =
            SimplePayloadStorage::open(dir.path(), Arc::new(SchemaStorage::new())).unwrap();
        let payload = PayloadType::Integer(vec![1, 2, 3]);
        let key = "key".to_owned();
        storage.assign(100, &key, payload.clone()).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &key, payload.clone()).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &key, payload).unwrap();
        assert!(!storage.payload(100).is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100).len(), 0);
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

        let v = serde_json::from_str(data).unwrap();
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage =
            SimplePayloadStorage::open(dir.path(), Arc::new(SchemaStorage::new())).unwrap();
        storage.assign_all_with_value(100, v).unwrap();
        let pload = storage.payload(100);
        let keys: Vec<_> = pload.keys().cloned().collect();
        assert!(keys.contains(&"geo_data".to_string()));
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"boolean".to_string()));
        assert!(keys.contains(&"floating".to_string()));
        assert!(keys.contains(&"metadata__temperature".to_string()));
        assert!(keys.contains(&"metadata__width".to_string()));
        assert!(keys.contains(&"metadata__height".to_string()));
        assert!(keys.contains(&"metadata__nested__feature".to_string()));
        assert!(keys.contains(&"string_array".to_string()));
        assert!(keys.contains(&"float_array".to_string()));
        assert!(keys.contains(&"integer_array".to_string()));
        assert!(keys.contains(&"boolean_array".to_string()));
        assert!(keys.contains(&"metadata__integer_array".to_string()));

        match &pload[&"name".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "John Doe".to_string());
            }
            _ => panic!(),
        }
        match &pload[&"age".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 43);
            }
            _ => panic!(),
        }
        match &pload[&"floating".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match &pload[&"boolean".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "true");
            }
            _ => panic!(),
        }
        match &pload[&"metadata__temperature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60.5);
            }
            _ => panic!(),
        }
        match &pload[&"metadata__width".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60);
            }
            _ => panic!(),
        }
        match &pload[&"metadata__height".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 50);
            }
            _ => panic!(),
        }
        match &pload[&"metadata__nested__feature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match &pload[&"string_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "hello");
                assert_eq!(x[1], "world");
            }
            _ => panic!(),
        }
        match &pload[&"integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match &pload[&"metadata__integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match &pload[&"float_array".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
            }
            _ => panic!(),
        }
        match &pload[&"boolean_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "true");
                assert_eq!(x[1], "false");
            }
            _ => panic!(),
        }
        match &pload[&"geo_data".to_string()] {
            PayloadType::Geo(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0].lat, 1.0);
                assert_eq!(x[0].lon, 1.0);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_invalid_serde_input() {
        let data = r#"
        {
            "array": [1, "hey"]
        }"#;

        let v = serde_json::from_str(data).unwrap();
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage =
            SimplePayloadStorage::open(dir.path(), Arc::new(SchemaStorage::new())).unwrap();
        storage.assign_all_with_value(100, v).unwrap();
    }
}
