use std::collections::HashMap;
use std::path::Path;
use crate::types::{PayloadKeyType, PayloadType, PointOffsetType, TheMap, PayloadSchemaType};

use rocksdb::{DB, IteratorMode, Options};

use crate::entry::entry_point::{OperationResult, OperationError};
use crate::payload_storage::payload_storage::PayloadStorage;

/// Since sled is used for reading only during the initialization, large read cache is not required
const DB_CACHE_SIZE: usize = 10 * 1024 * 1024;
// 10 mb
const DB_NAME: &'static str = "payload";


pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>>,
    schema: TheMap<PayloadKeyType, PayloadSchemaType>,
    store: DB,
}


impl SimplePayloadStorage {
    pub fn open(path: &Path) -> OperationResult<Self> {
        let mut options: Options = Options::default();
        options.set_write_buffer_size(DB_CACHE_SIZE);
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let store = DB::open_cf(&options, path, vec![DB_NAME])?;

        let mut payload_map: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>> = Default::default();
        let mut schema: TheMap<PayloadKeyType, PayloadSchemaType> = Default::default();

        let cf_handle = store.cf_handle(DB_NAME).unwrap();
        for (key, val) in store.iterator_cf(cf_handle, IteratorMode::Start) {
            let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
            let payload: TheMap<PayloadKeyType, PayloadType> = serde_cbor::from_slice(&val).unwrap();
            SimplePayloadStorage::update_schema(&mut schema, &payload).unwrap();
            payload_map.insert(point_id, payload);
        }

        Ok(SimplePayloadStorage {
            payload: payload_map,
            schema,
            store,
        })
    }

    fn update_schema_value(
        schema: &mut TheMap<PayloadKeyType, PayloadSchemaType>,
        key: &PayloadKeyType,
        value: &PayloadType
    ) -> OperationResult<()> {
        return match schema.get(key) {
            None => { schema.insert(key.to_owned(), value.into()); Ok(()) },
            Some(schema_type) => if schema_type != &value.into() {
                Err(OperationError::TypeError {
                    field_name: key.to_owned(),
                    expected_type: format!("{:?}", schema_type)
                })
            } else {
                Ok(())
            }
        }
    }

    fn update_schema(
        schema: &mut TheMap<PayloadKeyType, PayloadSchemaType>,
        payload: &TheMap<PayloadKeyType, PayloadType>) -> OperationResult<()> {
        for (key, value) in payload.iter() {
            SimplePayloadStorage::update_schema_value(schema, key, value)?;
        }
        Ok(())
    }

    fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        let cf_handle = self.store.cf_handle(DB_NAME).unwrap();
        match self.payload.get(point_id) {
            None => self.store.delete_cf(cf_handle, serde_cbor::to_vec(&point_id).unwrap())?,
            Some(payload) => self.store.put_cf(
                cf_handle,
                serde_cbor::to_vec(&point_id).unwrap(),
                serde_cbor::to_vec(payload).unwrap(),
            )?,
        };
        Ok(())
    }

    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&TheMap<PayloadKeyType, PayloadType>> {
        self.payload.get(&point_id)
    }
}

impl PayloadStorage for SimplePayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType) -> OperationResult<()> {
        SimplePayloadStorage::update_schema_value(&mut self.schema, key, &payload)?;
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => {
                point_payload.insert(key.to_owned(), payload);
            }
            None => {
                let mut new_payload = TheMap::default();
                new_payload.insert(key.to_owned(), payload);
                self.payload.insert(point_id, new_payload);
            }
        }
        self.update_storage(&point_id)?;
        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType> {
        match self.payload.get(&point_id) {
            Some(payload) => payload.clone(),
            None => TheMap::new()
        }
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType) -> OperationResult<Option<PayloadType>> {
        let point_payload = self.payload.get_mut(&point_id).unwrap();
        let res = point_payload.remove(key);
        self.update_storage(&point_id)?;
        Ok(res)
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<TheMap<PayloadKeyType, PayloadType>>> {
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
        self.schema = TheMap::new();
        Ok(())
    }

    fn flush(&self) -> OperationResult<()> {
        let cf_handle = self.store.cf_handle(DB_NAME).unwrap();
        Ok(self.store.flush_cf(cf_handle)?)
    }

    fn schema(&self) -> TheMap<PayloadKeyType, PayloadSchemaType> {
        return self.schema.clone()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        return Box::new(self.payload.keys().cloned())
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
        let payload = PayloadType::Integer(vec![1, 2, 3]);
        let key = "key".to_owned();
        storage.assign(100, &key, payload.clone()).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &key, payload.clone()).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &key, payload.clone()).unwrap();
        assert!(storage.payload(100).len() > 0);
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100).len(), 0);
    }
}