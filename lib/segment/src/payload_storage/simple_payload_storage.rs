use crate::payload_storage::payload_storage::{PayloadStorage};
use crate::types::{PayloadKeyType, PayloadType, PointOffsetType, TheMap};

use std::collections::{HashMap};

use crate::entry::entry_point::OperationResult;
use sled::Db;
use std::path::Path;

pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>>,
    store: Db,
}


impl SimplePayloadStorage {
    pub fn open(path: &Path) -> Self {
        let store = sled::open(path).unwrap();

        let mut payload_map: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>> = Default::default();

        for record in store.iter() {
            let (key, val) = record.unwrap();

            let point_id: PointOffsetType = serde_cbor::from_slice(&key).unwrap();
            let payload: TheMap<PayloadKeyType, PayloadType> = serde_cbor::from_slice(&val).unwrap();
            payload_map.insert(point_id, payload);
        }

        SimplePayloadStorage {
            payload: payload_map,
            store,
        }
    }

    fn update_storage(&self, point_id: &PointOffsetType) -> OperationResult<()> {
        match self.payload.get(point_id) {
            None => self.store.remove(serde_cbor::to_vec(&point_id).unwrap())?,
            Some(payload) => self.store.insert(
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
        self.store.clear()?;
        Ok(())
    }

    fn flush(&self) -> OperationResult<usize> {
        Ok(self.store.flush()?)
    }
}
