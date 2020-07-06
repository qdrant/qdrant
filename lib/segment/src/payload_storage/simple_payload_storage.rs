use crate::payload_storage::payload_storage::{PayloadStorage, TheMap, DeletedFlagStorage};
use crate::types::{PayloadKeyType, PayloadType, PointOffsetType};
use std::mem;
use std::collections::{HashMap, HashSet};

pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>>,
    deleted: HashSet<PointOffsetType>,
}


impl SimplePayloadStorage {
    pub fn new() -> Self {
        SimplePayloadStorage {
            payload: Default::default(),
            deleted: Default::default(),
        }
    }
}

impl PayloadStorage for SimplePayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType) {
        let point_payload = self.payload.get_mut(&point_id).unwrap();
        point_payload.insert(key.to_owned(), payload);
    }

    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType> {
        match self.payload.get(&point_id) {
            Some(payload) => payload.clone(),
            None => TheMap::new()
        }
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType) -> Option<PayloadType> {
        let point_payload = self.payload.get_mut(&point_id).unwrap();
        point_payload.remove(key)
    }

    fn drop(&mut self, point_id: PointOffsetType) -> Option<TheMap<PayloadKeyType, PayloadType>> {
        self.payload.remove(&point_id)
    }

    fn wipe(&mut self) {
        self.payload = HashMap::new()
    }
}

impl DeletedFlagStorage for SimplePayloadStorage {
    fn mark_deleted(&mut self, point_id: PointOffsetType) {
        self.deleted.insert(point_id);
    }

    fn is_deleted(&self, point_id: PointOffsetType) -> bool {
        self.deleted.contains(&point_id)
    }
}