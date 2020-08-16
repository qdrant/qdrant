use crate::payload_storage::payload_storage::{PayloadStorage};
use crate::types::{PayloadKeyType, PayloadType, PointOffsetType, TheMap, PointIdType};

use std::collections::{HashMap};

use crate::id_mapper::id_mapper::IdMapper;
use std::sync::Arc;
use atomic_refcell::AtomicRefCell;

pub struct SimplePayloadStorage {
    payload: HashMap<PointOffsetType, TheMap<PayloadKeyType, PayloadType>>,
    id_mapper: Arc<AtomicRefCell<dyn IdMapper>>,
}


impl SimplePayloadStorage {
    pub fn point_external_id(&self, internal_id: PointOffsetType) -> Option<PointIdType> {
        self.id_mapper.borrow().external_id(internal_id)
    }

    pub fn new(id_mapper: Arc<AtomicRefCell<dyn IdMapper>>) -> Self {
        SimplePayloadStorage {
            payload: Default::default(),
            id_mapper
        }
    }
}

impl PayloadStorage for SimplePayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType) {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => {
                point_payload.insert(key.to_owned(), payload);
            },
            None => {
                let mut new_payload = TheMap::default();
                new_payload.insert(key.to_owned(), payload);
                self.payload.insert(point_id, new_payload);
            }
        }
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
