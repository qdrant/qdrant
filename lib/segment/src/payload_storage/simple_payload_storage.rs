use crate::payload_storage::payload_storage::{PayloadStorage, TheMap, DeletedFlagStorage};
use crate::types::{Filter, PayloadKeyType, PayloadType, PointOffsetType};

pub struct SimplePayloadStorage {
    payload: Vec<TheMap<PayloadKeyType, PayloadType>>,
    deleted: Vec<bool>,

}

impl SimplePayloadStorage {
    fn rescale_storage(&mut self, point_id: PointOffsetType) {
        if self.payload.len() >= point_id as usize {
            self.payload.resize_with((point_id + 1) as usize, TheMap::new)
        }

        if self.deleted.len() >= point_id as usize {
            self.deleted.resize((point_id + 1) as usize, false);
        }
    }
}

impl PayloadStorage for SimplePayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, key: &PayloadKeyType, payload: PayloadType) {
        self.rescale_storage(point_id);
        let mut point_payload = self.payload.get_mut(point_id).unwrap();
        point_payload.insert(key.to_owned(), payload);
    }

    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType> {
        if self.payload.len() <= point_id {
            return TheMap::new();
        }
        return self.payload[point_id].clone();
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &PayloadKeyType) {
        self.rescale_storage(point_id);
        let mut point_payload = self.payload.get_mut(point_id).unwrap();
        point_payload.remove(key);
    }

    fn drop(&mut self, point_id: PointOffsetType) {
        self.rescale_storage(point_id);
        self.payload[point_id] = TheMap::new()
    }
}

impl DeletedFlagStorage for SimplePayloadStorage {
    fn mark_deleted(&mut self, point_id: PointOffsetType) {
        self.rescale_storage(point_id);
        self.deleted[point_id] = true;
    }

    fn is_deleted(&self, point_id: PointOffsetType) -> bool {
        if self.deleted.len() <= point_id {
            return false;
        }
        return self.deleted[point_id];
    }
}