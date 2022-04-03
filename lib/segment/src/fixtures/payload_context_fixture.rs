use std::path::Path;
use atomic_refcell::AtomicRefCell;
use crate::id_tracker::points_iterator::PointsIterator;
use crate::index::plain_payload_index::PlainPayloadIndex;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::{PayloadStorage, PayloadStorageSS};
use crate::payload_storage::simple_payload_storage::SimplePayloadStorage;
use crate::types::PointOffsetType;

pub struct IdsIterator {
    ids: Vec<PointOffsetType>
}

impl PointsIterator for IdsIterator {
    fn points_count(&self) -> usize {
        self.ids.len()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        Box::new(self.ids.iter().copied())
    }

    fn max_id(&self) -> PointOffsetType {
        self.ids.last().copied().unwrap()
    }
}


pub fn create_payload_storage(num_points: usize) -> InMemoryPayloadStorage {
    let mut payload_storage = InMemoryPayloadStorage::new();

    for id in 0..num_points {
        let payload = ();
        payload_storage.assign(id as PointOffsetType);
    }

    payload_storage
}

pub fn create_plain_payload_index(path: &Path) -> PlainPayloadIndex {

}