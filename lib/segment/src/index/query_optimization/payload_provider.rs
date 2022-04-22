use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
use crate::types::{Payload, PointOffsetType};
use atomic_refcell::AtomicRefCell;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub struct PayloadProvider {
    payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>,
    empty_payload: Payload,
}

impl PayloadProvider {
    pub fn new(payload_storage: Arc<AtomicRefCell<PayloadStorageEnum>>) -> Self {
        Self {
            payload_storage,
            empty_payload: Default::default(),
        }
    }

    pub fn with_payload<F, G>(&self, point_id: PointOffsetType, callback: F) -> G
    where
        F: FnOnce(&Payload) -> G,
    {
        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr_opt = match payload_storage_guard.deref() {
            PayloadStorageEnum::InMemoryPayloadStorage(s) => s.payload_ptr(point_id),
            PayloadStorageEnum::SimplePayloadStorage(s) => s.payload_ptr(point_id),
        };
        let payload = if let Some(payload_ptr) = payload_ptr_opt {
            payload_ptr
        } else {
            &self.empty_payload
        };

        callback(payload)
    }
}
