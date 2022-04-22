use crate::types::{Payload, PointOffsetType};
use std::collections::HashMap;

/// Same as `SimplePayloadStorage` but without persistence
/// Warn: for tests only
#[derive(Default)]
pub struct InMemoryPayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
}

impl InMemoryPayloadStorage {
    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}
