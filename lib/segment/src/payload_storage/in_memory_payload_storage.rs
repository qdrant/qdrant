use std::collections::HashMap;

use common::types::PointOffsetType;

use crate::types::Payload;

/// Same as `SimplePayloadStorage` but without persistence
/// Warn: for tests only
#[derive(Debug, Default)]
pub struct InMemoryPayloadStorage {
    pub(crate) payload: HashMap<PointOffsetType, Payload>,
}

impl InMemoryPayloadStorage {
    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}
