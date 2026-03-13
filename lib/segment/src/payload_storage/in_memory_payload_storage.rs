use common::types::PointOffsetType;
use dashmap::DashMap;

use crate::types::Payload;

/// Same as `SimplePayloadStorage` but without persistence
/// Warn: for tests only
#[derive(Debug, Default)]
pub struct InMemoryPayloadStorage {
    pub(crate) payload: DashMap<PointOffsetType, Payload>,
}

impl InMemoryPayloadStorage {
    pub fn payload_ptr(
        &self,
        point_id: PointOffsetType,
    ) -> Option<dashmap::mapref::one::Ref<'_, u32, Payload>> {
        self.payload.get(&point_id)
    }
}
