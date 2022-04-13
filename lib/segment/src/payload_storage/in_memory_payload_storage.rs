use crate::types::{Filter, Payload, PayloadKeyTypeRef, PointOffsetType};
use atomic_refcell::AtomicRefCell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use serde_json::Value;

use crate::entry::entry_point::OperationResult;
use crate::id_tracker::IdTrackerSS;
use crate::payload_storage::query_checker::check_payload;
use crate::payload_storage::{ConditionChecker, PayloadStorage};

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
