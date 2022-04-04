use crate::types::{Filter, Payload, PayloadKeyTypeRef, PointOffsetType};
use atomic_refcell::AtomicRefCell;
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
    payload: HashMap<PointOffsetType, Payload>,
}

impl InMemoryPayloadStorage {
    pub fn payload_ptr(&self, point_id: PointOffsetType) -> Option<&Payload> {
        self.payload.get(&point_id)
    }
}

impl PayloadStorage for InMemoryPayloadStorage {
    fn assign(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge(payload),
            None => {
                self.payload.insert(point_id, payload.to_owned());
            }
        }
        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> Payload {
        match self.payload.get(&point_id) {
            Some(payload) => payload.to_owned(),
            None => Default::default(),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Option<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                Ok(res)
            }
            None => Ok(None),
        }
    }

    fn drop(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
        let res = self.payload.remove(&point_id);
        Ok(res)
    }

    fn wipe(&mut self) -> OperationResult<()> {
        self.payload = HashMap::new();
        Ok(())
    }

    fn flush(&self) -> OperationResult<()> {
        Ok(())
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(self.payload.keys().copied())
    }
}

pub struct InMemoryConditionChecker {
    payload_storage: Arc<AtomicRefCell<InMemoryPayloadStorage>>,
    id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
}

impl InMemoryConditionChecker {
    pub fn new(
        payload_storage: Arc<AtomicRefCell<InMemoryPayloadStorage>>,
        id_tracker: Arc<AtomicRefCell<IdTrackerSS>>,
    ) -> Self {
        InMemoryConditionChecker {
            payload_storage,
            id_tracker,
        }
    }
}

impl ConditionChecker for InMemoryConditionChecker {
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool {
        let empty_payload: Payload = Default::default();

        let payload_storage_guard = self.payload_storage.borrow();
        let payload_ptr = payload_storage_guard.payload_ptr(point_id);

        let payload = match payload_ptr {
            None => &empty_payload,
            Some(x) => x,
        };

        check_payload(payload, self.id_tracker.borrow().deref(), query, point_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wipe() {
        let mut storage = InMemoryPayloadStorage::default();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        assert!(!storage.payload(100).is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100), Default::default());
    }

    #[test]
    fn test_assign_payload_from_serde_json() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "boolean": "true",
            "floating": 30.5,
            "string_array": ["hello", "world"],
            "boolean_array": ["true", "false"],
            "float_array": [1.0, 2.0],
            "integer_array": [1, 2],
            "geo_data": {"type": "geo", "value": {"lon": 1.0, "lat": 1.0}},
            "metadata": {
                "height": 50,
                "width": 60,
                "temperature": 60.5,
                "nested": {
                    "feature": 30.5
                },
                "integer_array": [1, 2]
            }
        }"#;

        let payload: Payload = serde_json::from_str(data).unwrap();
        let mut storage = InMemoryPayloadStorage::default();
        storage.assign(100, &payload).unwrap();
        let pload = storage.payload(100);
        assert_eq!(pload, payload);
    }
}
