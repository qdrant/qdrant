use std::collections::HashMap;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::{Payload, PayloadKeyTypeRef};

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

    fn assign_by_key(&mut self, point_id: PointOffsetType, payload: &Payload, key: &str) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge_by_key(payload, key)?,
            None => {
                self.payload.insert(point_id, payload.to_owned());
            }
        }
        Ok(())
    }

    fn payload(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self.payload.get(&point_id) {
            Some(payload) => Ok(payload.to_owned()),
            None => Ok(Default::default()),
        }
    }

    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: PayloadKeyTypeRef,
    ) -> OperationResult<Vec<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                Ok(res)
            }
            None => Ok(vec![]),
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

    fn flusher(&self) -> Flusher {
        Box::new(|| Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use serde_json::json;

    use super::*;
    use crate::common::utils::IndexesMap;
    use crate::fixtures::payload_context_fixture::FixtureIdTracker;
    use crate::payload_storage::query_checker::check_payload;
    use crate::types::{Condition, FieldCondition, Filter, OwnedPayloadRef};

    #[test]
    fn test_condition_checking() {
        let id_tracker = FixtureIdTracker::new(1);
        let get_payload = || {
            let payload: Payload = serde_json::from_value(json!({
                "name": "John Doe",
                "age": 43,
                "boolean": "true",
                "floating": 30.5,
                "string_array": ["hello", "world"],
                "boolean_array": ["true", "false"],
                "float_array": [1.0, 2.0],
            }))
            .unwrap();
            eprintln!("assigning payload"); // should occur only once
            payload
        };

        let query = Filter {
            should: None,
            min_should: None,
            must: Some(vec![
                Condition::Field(FieldCondition::new_match("age".to_string(), 43.into())),
                Condition::Field(FieldCondition::new_match(
                    "name".to_string(),
                    "John Doe".to_string().into(),
                )),
            ]),
            must_not: None,
        };

        // Example:
        // How to check for payload in case if Payload is stored on disk
        // and it is preferred to only load the Payload once and if it is strictly required.

        let payload: RefCell<Option<OwnedPayloadRef>> = RefCell::new(None);
        check_payload(
            Box::new(|| {
                eprintln!("request payload");
                if payload.borrow().is_none() {
                    payload.replace(Some(get_payload().into()));
                }
                payload.borrow().as_ref().cloned().unwrap()
            }),
            Some(&id_tracker),
            &query,
            0,
            &IndexesMap::new(),
        );
    }

    #[test]
    fn test_wipe() {
        let mut storage = InMemoryPayloadStorage::default();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.assign(100, &payload).unwrap();
        assert!(!storage.payload(100).unwrap().is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.payload(100).unwrap(), Default::default());
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
        let pload = storage.payload(100).unwrap();
        assert_eq!(pload, payload);
    }
}
