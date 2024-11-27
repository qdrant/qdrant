use std::collections::HashMap;
use std::path::PathBuf;

use common::types::PointOffsetType;
use serde_json::Value;

use crate::common::operation_error::OperationResult;
use crate::common::Flusher;
use crate::json_path::JsonPath;
use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
use crate::payload_storage::PayloadStorage;
use crate::types::Payload;

impl PayloadStorage for InMemoryPayloadStorage {
    fn overwrite(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        self.payload.insert(point_id, payload.to_owned());
        Ok(())
    }

    fn set(&mut self, point_id: PointOffsetType, payload: &Payload) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge(payload),
            None => {
                self.payload.insert(point_id, payload.to_owned());
            }
        }
        Ok(())
    }

    fn set_by_key(
        &mut self,
        point_id: PointOffsetType,
        payload: &Payload,
        key: &JsonPath,
    ) -> OperationResult<()> {
        match self.payload.get_mut(&point_id) {
            Some(point_payload) => point_payload.merge_by_key(payload, key),
            None => {
                let mut dest_payload = Payload::default();
                dest_payload.merge_by_key(payload, key);
                self.payload.insert(point_id, dest_payload);
            }
        }
        Ok(())
    }

    fn get(&self, point_id: PointOffsetType) -> OperationResult<Payload> {
        match self.payload.get(&point_id) {
            Some(payload) => Ok(payload.to_owned()),
            None => Ok(Default::default()),
        }
    }

    fn delete(&mut self, point_id: PointOffsetType, key: &JsonPath) -> OperationResult<Vec<Value>> {
        match self.payload.get_mut(&point_id) {
            Some(payload) => {
                let res = payload.remove(key);
                Ok(res)
            }
            None => Ok(vec![]),
        }
    }

    fn clear(&mut self, point_id: PointOffsetType) -> OperationResult<Option<Payload>> {
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

    fn iter<F>(&self, mut callback: F) -> OperationResult<()>
    where
        F: FnMut(PointOffsetType, &Payload) -> OperationResult<bool>,
    {
        for (key, val) in self.payload.iter() {
            let do_continue = callback(*key, val)?;
            if !do_continue {
                return Ok(());
            }
        }
        Ok(())
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![]
    }

    fn get_storage_size_bytes(&self) -> OperationResult<usize> {
        let mut estimated_size = 0;
        for (_p_id, val) in self.payload.iter() {
            // account for point_id
            estimated_size += size_of::<PointOffsetType>();
            for (key, val) in val.0.iter() {
                // account for key and value
                estimated_size += key.len() + serde_json::to_string(val).unwrap().len()
            }
        }
        Ok(estimated_size)
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
                Condition::Field(FieldCondition::new_match(JsonPath::new("age"), 43.into())),
                Condition::Field(FieldCondition::new_match(
                    JsonPath::new("name"),
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
            &HashMap::new(),
            &query,
            0,
            &IndexesMap::new(),
        );
    }

    #[test]
    fn test_wipe() {
        let mut storage = InMemoryPayloadStorage::default();
        let payload: Payload = serde_json::from_str(r#"{"name": "John Doe"}"#).unwrap();
        storage.set(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.set(100, &payload).unwrap();
        storage.wipe().unwrap();
        storage.set(100, &payload).unwrap();
        assert!(!storage.get(100).unwrap().is_empty());
        storage.wipe().unwrap();
        assert_eq!(storage.get(100).unwrap(), Default::default());
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
        storage.set(100, &payload).unwrap();
        let pload = storage.get(100).unwrap();
        assert_eq!(pload, payload);
    }
}
