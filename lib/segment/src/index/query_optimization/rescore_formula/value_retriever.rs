#![allow(dead_code)] // TODO: remove this
use std::collections::{HashMap, HashSet};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::{Number, Value};

use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::{DateTimePayloadType, PayloadContainer, UuidPayloadType};

type VariableRetrieverFn<'a> = Box<dyn Fn(PointOffsetType) -> Option<Value> + 'a>;

impl StructPayloadIndex {
    /// Prepares optimized functions to extract each of the variables, given a point id.
    pub(super) fn retrievers_map<'a, 'q>(
        &'a self,
        variables: HashSet<JsonPath>,
        hw_counter: &'q HardwareCounterCell,
    ) -> HashMap<JsonPath, VariableRetrieverFn<'q>>
    where
        'a: 'q,
    {
        let payload_provider = PayloadProvider::new(self.payload.clone());

        // prepare extraction of the variables from field indices or payload.
        let mut var_retrievers = HashMap::new();
        for key in variables {
            let payload_provider = payload_provider.clone();

            let retriever = variable_retriever(
                &self.field_indexes,
                &key,
                payload_provider.clone(),
                hw_counter,
            );

            var_retrievers.insert(key, retriever);
        }

        var_retrievers
    }
}

fn variable_retriever<'a, 'q>(
    indices: &'a HashMap<JsonPath, Vec<FieldIndex>>,
    json_path: &JsonPath,
    payload_provider: PayloadProvider,
    hw_counter: &'q HardwareCounterCell,
) -> VariableRetrieverFn<'q>
where
    'a: 'q,
{
    indices
        .get(json_path)
        .and_then(|indices| {
            indices
                .iter()
                .find_map(|index| indexed_variable_retriever(index))
        })
        // TODO(scoreboost): optimize by reusing the same payload for all variables?
        .unwrap_or_else(|| {
            // if the variable is not found in the index, try to find it in the payload
            let key = json_path.clone();
            payload_variable_retriever(payload_provider, key, hw_counter)
        })
}

fn payload_variable_retriever(
    payload_provider: PayloadProvider,
    json_path: JsonPath,
    hw_counter: &HardwareCounterCell,
) -> VariableRetrieverFn {
    let retriever_fn = move |point_id: PointOffsetType| {
        payload_provider.with_payload(
            point_id,
            |payload| {
                let values = payload.get_value(&json_path);
                let value = *values.first()?;
                Some(value.clone())
            },
            hw_counter,
        )
    };
    Box::new(retriever_fn)
}

/// Returns function to extract the first number a point may have from the index
///
/// If there is no appropriate index, returns None
fn indexed_variable_retriever(index: &FieldIndex) -> Option<VariableRetrieverFn> {
    match index {
        FieldIndex::IntIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                numeric_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| Value::Number(value.into()))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::IntMapIndex(map_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                map_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|&value| Value::Number(value.into()))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::FloatIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                numeric_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .and_then(Number::from_f64)
                    .map(Value::Number)
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::DatetimeIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                numeric_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .and_then(DateTimePayloadType::from_timestamp)
                    .and_then(|dt| serde_json::to_value(dt).ok())
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::KeywordIndex(keyword_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                keyword_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|s| Value::String(s.to_owned()))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::GeoIndex(geo_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                geo_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| serde_json::to_value(value).ok())
                    .flatten()
            };
            Some(Box::new(extract_fn))
        }

        FieldIndex::BoolIndex(bool_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                bool_index
                    .get_point_values(point_id)
                    .first()
                    .map(|&value| Value::Bool(value))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::UuidMapIndex(uuid_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                uuid_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| UuidPayloadType::from_u128(*value))
                    .map(|value| Value::String(value.to_string()))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::UuidIndex(uuid_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<Value> {
                uuid_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| UuidPayloadType::from_u128(value))
                    .map(|value| Value::String(value.to_string()))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::FullTextIndex(_) => None, // Better get it from the payload
    }
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use serde_json::{from_value, json};

    use crate::index::field_index::geo_index::GeoMapIndex;
    use crate::index::field_index::numeric_index::NumericIndex;
    use crate::index::field_index::{FieldIndex, FieldIndexBuilderTrait};
    use crate::index::query_optimization::payload_provider::PayloadProvider;
    use crate::index::query_optimization::rescore_formula::value_retriever::variable_retriever;
    use crate::payload_storage::in_memory_payload_storage::InMemoryPayloadStorage;
    use crate::payload_storage::payload_storage_enum::PayloadStorageEnum;
    use crate::types::Payload;

    pub fn fixture_payload_provider() -> PayloadProvider {
        // Create an in-memory payload storage and populate it with some payload maps containing numbers and geo points.
        let mut in_memory_storage = InMemoryPayloadStorage::default();

        // For point id 0: a payload with a numeric value.
        let payload1: Payload = from_value(json!({
            "value": 42
        }))
        .unwrap();

        // For point id 1: a payload with a geo point.
        let payload2: Payload = from_value(json!({
            "location": {
                "lat": 10.0,
                "lon": 20.0
            }
        }))
        .unwrap();

        // For point id 2: a payload containing both a number and a geo point.
        let payload3: Payload = from_value(json!({
            "value": [99, 55],
            "location": {
                "lat": 15.5,
                "lon": 25.5
            }
        }))
        .unwrap();

        // Insert the payloads into the in-memory storage.
        in_memory_storage.payload.insert(0, payload1);
        in_memory_storage.payload.insert(1, payload2);
        in_memory_storage.payload.insert(2, payload3);

        // Wrap the in-memory storage in a PayloadStorageEnum.
        let storage_enum = PayloadStorageEnum::InMemoryPayloadStorage(in_memory_storage);

        let arc_storage = Arc::new(AtomicRefCell::new(storage_enum));
        PayloadProvider::new(arc_storage)
    }

    #[test]
    fn test_variable_retriever_from_payload() {
        let payload_provider = fixture_payload_provider();

        let no_indices = Default::default();

        let hw_counter = Default::default();

        // Test retrieving a number from the payload.
        let retriever = variable_retriever(
            &no_indices,
            &"value".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, Some(json!(42))),
                1 => assert_eq!(value, None),
                2 => assert_eq!(value, Some(json!(99.0))),
                _ => unreachable!(),
            }
        }

        // Test retrieving a geo point from the payload.
        let retriever = variable_retriever(
            &no_indices,
            &"location".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, None),
                1 => assert_eq!(value, Some(json!({ "lat": 10.0, "lon": 20.0 }))),
                2 => assert_eq!(value, Some(json!({ "lat": 15.5, "lon": 25.5 }))),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_variable_retriever_from_index() {
        // Empty payload provider.
        let payload_provider = PayloadProvider::new(Arc::new(AtomicRefCell::new(
            PayloadStorageEnum::InMemoryPayloadStorage(InMemoryPayloadStorage::default()),
        )));

        // Create a field index for a number.
        let dir = tempfile::tempdir().unwrap();
        let mut builder = NumericIndex::builder_mmap(dir.path());
        builder.add_point(0, &[&42.into()]).unwrap();
        builder.add_point(1, &[]).unwrap();
        builder.add_point(2, &[&99.into(), &55.into()]).unwrap();
        let numeric_index = builder.finalize().unwrap();
        let numeric_index = FieldIndex::IntIndex(numeric_index);

        // Create a field index for a geo point.
        let dir = tempfile::tempdir().unwrap();
        let mut builder = GeoMapIndex::mmap_builder(dir.path());

        builder.add_point(0, &[]).unwrap();
        builder
            .add_point(1, &[&json!({ "lat": 10.0, "lon": 20.0})])
            .unwrap();
        builder
            .add_point(2, &[&json!({"lat": 15.5, "lon": 25.5})])
            .unwrap();
        let geo_index = builder.finalize().unwrap();
        let geo_index = FieldIndex::GeoIndex(geo_index);

        let mut indices = HashMap::new();
        indices.insert("value".try_into().unwrap(), vec![numeric_index]);
        indices.insert("location".try_into().unwrap(), vec![geo_index]);

        let hw_counter = Default::default();

        // Test retrieving a number from the index.
        let retriever = variable_retriever(
            &indices,
            &"value".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, Some(json!(42))),
                1 => assert_eq!(value, None),
                2 => assert_eq!(value, Some(json!(99))),
                _ => unreachable!(),
            }
        }

        // Test retrieving a geo point from the index.
        let retriever = variable_retriever(
            &indices,
            &"location".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, None),
                1 => assert_eq!(value, Some(json!({ "lat": 10.0, "lon": 20.0 }))),
                2 => assert_eq!(value, Some(json!({ "lat": 15.5, "lon": 25.5 }))),
                _ => unreachable!(),
            }
        }
    }
}
