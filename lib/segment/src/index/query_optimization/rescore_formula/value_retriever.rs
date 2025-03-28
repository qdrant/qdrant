#![allow(dead_code)] // TODO: remove this
use std::collections::{HashMap, HashSet};

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::{Number, Value};

use crate::common::utils::MultiValue;
use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::{DateTimePayloadType, PayloadContainer, UuidPayloadType};

pub type VariableRetrieverFn<'a> = Box<dyn Fn(PointOffsetType) -> MultiValue<Value> + 'a>;

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
        .and_then(|indices| indices.iter().find_map(indexed_variable_retriever))
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
                let values = payload.get_value_cloned(&json_path);

                if json_path.has_wildcard_suffix() {
                    return values;
                }

                // Not using array wildcard `[]` on a key which has an array value will return the whole
                // array as one value, let's flatten the array if that is the case.
                //
                // This is the same thing we do for indexing payload values
                let mut multi_value = MultiValue::new();
                for value in values {
                    if let Value::Array(array) = value {
                        multi_value.extend(array);
                    } else {
                        multi_value.push(value);
                    }
                }
                multi_value
            },
            hw_counter,
        )
    };
    Box::new(retriever_fn)
}

/// Returns function to extract all the values a point has in the index
///
/// If there is no appropriate index, returns None
fn indexed_variable_retriever(index: &FieldIndex) -> Option<VariableRetrieverFn> {
    match index {
        FieldIndex::IntIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                numeric_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .map(|v| Value::Number(Number::from(v)))
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::IntMapIndex(map_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                map_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .map(|v| Value::Number(Number::from(*v)))
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::FloatIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                numeric_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .filter_map(|v| Some(Value::Number(Number::from_f64(v)?)))
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::DatetimeIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                numeric_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .filter_map(|v| {
                        serde_json::to_value(DateTimePayloadType::from_timestamp(v)?).ok()
                    })
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::KeywordIndex(keyword_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                keyword_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .filter_map(|v| serde_json::to_value(v).ok())
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::GeoIndex(geo_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                geo_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .filter_map(|v| serde_json::to_value(v).ok())
                    .collect()
            };
            Some(Box::new(extract_fn))
        }

        FieldIndex::BoolIndex(bool_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                bool_index
                    .get_point_values(point_id)
                    .into_iter()
                    .map(Value::Bool)
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::UuidMapIndex(uuid_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                uuid_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .map(|value| Value::String(UuidPayloadType::from_u128(*value).to_string()))
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::UuidIndex(uuid_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> MultiValue<Value> {
                uuid_index
                    .get_values(point_id)
                    .into_iter()
                    .flatten()
                    .map(|value| Value::String(UuidPayloadType::from_u128(value).to_string()))
                    .collect()
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::FullTextIndex(_) => None, // Better get it from the payload
        FieldIndex::NullIndex(_) => None,     // There should be other index for the same field
    }
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use common::counter::hardware_counter::HardwareCounterCell;
    use serde_json::{Value, from_value, json};

    use crate::common::utils::MultiValue;
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
        let payload0: Payload = from_value(json!({
            "value": 42
        }))
        .unwrap();

        // For point id 1: a payload with a geo point.
        let payload1: Payload = from_value(json!({
            "location": {
                "lat": 10.0,
                "lon": 20.0
            }
        }))
        .unwrap();

        // For point id 2: a payload containing both a number and a geo point.
        let payload2: Payload = from_value(json!({
            "value": [99, 55],
            "location": {
                "lat": 15.5,
                "lon": 25.5
            }
        }))
        .unwrap();

        // For point id 3: a payload with an array of 1 number, and an array of 1 geo point.
        let payload3: Payload = from_value(json!({
            "value": [42.5],
            "location": [{
                "lat": 16.5,
                "lon": 26.5
            }]
        }))
        .unwrap();

        // Insert the payloads into the in-memory storage.
        in_memory_storage.payload.insert(0, payload0);
        in_memory_storage.payload.insert(1, payload1);
        in_memory_storage.payload.insert(2, payload2);
        in_memory_storage.payload.insert(3, payload3);

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
        for id in 0..=3 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, [json!(42)].into()),
                1 => assert_eq!(value, MultiValue::<Value>::new()),
                2 => assert_eq!(value, [json!(99), json!(55)].into()),
                3 => assert_eq!(value, [json!(42.5)].into()),
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
        for id in 0..=3 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, MultiValue::<Value>::new()),
                1 => assert_eq!(value, [json!({ "lat": 10.0, "lon": 20.0 })].into()),
                2 => assert_eq!(value, [json!({ "lat": 15.5, "lon": 25.5 })].into()),
                3 => assert_eq!(value, [json!({ "lat": 16.5, "lon": 26.5 })].into()),
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
        let hw_counter = HardwareCounterCell::new();

        // Create a field index for a number.
        let dir = tempfile::tempdir().unwrap();
        let mut builder = NumericIndex::builder_mmap(dir.path(), false);
        builder.add_point(0, &[&42.into()], &hw_counter).unwrap();
        builder.add_point(1, &[], &hw_counter).unwrap();
        builder
            .add_point(2, &[&99.into(), &55.into()], &hw_counter)
            .unwrap();
        let numeric_index = builder.finalize().unwrap();
        let numeric_index = FieldIndex::IntIndex(numeric_index);

        // Create a field index for a geo point.
        let dir = tempfile::tempdir().unwrap();
        let mut builder = GeoMapIndex::mmap_builder(dir.path(), false);

        builder.add_point(0, &[], &hw_counter).unwrap();
        builder
            .add_point(1, &[&json!({ "lat": 10.0, "lon": 20.0})], &hw_counter)
            .unwrap();
        builder
            .add_point(2, &[&json!({"lat": 15.5, "lon": 25.5})], &hw_counter)
            .unwrap();
        let geo_index = builder.finalize().unwrap();
        let geo_index = FieldIndex::GeoIndex(geo_index);

        // Create a field index for datetime
        let dir = tempfile::tempdir().unwrap();
        let mut builder = NumericIndex::builder_mmap(dir.path(), false);

        builder
            .add_point(0, &[&json!("2023-01-01T00:00:00Z")], &hw_counter)
            .unwrap();
        builder
            .add_point(
                1,
                &[&json!("2023-01-02"), &json!("2023-01-03T00:00:00Z")],
                &hw_counter,
            )
            .unwrap();
        builder.add_point(2, &[], &hw_counter).unwrap();
        let datetime_index = builder.finalize().unwrap();
        let datetime_index = FieldIndex::DatetimeIndex(datetime_index);

        let mut indices = HashMap::new();
        indices.insert("value".try_into().unwrap(), vec![numeric_index]);
        indices.insert("location".try_into().unwrap(), vec![geo_index]);
        indices.insert("creation".try_into().unwrap(), vec![datetime_index]);

        let hw_counter = Default::default();

        // Test retrieving a number from the index.
        let retriever = variable_retriever(
            &indices,
            &"value".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..=2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, [json!(42)].into()),
                1 => assert_eq!(value, MultiValue::<Value>::new()),
                2 => assert_eq!(value, [json!(99), json!(55)].into()),
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
        for id in 0..=2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, MultiValue::<Value>::new()),
                1 => assert_eq!(value, [json!({ "lat": 10.0, "lon": 20.0 })].into()),
                2 => assert_eq!(value, [json!({ "lat": 15.5, "lon": 25.5 })].into()),
                _ => unreachable!(),
            }
        }

        // Test retrieving a datetime from the index.
        let retriever = variable_retriever(
            &indices,
            &"creation".try_into().unwrap(),
            payload_provider.clone(),
            &hw_counter,
        );
        for id in 0..=2 {
            let value = retriever(id);
            match id {
                0 => assert_eq!(value, [json!("2023-01-01T00:00:00Z")].into()),
                1 => assert_eq!(
                    value,
                    [json!("2023-01-02T00:00:00Z"), json!("2023-01-03T00:00:00Z")].into()
                ),
                2 => assert_eq!(value, MultiValue::<Value>::new()),
                _ => unreachable!(),
            }
        }
    }
}
