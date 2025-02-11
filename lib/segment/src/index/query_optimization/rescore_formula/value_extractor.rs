#![allow(dead_code)] // TODO: remove this
use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;

use crate::index::field_index::FieldIndex;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;
use crate::types::PayloadContainer;

#[derive(Clone, Copy)]
pub(super) enum VariableKind {
    Number,
    GeoPoint,
}

#[derive(Debug, PartialEq)]
pub(super) enum VariableValue {
    Number(f64),
    GeoPoint { lat: f64, lon: f64 },
}

type ExtractVariableFn<'a> = Box<dyn Fn(PointOffsetType) -> Option<VariableValue> + 'a>;

impl StructPayloadIndex {
    /// Prepares optimized functions to extract each of the variables, given a point id.
    pub(super) fn extractors_map(
        &self,
        variables: Vec<(JsonPath, VariableKind)>,
    ) -> HashMap<JsonPath, ExtractVariableFn> {
        let payload_provider = PayloadProvider::new(self.payload.clone());

        // prepare extraction of the variables from field indices or payload.
        let mut var_extractors = HashMap::new();
        for (key, var_kind) in variables {
            let payload_provider = payload_provider.clone();
            let extractor = variable_extractor(
                &self.field_indexes,
                &key,
                var_kind,
                payload_provider.clone(),
            );
            var_extractors.insert(key, extractor);
        }

        var_extractors
    }
}

fn variable_extractor<'index>(
    indices: &'index HashMap<JsonPath, Vec<FieldIndex>>,
    json_path: &JsonPath,
    var_kind: VariableKind,
    payload_provider: PayloadProvider,
) -> ExtractVariableFn<'index> {
    indices
        .get(&json_path)
        .and_then(|indices| {
            indices
                .iter()
                .find_map(|index| indexed_variable_extractor(index, var_kind))
        })
        // TODO(scoreboost): optimize by reusing the same payload for all variables?
        .unwrap_or_else(|| {
            // if the variable is not found in the index, try to find it in the payload
            let key = json_path.clone();
            let extractor_fn = move |point_id: PointOffsetType| {
                payload_provider.with_payload(
                    point_id,
                    |payload| {
                        let values = payload.get_value(&key);
                        let value = values.first()?;
                        let var_value = match var_kind {
                            VariableKind::Number => VariableValue::Number(value.as_f64()?),
                            VariableKind::GeoPoint => {
                                let lat = value.get("lat")?.as_f64()?;
                                let lon = value.get("lon")?.as_f64()?;
                                VariableValue::GeoPoint { lat, lon }
                            }
                        };
                        Some(var_value)
                    },
                    &HardwareCounterCell::new(),
                )
            };
            Box::new(extractor_fn)
        })
}

fn indexed_variable_extractor(
    index: &FieldIndex,
    var_kind: VariableKind,
) -> Option<ExtractVariableFn> {
    match var_kind {
        VariableKind::Number => indexed_number_extractor(index),
        VariableKind::GeoPoint => geo_point_extractor(index),
    }
}

/// Returns function to extract the first number a point may have from the index
///
/// If there is no appropriate index, returns None
fn indexed_number_extractor(index: &FieldIndex) -> Option<ExtractVariableFn> {
    match index {
        FieldIndex::IntIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<VariableValue> {
                numeric_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| VariableValue::Number(value as f64))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::IntMapIndex(map_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<VariableValue> {
                map_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|&value| VariableValue::Number(value as f64))
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::FloatIndex(numeric_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<VariableValue> {
                numeric_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(VariableValue::Number)
            };
            Some(Box::new(extract_fn))
        }
        // TODO: how to handle this?
        FieldIndex::DatetimeIndex(numeric_index) => todo!(),
        FieldIndex::KeywordIndex(_)
        | FieldIndex::GeoIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::BoolIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

fn geo_point_extractor(index: &FieldIndex) -> Option<ExtractVariableFn> {
    match index {
        FieldIndex::GeoIndex(geo_map_index) => {
            let extract_fn = move |point_id: PointOffsetType| -> Option<VariableValue> {
                geo_map_index
                    .get_values(point_id)
                    .and_then(|mut values| values.next())
                    .map(|value| VariableValue::GeoPoint {
                        lat: value.lat,
                        lon: value.lon,
                    })
            };
            Some(Box::new(extract_fn))
        }
        FieldIndex::IntIndex(_)
        | FieldIndex::DatetimeIndex(_)
        | FieldIndex::IntMapIndex(_)
        | FieldIndex::KeywordIndex(_)
        | FieldIndex::FloatIndex(_)
        | FieldIndex::FullTextIndex(_)
        | FieldIndex::BoolIndex(_)
        | FieldIndex::UuidIndex(_)
        | FieldIndex::UuidMapIndex(_) => None,
    }
}

#[cfg(test)]
#[cfg(feature = "testing")]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use atomic_refcell::AtomicRefCell;
    use serde_json::{from_value, json};

    use super::VariableKind;
    use crate::index::field_index::geo_index::GeoMapIndex;
    use crate::index::field_index::numeric_index::NumericIndex;
    use crate::index::field_index::{FieldIndex, FieldIndexBuilderTrait};
    use crate::index::query_optimization::payload_provider::PayloadProvider;
    use crate::index::query_optimization::rescore_formula::value_extractor::{
        variable_extractor, VariableValue,
    };
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
    fn test_variable_extractor_from_payload() {
        let payload_provider = fixture_payload_provider();

        let no_indices = Default::default();

        // Test extracting a number from the payload.
        let extractor = variable_extractor(
            &no_indices,
            &"value".try_into().unwrap(),
            VariableKind::Number,
            payload_provider.clone(),
        );
        for id in 0..2 {
            let value = extractor(id);
            match id {
                0 => assert_eq!(value, Some(VariableValue::Number(42.0))),
                1 => assert_eq!(value, None),
                2 => assert_eq!(value, Some(VariableValue::Number(99.0))),
                _ => unreachable!(),
            }
        }

        // Test extracting a geo point from the payload.
        let extractor = variable_extractor(
            &no_indices,
            &"location".try_into().unwrap(),
            VariableKind::GeoPoint,
            payload_provider.clone(),
        );
        for id in 0..2 {
            let value = extractor(id);
            match id {
                0 => assert_eq!(value, None),
                1 => assert_eq!(
                    value,
                    Some(VariableValue::GeoPoint {
                        lat: 10.0,
                        lon: 20.0
                    })
                ),
                2 => assert_eq!(
                    value,
                    Some(VariableValue::GeoPoint {
                        lat: 15.5,
                        lon: 25.5
                    })
                ),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_variable_extractor_from_index() {
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

        // Test extracting a number from the index.
        let extractor = variable_extractor(
            &indices,
            &"value".try_into().unwrap(),
            VariableKind::Number,
            payload_provider.clone(),
        );
        for id in 0..2 {
            let value = extractor(id);
            match id {
                0 => assert_eq!(value, Some(VariableValue::Number(42.0))),
                1 => assert_eq!(value, None),
                2 => assert_eq!(value, Some(VariableValue::Number(99.0))),
                _ => unreachable!(),
            }
        }

        // Test extracting a geo point from the index.
        let extractor = variable_extractor(
            &indices,
            &"location".try_into().unwrap(),
            VariableKind::GeoPoint,
            payload_provider.clone(),
        );
        for id in 0..2 {
            let value = extractor(id);
            match id {
                0 => assert_eq!(value, None),
                1 => assert_eq!(
                    value,
                    Some(VariableValue::GeoPoint {
                        lat: 10.0,
                        lon: 20.0
                    })
                ),
                2 => assert_eq!(
                    value,
                    Some(VariableValue::GeoPoint {
                        lat: 15.5,
                        lon: 25.5
                    })
                ),
                _ => unreachable!(),
            }
        }
    }
}
