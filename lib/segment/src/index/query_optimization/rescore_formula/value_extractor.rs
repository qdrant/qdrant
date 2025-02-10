use std::collections::HashMap;
use std::rc::Rc;

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
        let payload_provider = Rc::new(PayloadProvider::new(self.payload.clone()));

        // prepare extraction of the variables from field indices or payload.
        let mut var_extractors = HashMap::new();
        for (key, var_kind) in variables {
            let extractor = self
                .field_indexes
                .get(&key)
                .and_then(|indices| {
                    indices
                        .iter()
                        .find_map(|index| variable_extractor(index, var_kind))
                })
                .unwrap_or_else(|| {
                    // TODO: optimize by reusing the same payload for all variables
                    // if the variable is not found in the index, try to find it in the payload
                    let key = key.clone();
                    let payload_provider = payload_provider.clone();
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
                });
            var_extractors.insert(key, extractor);
        }

        var_extractors
    }
}

fn variable_extractor(index: &FieldIndex, var_kind: VariableKind) -> Option<ExtractVariableFn> {
    match var_kind {
        VariableKind::Number => number_extractor(index),
        VariableKind::GeoPoint => geo_point_extractor(index),
    }
}

/// Returns function to extract the first number a point may have from the index
///
/// If there is no appropriate index, returns None
fn number_extractor(index: &FieldIndex) -> Option<ExtractVariableFn> {
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
        FieldIndex::IntMapIndex(map_index) => todo!(),
        FieldIndex::FloatIndex(numeric_index) => todo!(),
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
        FieldIndex::IntIndex(numeric_index) => todo!(),
        FieldIndex::DatetimeIndex(numeric_index) => todo!(),
        FieldIndex::IntMapIndex(map_index) => todo!(),
        FieldIndex::KeywordIndex(map_index) => todo!(),
        FieldIndex::FloatIndex(numeric_index) => todo!(),
        FieldIndex::GeoIndex(geo_map_index) => todo!(),
        FieldIndex::FullTextIndex(full_text_index) => todo!(),
        FieldIndex::BoolIndex(bool_index) => todo!(),
        FieldIndex::UuidIndex(numeric_index) => todo!(),
        FieldIndex::UuidMapIndex(map_index) => todo!(),
    }
}
