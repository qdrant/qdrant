use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::{Number, Value};
use uuid::Uuid;

use super::MapIndex;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::ValueIndexer;
use crate::index::field_index::utils::value_to_integer;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{IntPayloadType, UuidIntType, UuidPayloadType};

impl ValueIndexer for MapIndex<str> {
    type ValueType = String;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<String>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<String> {
        if let Value::String(keyword) = value {
            return Some(keyword.to_owned());
        }
        None
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<IntPayloadType> {
    type ValueType = IntPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<IntPayloadType> {
        value_to_integer(value)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

impl ValueIndexer for MapIndex<UuidIntType> {
    type ValueType = UuidIntType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match self {
            MapIndex::Mutable(index) => index.add_many_to_map(id, values, hw_counter),
            MapIndex::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable map index",
            )),
            MapIndex::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap map index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<Self::ValueType> {
        Some(Uuid::parse_str(value.as_str()?).ok()?.as_u128())
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.remove_point(id)
    }
}

// Per-K value retrievers — produce a closure that maps a point id to
// its indexed keyword/int/uuid values as JSON `Value`s. The conversion
// is K-specific, so each MapIndex variant has its own inherent impl.
// `FieldIndex::value_retriever` dispatches here per variant.

impl MapIndex<str> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(v).ok())
                .collect()
        })
    }
}

impl MapIndex<IntPayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .map(|v| Value::Number(Number::from(*v)))
                .collect()
        })
    }
}

impl MapIndex<UuidIntType> {
    pub fn value_retriever<'a>(
        &'a self,
        hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id, hw_counter)
                .into_iter()
                .flatten()
                .map(|value| Value::String(UuidPayloadType::from_u128(*value).to_string()))
                .collect()
        })
    }
}
