use std::str::FromStr;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use serde_json::{Number, Value};
use uuid::Uuid;

use super::index::{NumericIndex, NumericIndexIntoInnerValue};
use super::storage::NumericIndexInner;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::utils::MultiValue;
use crate::index::field_index::ValueIndexer;
use crate::index::field_index::utils::value_to_integer;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType, UuidPayloadType,
};

impl ValueIndexer for NumericIndex<IntPayloadType, IntPayloadType> {
    type ValueType = IntPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<IntPayloadType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(id, values, hw_counter),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
            NumericIndexInner::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap numeric index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<IntPayloadType> {
        value_to_integer(value)
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl NumericIndexIntoInnerValue<IntPayloadType, IntPayloadType>
    for NumericIndex<IntPayloadType, IntPayloadType>
{
    fn into_inner_value(value: IntPayloadType) -> IntPayloadType {
        value
    }
}

impl ValueIndexer for NumericIndex<IntPayloadType, DateTimePayloadType> {
    type ValueType = DateTimePayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<DateTimePayloadType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(
                id,
                values.into_iter().map(Self::into_inner_value).collect(),
                hw_counter,
            ),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
            NumericIndexInner::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap numeric index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<DateTimePayloadType> {
        DateTimePayloadType::from_str(value.as_str()?).ok()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl NumericIndexIntoInnerValue<IntPayloadType, DateTimePayloadType>
    for NumericIndex<IntPayloadType, DateTimePayloadType>
{
    fn into_inner_value(value: DateTimePayloadType) -> IntPayloadType {
        value.timestamp()
    }
}

impl ValueIndexer for NumericIndex<FloatPayloadType, FloatPayloadType> {
    type ValueType = FloatPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<FloatPayloadType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => index.add_many_to_list(id, values, hw_counter),
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
            NumericIndexInner::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap numeric index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<FloatPayloadType> {
        value.as_f64()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl NumericIndexIntoInnerValue<FloatPayloadType, FloatPayloadType>
    for NumericIndex<FloatPayloadType, FloatPayloadType>
{
    fn into_inner_value(value: FloatPayloadType) -> FloatPayloadType {
        value
    }
}

impl ValueIndexer for NumericIndex<UuidIntType, UuidPayloadType> {
    type ValueType = UuidPayloadType;

    fn add_many(
        &mut self,
        id: PointOffsetType,
        values: Vec<Self::ValueType>,
        hw_counter: &HardwareCounterCell,
    ) -> OperationResult<()> {
        match &mut self.inner {
            NumericIndexInner::Mutable(index) => {
                let values: Vec<u128> = values.iter().map(|i| i.as_u128()).collect();
                index.add_many_to_list(id, values, hw_counter)
            }
            NumericIndexInner::Immutable(_) => Err(OperationError::service_error(
                "Can't add values to immutable numeric index",
            )),
            NumericIndexInner::Mmap(_) => Err(OperationError::service_error(
                "Can't add values to mmap numeric index",
            )),
        }
    }

    fn get_value(value: &Value) -> Option<Self::ValueType> {
        Uuid::parse_str(value.as_str()?).ok()
    }

    fn remove_point(&mut self, id: PointOffsetType) -> OperationResult<()> {
        self.inner.remove_point(id)
    }
}

impl NumericIndexIntoInnerValue<UuidIntType, UuidPayloadType>
    for NumericIndex<UuidIntType, UuidPayloadType>
{
    fn into_inner_value(value: UuidPayloadType) -> UuidIntType {
        value.as_u128()
    }
}

// Per-(T, U) value retrievers — produce a closure that maps a point id
// to its indexed values as JSON `Value`s. The conversion is U-specific
// (the second type param), so each numeric variant has its own inherent
// impl. `FieldIndex::value_retriever` dispatches here per variant.

impl NumericIndex<IntPayloadType, IntPayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .map(|v| Value::Number(Number::from(v)))
                .collect()
        })
    }
}

impl NumericIndex<IntPayloadType, DateTimePayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| serde_json::to_value(DateTimePayloadType::from_timestamp(v)?).ok())
                .collect()
        })
    }
}

impl NumericIndex<FloatPayloadType, FloatPayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .filter_map(|v| Some(Value::Number(Number::from_f64(v)?)))
                .collect()
        })
    }
}

impl NumericIndex<UuidIntType, UuidPayloadType> {
    pub fn value_retriever<'a>(
        &'a self,
        _hw_counter: &'a HardwareCounterCell,
    ) -> VariableRetrieverFn<'a> {
        Box::new(move |point_id: PointOffsetType| -> MultiValue<Value> {
            self.get_values(point_id)
                .into_iter()
                .flatten()
                .map(|value| Value::String(UuidPayloadType::from_u128(value).to_string()))
                .collect()
        })
    }
}
