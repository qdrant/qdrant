//! Per-`(T, P)` value retrievers for [`ReadOnlyNumericIndex`].
//!
//! Mirrors the inherent `value_retriever` methods on the writable
//! `NumericIndex` (see `numeric_index/value_indexer.rs`): each numeric
//! variant produces a closure that maps a point id back to its indexed
//! values as JSON `Value`s. The conversion is `P`-specific, so each
//! `(T, P)` combination has its own inherent impl.

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::PointOffsetType;
use common::universal_io::UniversalRead;
use serde_json::{Number, Value};

use super::super::numeric_index_read::NumericIndexRead;
use super::ReadOnlyNumericIndex;
use crate::common::utils::MultiValue;
use crate::index::query_optimization::rescore_formula::value_retriever::VariableRetrieverFn;
use crate::types::{
    DateTimePayloadType, FloatPayloadType, IntPayloadType, UuidIntType, UuidPayloadType,
};

impl<S: UniversalRead> ReadOnlyNumericIndex<IntPayloadType, IntPayloadType, S> {
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

impl<S: UniversalRead> ReadOnlyNumericIndex<IntPayloadType, DateTimePayloadType, S> {
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

impl<S: UniversalRead> ReadOnlyNumericIndex<FloatPayloadType, FloatPayloadType, S> {
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

impl<S: UniversalRead> ReadOnlyNumericIndex<UuidIntType, UuidPayloadType, S> {
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
